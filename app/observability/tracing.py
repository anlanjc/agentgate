"""分布式追踪(OpenTelemetry,默认关闭,AGENTGATE_OTEL_ENABLED=1 开启)。

设计:
- 手动 span(而非自动 instrumentation):只在有语义的边界开 span——网关请求整体、
  上游调用;属性挂排障有用的维度(tenant/model/channel/status),不挂 request_id 全量;
- 导出器:console(演示)= 控制台 JSON;otlp(生产)= OTLP gRPC 到 collector;
  测试用 InMemorySpanExporter 直接断言;
- 与 request_id 的关系:request_id 是业务排障键(日志),trace_id 是跨服务关联键(追踪)——
  两者互补,span 属性里带 request_id 做桥。
"""


from app.config import Settings
from app.log import get_logger

logger = get_logger("tracing")

_tracer = None


def reset_tracing():
    """测试辅助:清空全局 tracer(否则 lru 式缓存会让第二个 provider 失效)。"""
    global _tracer
    _tracer = None


def setup_tracing(settings: Settings, exporter=None):
    """初始化全局 TracerProvider;返回 tracer 或 None(未启用)。

    exporter 参数供测试注入(如 InMemorySpanExporter);缺省按配置选 console/otlp。
    """
    global _tracer
    if not settings.otel_enabled:
        return None
    if _tracer is not None:
        return _tracer

    from opentelemetry import trace
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

    provider = TracerProvider(resource=Resource.create({
        "service.name": "agentgate",
        "service.version": settings.otel_service_version,
    }))
    if exporter is not None:
        # 测试注入:Simple 即时刷,断言无缓冲
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor

        provider.add_span_processor(SimpleSpanProcessor(exporter))
    elif settings.otel_exporter == "otlp" and settings.otel_otlp_endpoint:
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=settings.otel_otlp_endpoint)))
    else:
        provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
    trace.set_tracer_provider(provider)
    _tracer = trace.get_tracer("agentgate.gateway")
    logger.info("otel_tracing_enabled", exporter=settings.otel_exporter)
    return _tracer


def get_tracer():
    return _tracer


# ---------- W3C Trace Context 提取/注入(P4-2:trace 跨服务连续) ----------

import re

_TP_RE = re.compile(r"^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$")


def _valid_traceparent(value: str | None) -> str | None:
    """W3C traceparent 语法校验(版本 ff 非法/长度/hex);非法返回 None,不猜。"""
    if not value:
        return None
    v = value.strip().lower()
    if not _TP_RE.match(v) or v.startswith("ff-"):
        return None
    return v


def incoming_context(headers):
    """client traceparent → OTel Context(网关 span 挂到调用方 trace 上)。

    追踪未启用或头非法 → None(span 照常独立建,行为与从前一致)。
    """
    if get_tracer() is None:
        return None
    v = _valid_traceparent(headers.get("traceparent"))
    if v is None:
        return None
    from opentelemetry.propagate import extract

    return extract({"traceparent": v})


def current_traceparent() -> str | None:
    """当前 span 的 traceparent(响应头回传/子调用注入用);无活动 span 返回 None。"""
    if get_tracer() is None:
        return None
    from opentelemetry.propagate import inject

    carrier: dict = {}
    inject(carrier)
    return carrier.get("traceparent") or None
