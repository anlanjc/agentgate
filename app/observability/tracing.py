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
