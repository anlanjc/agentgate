"""M6 验收:OTel 追踪(内存 exporter 断言 span 与属性;默认关闭路径零影响)。"""

import httpx
import pytest

from app.observability import tracing
from tests.conftest import auth_headers
from tests.mock_upstream import build_mock_upstream


def _body(**extra) -> dict:
    body = {"model": "mock-model", "messages": [{"role": "user", "content": "hi"}]}
    body.update(extra)
    return body


@pytest.fixture
def otel_settings(auth_settings):
    auth_settings.otel_enabled = True
    return auth_settings


# OTel 全局 provider 只允许 set 一次:后续 fixture 复用首个 tracer/exporter,
# 测试间 clear() 隔离(此前每次重建 provider,span 会静默落进第一个 exporter)
_EXPORTER = None


@pytest.fixture
async def otel_gateway(otel_settings, redis_client):
    global _EXPORTER
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    from app.api.main import create_app
    from app.auth.keys import generate_key
    from app.db.models import Tenant, VirtualKey
    from app.db.session import init_db
    from app.upstream.client import UpstreamClient

    if tracing.get_tracer() is None:
        tracing.reset_tracing()
        _EXPORTER = InMemorySpanExporter()
        tracer = tracing.setup_tracing(otel_settings, exporter=_EXPORTER)
    else:
        tracer = tracing.get_tracer()
        _EXPORTER.clear()
    assert tracer is not None
    exporter = _EXPORTER

    mock_app = build_mock_upstream()
    upstream = UpstreamClient(
        otel_settings.upstream_base_url,
        otel_settings.upstream_api_key,
        client=httpx.AsyncClient(
            transport=httpx.ASGITransport(app=mock_app), base_url=otel_settings.upstream_base_url
        ),
    )
    app = create_app(settings=otel_settings, upstream=upstream, redis_client=redis_client)
    app.state.tracer = tracer
    await init_db(app.state.engine)

    raw, key_hash, prefix = generate_key()
    async with app.state.session_factory() as db:
        db.add(Tenant(id="t-otel", name="otel"))
        db.add(
            VirtualKey(
                id="k-otel", tenant_id="t-otel", key_hash=key_hash, key_prefix=prefix,
                rpm_limit=10_000, tpm_limit=10_000_000,
            )
        )
        await db.commit()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://gateway"
    ) as client:
        yield client, app, exporter, raw
    await upstream.aclose()
    await app.state.task_queue.stop()
    # 注意:此处不可 reset_tracing() —— OTel 全局 provider 只允许 set 一次,
    # reset 后下个 fixture 走"新建 provider"路径,新 exporter 会静默收不到 span
    # (trace.set_tracer_provider 二次调用被 SDK 忽略,tracer 仍绑首个 provider)


async def test_upstream_span_recorded_with_attributes(otel_gateway):
    client, app, exporter, raw = otel_gateway
    r = await client.post("/v1/chat/completions", json=_body(), headers=auth_headers(raw))
    assert r.status_code == 200

    spans = exporter.get_finished_spans()
    upstream_spans = [s for s in spans if s.name == "gateway.upstream"]
    assert len(upstream_spans) == 1
    attrs = dict(upstream_spans[0].attributes)
    assert attrs["gateway.channel"] == "mock"
    assert attrs["gateway.model"] == "mock-model"
    assert attrs["http.status_code"] == 200


async def test_otel_disabled_by_default(auth_settings, redis_client):
    """默认(未开 OTel)路径:tracer 为 None,请求零影响。"""
    from app.api.main import create_app
    from app.db.session import init_db

    app = create_app(settings=auth_settings, redis_client=redis_client)
    await init_db(app.state.engine)
    assert app.state.tracer is None
    await app.state.task_queue.stop()


# ---------- W3C Trace Context 传播(P4-2):提取/注入/回传 ----------


def _tp(trace_hex: str, span_hex: str) -> str:
    return f"00-{trace_hex}-{span_hex}-01"


async def test_incoming_traceparent_becomes_parent(otel_gateway):
    """client traceparent → 网关 span 挂同一 trace,父 span id = client span id。"""
    client, app, exporter, raw = otel_gateway
    trace_hex = "4bf92f3577b34da6a3ce929d0e0e4736"
    span_hex = "00f067aa0ba902b7"
    r = await client.post(
        "/v1/chat/completions", json=_body(),
        headers={**auth_headers(raw), "traceparent": _tp(trace_hex, span_hex)},
    )
    assert r.status_code == 200
    spans = exporter.get_finished_spans()
    gw = next(s for s in spans if s.name == "gateway.upstream")
    assert f"{gw.context.trace_id:032x}" == trace_hex
    assert gw.parent is not None and f"{gw.parent.span_id:016x}" == span_hex


async def test_traceparent_generated_and_echoed(otel_gateway):
    """client 未带 traceparent:网关从 span 生成注入上游,并经响应头回传同一 trace。"""
    client, app, exporter, raw = otel_gateway
    r = await client.post("/v1/chat/completions", json=_body(), headers=auth_headers(raw))
    assert r.status_code == 200
    tp = r.headers.get("traceparent")
    assert tp and len(tp) == 55
    spans = exporter.get_finished_spans()
    gw = next(s for s in spans if s.name == "gateway.upstream")
    assert tp.split("-")[1] == f"{gw.context.trace_id:032x}"  # 同一 trace


async def test_invalid_traceparent_ignored(otel_gateway):
    """非法 traceparent(版本 ff/长度不对):不猜,按无头处理,新建 trace。"""
    client, app, exporter, raw = otel_gateway
    r = await client.post(
        "/v1/chat/completions", json=_body(),
        headers={**auth_headers(raw), "traceparent": "ff-xyz-short-01"},
    )
    assert r.status_code == 200
    spans = exporter.get_finished_spans()
    gw = next(s for s in spans if s.name == "gateway.upstream")
    # 非法头=忽略:不认领垃圾头,trace 是正常新建的 32-hex(与垃圾串无任何派生关系;
    # FastAPI 0.162 原生 telemetry 会叠加自己的请求 span,不绑它的层级细节)
    assert gw.context.trace_id != 0
    assert all(c in "0123456789abcdef" for c in f"{gw.context.trace_id:032x}")


async def test_no_tracing_no_traceparent_header(auth_gateway):
    """追踪关闭:诚实不回传(无 trace 存在,不假装)。"""
    client, app, mock, raw = auth_gateway
    r = await client.post("/v1/chat/completions", json=_body(), headers=auth_headers(raw))
    assert r.status_code == 200
    assert r.headers.get("traceparent") is None
