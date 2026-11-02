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


@pytest.fixture
async def otel_gateway(otel_settings, redis_client):
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    from app.api.main import create_app
    from app.auth.keys import generate_key
    from app.db.models import Tenant, VirtualKey
    from app.db.session import init_db
    from app.upstream.client import UpstreamClient

    tracing.reset_tracing()
    exporter = InMemorySpanExporter()
    # 先建 provider(内存 exporter),create_app 内部的 setup_tracing 会复用缓存 tracer
    tracer = tracing.setup_tracing(otel_settings, exporter=exporter)
    assert tracer is not None

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
    tracing.reset_tracing()


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
