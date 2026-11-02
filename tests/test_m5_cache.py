"""M5 验收:精确响应缓存(租户隔离 / 确定性门控 / X-Cache 语义 / 清空)。"""

import httpx
import pytest

from tests.conftest import TENANT_ID, auth_headers


def _body(**extra) -> dict:
    body = {"model": "mock-model", "messages": [{"role": "user", "content": "hi"}]}
    body.update(extra)
    return body


@pytest.fixture
def cache_settings(auth_settings):
    auth_settings.response_cache_enabled = True
    auth_settings.response_cache_ttl = 300
    return auth_settings


@pytest.fixture
async def cache_gateway(cache_settings, redis_client):
    from app.api.main import create_app
    from app.auth.keys import generate_key
    from app.db.models import Tenant, VirtualKey
    from app.db.session import init_db
    from app.upstream.client import UpstreamClient
    from tests.mock_upstream import build_mock_upstream

    mock_app = build_mock_upstream()
    upstream = UpstreamClient(
        cache_settings.upstream_base_url,
        cache_settings.upstream_api_key,
        client=httpx.AsyncClient(
            transport=httpx.ASGITransport(app=mock_app), base_url=cache_settings.upstream_base_url
        ),
    )
    app = create_app(settings=cache_settings, upstream=upstream, redis_client=redis_client)
    await init_db(app.state.engine)
    raw, key_hash, prefix = generate_key()
    async with app.state.session_factory() as db:
        db.add(Tenant(id="t-c1", name="c1"))
        db.add(Tenant(id="t-c2", name="c2"))
        db.add(
            VirtualKey(
                id="k-c1", tenant_id="t-c1", key_hash=key_hash, key_prefix=prefix,
                rpm_limit=10_000, tpm_limit=10_000_000,
            )
        )
        await db.commit()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://gateway"
    ) as client:
        yield client, app, raw
    await upstream.aclose()
    await app.state.task_queue.stop()


async def test_second_request_hits_cache(cache_gateway):
    client, app, raw = cache_gateway
    h = auth_headers(raw)
    r1 = await client.post("/v1/chat/completions", json=_body(), headers=h)
    assert r1.status_code == 200 and r1.headers["x-cache"] == "MISS"
    r2 = await client.post("/v1/chat/completions", json=_body(), headers=h)
    assert r2.status_code == 200 and r2.headers["x-cache"] == "HIT"
    assert r2.json() == r1.json()  # 内容逐字段一致


async def test_cache_tenant_isolation(cache_gateway):
    """租户 A 的缓存答案,租户 B 读不到 —— 缓存键含 tenant_id(安全边界)。"""
    client, app, raw_a = cache_gateway
    created = (
        await client.post(f"/admin/tenants/{TENANT_ID}/keys".replace(TENANT_ID, "t-c2"), json={"name": "b"})
    ).json()
    h_a = auth_headers(raw_a)
    h_b = auth_headers(created["key"])
    r1 = await client.post("/v1/chat/completions", json=_body(), headers=h_a)
    assert r1.headers["x-cache"] == "MISS"
    r2 = await client.post("/v1/chat/completions", json=_body(), headers=h_b)
    assert r2.headers["x-cache"] == "MISS"  # 不同租户不共享


async def test_nondeterministic_requests_not_cached(cache_gateway):
    """temperature>0 / 流式 / 带 tools 一律不进缓存(无 x-cache 头 —— 头只在缓存语义参与时出现)。"""
    client, app, raw = cache_gateway
    h = auth_headers(raw)
    r1 = await client.post("/v1/chat/completions", json=_body(temperature=0.7), headers=h)
    r2 = await client.post("/v1/chat/completions", json=_body(temperature=0.7), headers=h)
    assert "x-cache" not in r1.headers and "x-cache" not in r2.headers

    s1 = await client.post("/v1/chat/completions", json=_body(stream=True), headers=h)
    assert "x-cache" not in s1.headers  # 流式路径无缓存语义

    t1 = await client.post("/v1/chat/completions", json=_body(tools=[{"type": "function"}]), headers=h)
    assert "x-cache" not in t1.headers


async def test_cache_ledger_marks_channel_cache(cache_gateway):
    """命中缓存的请求照记账,channel='cache'(租户账本完整,上游成本为零)。"""
    client, app, raw = cache_gateway
    h = auth_headers(raw)
    await client.post("/v1/chat/completions", json=_body(), headers=h)
    await app.state.task_queue.flush()
    r2 = await client.post("/v1/chat/completions", json=_body(), headers=h)
    assert r2.headers["x-cache"] == "HIT"
    await app.state.task_queue.flush()
    usage = (await client.get("/admin/tenants/t-c1/usage")).json()
    assert usage["requests"] == 2 and usage["total_tokens"] == 36


async def test_admin_purge(cache_gateway):
    client, app, raw = cache_gateway
    h = auth_headers(raw)
    await client.post("/v1/chat/completions", json=_body(), headers=h)
    r = await client.request("DELETE", "/admin/cache")
    assert r.status_code == 200 and r.json()["purged"] >= 1
    r2 = await client.post("/v1/chat/completions", json=_body(), headers=h)
    assert r2.headers["x-cache"] == "MISS"  # 清空后重新回源
