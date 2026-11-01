"""M2 验收:故障转移状态机(500 冷却 / 429 尊重 Retry-After / 延迟优先路由 / 全渠道耗尽透传)。"""

import time

import httpx
import pytest

from app.api.main import create_app
from app.auth.keys import generate_key
from app.db.models import Tenant, VirtualKey
from app.db.session import build_session_factory, init_db
from app.upstream.client import UpstreamClient
from tests.mock_upstream import build_mock_upstream


def _body(**extra) -> dict:
    body = {"model": "mock-model", "messages": [{"role": "user", "content": "hi"}]}
    body.update(extra)
    return body


def _build_upstream(settings, mock_app):
    return UpstreamClient(
        settings.upstream_base_url,
        settings.upstream_api_key,
        client=httpx.AsyncClient(
            transport=httpx.ASGITransport(app=mock_app), base_url=settings.upstream_base_url
        ),
    )


async def _two_channel_app(auth_settings, redis_client, case_a: str, case_b: str):
    """ch-a(按 case_a 故障)+ ch-b(按 case_b);ch-a 为默认注册渠道。"""
    auth_settings.upstream_name = "ch-a"
    mock_a = build_mock_upstream(default_case=case_a)
    mock_b = build_mock_upstream(default_case=case_b)
    upstream_a = _build_upstream(auth_settings, mock_a)
    upstream_b = _build_upstream(auth_settings, mock_b)
    app = create_app(settings=auth_settings, upstream=upstream_a, redis_client=redis_client)
    app.state.channel_router.register("ch-b", upstream_b)
    await init_db(app.state.engine)

    raw, key_hash, prefix = generate_key()
    async with build_session_factory(app.state.engine)() as db:
        db.add(Tenant(id="t-test", name="t"))
        db.add(
            VirtualKey(
                id="k-test", tenant_id="t-test", key_hash=key_hash, key_prefix=prefix,
                rpm_limit=10_000, tpm_limit=10_000_000,
            )
        )
        await db.commit()

    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gateway")
    return client, app, upstream_a, upstream_b


@pytest.fixture
def auth_settings(tmp_path, pg_url):
    from app.config import Settings

    return Settings(
        upstream_name="ch-a",
        upstream_base_url="http://mock/v1",
        upstream_api_key="sk-test",
        models=["mock-model"],
        db_url=pg_url or f"sqlite+aiosqlite:///{tmp_path}/t.db",
        redis_url="redis://127.0.0.1:6379/1",
        auth_enabled=True,
    )


async def test_failover_on_500(auth_settings, redis_client):
    """ch-a 恒 500 → 自动切 ch-b,响应头标注命中渠道;ch-a 进入冷却。"""
    auth_settings.auth_enabled = False
    client, app, ua, ub = await _two_channel_app(auth_settings, redis_client, "err_500", "ok")
    try:
        resp = await client.post("/v1/chat/completions", json=_body())
        assert resp.status_code == 200
        assert resp.headers["x-upstream-channel"] == "ch-b"
        st = await app.state.channel_router.state_of("ch-a")
        assert st["cooldown_until_ms"] > int(time.time() * 1000)  # 已进入冷却
    finally:
        await client.aclose()
        await ua.aclose()
        await ub.aclose()
        await app.state.writer.stop()


async def test_429_respects_retry_after(auth_settings, redis_client):
    """ch-a 恒 429(Retry-After: 1)→ 切 ch-b;ch-a 冷却 ≈ 1s。"""
    auth_settings.auth_enabled = False
    client, app, ua, ub = await _two_channel_app(auth_settings, redis_client, "err_429", "ok")
    try:
        resp = await client.post("/v1/chat/completions", json=_body())
        assert resp.status_code == 200
        assert resp.headers["x-upstream-channel"] == "ch-b"
        st = await app.state.channel_router.state_of("ch-a")
        now_ms = int(time.time() * 1000)
        assert 0 < st["cooldown_until_ms"] - now_ms <= 1000  # 尊重上游 Retry-After,而非默认 60s
    finally:
        await client.aclose()
        await ua.aclose()
        await ub.aclose()
        await app.state.writer.stop()


async def test_latency_based_routing(auth_settings, redis_client):
    """双渠道健康时按 EMA 升序:ch-a EMA 更低 → 优先。"""
    auth_settings.auth_enabled = False
    client, app, ua, ub = await _two_channel_app(auth_settings, redis_client, "ok", "ok")
    try:
        await app.state.redis.hset("gate:ch:ch-a", mapping={"ema_ms": 5.0})
        await app.state.redis.hset("gate:ch:ch-b", mapping={"ema_ms": 500.0})
        resp = await client.post("/v1/chat/completions", json=_body())
        assert resp.status_code == 200
        assert resp.headers["x-upstream-channel"] == "ch-a"
    finally:
        await client.aclose()
        await ua.aclose()
        await ub.aclose()
        await app.state.writer.stop()


async def test_all_channels_fail_passthrough(auth_settings, redis_client):
    """双渠道全 500 → 透传最后的上游错误,渠道头 = exhausted。"""
    auth_settings.auth_enabled = False
    client, app, ua, ub = await _two_channel_app(auth_settings, redis_client, "err_500", "err_500")
    try:
        resp = await client.post("/v1/chat/completions", json=_body())
        assert resp.status_code == 500
        assert resp.headers["x-upstream-channel"] == "exhausted"
    finally:
        await client.aclose()
        await ua.aclose()
        await ub.aclose()
        await app.state.writer.stop()


async def test_stream_failover(auth_settings, redis_client):
    """流式请求同样走故障转移:ch-a 500 → ch-b 流式 200。"""
    auth_settings.auth_enabled = False
    client, app, ua, ub = await _two_channel_app(auth_settings, redis_client, "err_500", "ok")
    try:
        resp = await client.post("/v1/chat/completions", json=_body(stream=True))
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/event-stream")
        assert resp.headers["x-upstream-channel"] == "ch-b"
        assert "data: [DONE]" in resp.text
    finally:
        await client.aclose()
        await ua.aclose()
        await ub.aclose()
        await app.state.writer.stop()


async def test_redis_down_router_degrades():
    """Redis 挂掉:路由降级为无状态(仍能选出候选),网关不死。"""
    from redis.asyncio import Redis

    from app.router.failover import ChannelRouter

    dead = Redis(host="127.0.0.1", port=6390, protocol=2, socket_connect_timeout=0.2)
    router = ChannelRouter(dead)
    router.register("ch-a", object())
    router.register("ch-b", object())
    try:
        cands = await router.candidates()
        assert set(cands) == {"ch-a", "ch-b"}  # 降级:全部候选,无排序保证
        await router.mark_failure("ch-a")  # 不抛异常即通过
    finally:
        await dead.aclose()
