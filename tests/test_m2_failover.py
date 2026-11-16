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
        await app.state.task_queue.stop()


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
        # 尊重上游 Retry-After(5s),而非默认 60s;上界是核心判别
        assert 0 < st["cooldown_until_ms"] - now_ms <= 5000
    finally:
        await client.aclose()
        await ua.aclose()
        await ub.aclose()
        await app.state.task_queue.stop()


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
        await app.state.task_queue.stop()


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
        await app.state.task_queue.stop()


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
        await app.state.task_queue.stop()


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


# ---- GATE-9:冷却窗时钟纪律(写入与过滤必须同源 Redis TIME,对齐 GW-4) ----


class _FastClock:
    """把"本地墙钟"拨快 skew 秒,模拟双副本部署里的快钟副本(k8s replicas=2)。

    注入点 app.router.failover.time 是旧实现消费的时钟源;修复后该模块不再
    读取本地墙钟,此注入自然失效 —— "注入失效仍绿"正是修复的判别点。
    """

    def __init__(self, skew_s: float = 120.0) -> None:
        self._skew = skew_s

    def time(self) -> float:
        return time.time() + self._skew


async def _redis_now_ms(redis) -> int:
    seconds, micros = await redis.time()
    return seconds * 1000 + micros // 1000


async def test_cooldown_filter_uses_redis_clock(redis_client, monkeypatch):
    """GATE-9:候选过滤的 now 必须取 Redis 时钟,与冷却 deadline 同源。

    场景:另一副本刚用 Redis 时钟把 ch-a 冷却 60s;本副本本地墙钟快 120s。
    本地口径下冷却窗"已过期"→ 快钟副本往别的副本刚冷却的渠道继续打流量;
    Redis 口径下冷却窗未到 → ch-a 对本副本不可见。
    """
    from app.router.failover import ChannelRouter

    monkeypatch.setattr("app.router.failover.time", _FastClock(), raising=False)
    router = ChannelRouter(redis_client)
    router.register("ch-a", object())
    router.register("ch-b", object())
    await redis_client.hset(
        "gate:ch:ch-a", mapping={"cooldown_until_ms": await _redis_now_ms(redis_client) + 60_000}
    )
    cands = await router.candidates()
    assert cands == ["ch-b"], f"快钟副本漏判冷却窗: {cands}"


async def test_mark_failure_deadline_uses_redis_clock(redis_client, monkeypatch):
    """GATE-9:mark_failure 写入的冷却 deadline 基于 Redis 时钟(≈+60s),非本地墙钟。"""
    from app.router.failover import COOLDOWN_MS, ChannelRouter

    monkeypatch.setattr("app.router.failover.time", _FastClock(), raising=False)
    router = ChannelRouter(redis_client)
    router.register("ch-a", object())
    await router.mark_failure("ch-a")
    st = await router.state_of("ch-a")
    delta = st["cooldown_until_ms"] - await _redis_now_ms(redis_client)
    # 旧实现:deadline = 快钟本地 +60s → 距 Redis now ≈180s;修复后 ≈60s
    assert 0 < delta <= COOLDOWN_MS + 2_000, f"deadline 偏离 Redis 时钟: delta={delta}ms"
    assert st["fail_streak"] == 1


async def test_mark_rate_limited_deadline_uses_redis_clock(redis_client, monkeypatch):
    """GATE-9:mark_rate_limited 的冷却窗(Retry-After=5s)基于 Redis 时钟,非本地墙钟。"""
    from app.router.failover import ChannelRouter

    monkeypatch.setattr("app.router.failover.time", _FastClock(), raising=False)
    router = ChannelRouter(redis_client)
    router.register("ch-a", object())
    await router.mark_rate_limited("ch-a", 5)
    st = await router.state_of("ch-a")
    delta = st["cooldown_until_ms"] - await _redis_now_ms(redis_client)
    # 旧实现:deadline = 快钟本地 +5s → 距 Redis now ≈125s;修复后 ≈5s
    assert 0 < delta <= 5_000 + 2_000, f"deadline 偏离 Redis 时钟: delta={delta}ms"
