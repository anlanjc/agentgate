"""M6 验收:持久化与多副本——渠道注册表重启恢复 / 双副本限流恰放行 / /ready。"""

import asyncio

import httpx

from app.api.main import create_app, load_channels_from_db
from app.auth.keys import generate_key
from app.db.models import Tenant, VirtualKey
from app.db.session import build_session_factory, init_db
from tests.conftest import auth_headers
from tests.mock_upstream import build_mock_upstream


def _body(**extra) -> dict:
    body = {"model": "mock-model", "messages": [{"role": "user", "content": "hi"}]}
    body.update(extra)
    return body


async def _make_app(settings, redis_client, tmp_path, tag: str):
    app = create_app(settings=settings, redis_client=redis_client)
    await init_db(app.state.engine)
    return app


async def test_channel_registry_survives_restart(auth_settings, redis_client):
    """渠道持久化:admin 注册 → 模拟重启(新 app + load_channels_from_db)→ 渠道仍在。"""
    app1 = create_app(settings=auth_settings, redis_client=redis_client)
    await init_db(app1.state.engine)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app1), base_url="http://gateway"
    ) as c1:
        r = await c1.post(
            "/admin/channels",
            json={"name": "agent-x", "base_url": "http://127.0.0.1:9/v1", "models": ["mock-model"]},
        )
        assert r.status_code == 200 and r.json()["persisted"] is True

    # 模拟重启:全新 app 实例(共享同一 DB/Redis)
    app2 = create_app(settings=auth_settings, redis_client=redis_client)
    await init_db(app2.state.engine)
    assert "agent-x" not in app2.state.channel_router.names  # 重启后尚未恢复
    restored = await load_channels_from_db(app2)
    assert restored >= 1
    assert "agent-x" in app2.state.channel_router.names  # 恢复成功
    await app1.state.task_queue.stop()
    await app2.state.task_queue.stop()


async def test_two_replica_rate_limit_no_amplification(auth_settings, redis_client):
    """双副本一致性:两 app 共享 Redis,合并并发 100 × 容量 50 → 全局恰放行 50。

    这是"单机限流多副本放大 N 倍"反例的直接反证测试(ADR-001/README 硬点 2)。
    """
    from app.upstream.client import UpstreamClient

    mock_app = build_mock_upstream()
    settings_a = auth_settings
    app_a = create_app(
        settings=settings_a, redis_client=redis_client,
        upstream=UpstreamClient(
            settings_a.upstream_base_url, settings_a.upstream_api_key,
            client=httpx.AsyncClient(
                transport=httpx.ASGITransport(app=mock_app), base_url=settings_a.upstream_base_url
            ),
        ),
    )
    await init_db(app_a.state.engine)
    app_b = create_app(
        settings=settings_a, redis_client=redis_client,
        upstream=UpstreamClient(
            settings_a.upstream_base_url, settings_a.upstream_api_key,
            client=httpx.AsyncClient(
                transport=httpx.ASGITransport(app=mock_app), base_url=settings_a.upstream_base_url
            ),
        ),
    )

    raw, key_hash, prefix = generate_key()
    async with build_session_factory(app_a.state.engine)() as db:
        db.add(Tenant(id="t-rep", name="rep"))
        db.add(
            VirtualKey(
                id="k-rep", tenant_id="t-rep", key_hash=key_hash, key_prefix=prefix,
                rpm_limit=50, tpm_limit=10_000_000,
            )
        )
        await db.commit()
    await redis_client.delete("vk:" + key_hash)  # 清鉴权缓存,让两副本各自回源一次

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app_a), base_url="http://a"
    ) as ca, httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app_b), base_url="http://b"
    ) as cb:
        h = auth_headers(raw)
        body = _body()
        sem = asyncio.Semaphore(50)
        results = []

        async def one(i):
            async with sem:
                client = ca if i % 2 == 0 else cb
                r = await client.post("/v1/chat/completions", json=body, headers=h)
                results.append(r.status_code)

        await asyncio.gather(*[one(i) for i in range(100)])

    ok200 = sum(1 for s in results if s == 200)
    rejected = sum(1 for s in results if s == 429)
    # 令牌桶语义:容量 50 + 测试窗口内的惰性回补(速率 50/60/s × 数秒 ≈ +2)
    # —— 多副本不放大 N 倍(那是 100),回补是正确行为,允许 ±2 容差并注明
    assert 50 <= ok200 <= 52, f"双副本异常:放行 {ok200}(应 50 + 回补≤2)"
    assert rejected >= 48
    await app_a.state.task_queue.stop()
    await app_b.state.task_queue.stop()


async def test_ready_endpoint(auth_settings, redis_client):
    """/ready:Redis + DB 双检全过 → ready true。"""
    app = create_app(settings=auth_settings, redis_client=redis_client)
    await init_db(app.state.engine)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://gateway"
    ) as client:
        r = await client.get("/ready")
    assert r.status_code == 200
    data = r.json()
    assert data["ready"] is True
    assert data["checks"]["redis"] == "ok" and data["checks"]["db"] == "ok"
    await app.state.task_queue.stop()
