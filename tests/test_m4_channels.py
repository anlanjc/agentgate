"""M4 验收:运行时渠道注册(admin API)—— 自家 Agent 服务接入网关的机制。

端到端剧本(也是面试演示剧本):
1. 启动时只有默认渠道;
2. 经 admin API 注册两个"Agent 服务"渠道(进程内 mock);
3. kill 其一(故障注入)→ 网关自动 failover 到另一个,账本照记。
"""

import httpx
import pytest

from app.api.main import create_app
from app.auth.keys import generate_key
from app.db.models import Tenant, VirtualKey
from app.db.session import init_db
from app.upstream.client import UpstreamClient
from tests.conftest import auth_headers
from tests.mock_upstream import build_mock_upstream


def _body(**extra) -> dict:
    body = {"model": "mock-model", "messages": [{"role": "user", "content": "hi"}]}
    body.update(extra)
    return body


@pytest.fixture
def agent_settings(tmp_path, pg_url):
    from app.config import Settings

    return Settings(
        upstream_name="legacy-upstream",
        upstream_base_url="http://mock/v1",
        upstream_api_key="sk-x",
        models=["mock-model"],
        db_url=pg_url or f"sqlite+aiosqlite:///{tmp_path}/m4.db",
        redis_url="redis://127.0.0.1:6379/1",
        auth_enabled=True,
    )


async def test_runtime_channel_registration_and_failover(agent_settings, redis_client):
    # 网关:默认渠道 legacy-upstream 指向健康 mock;再注册两个"Agent"渠道
    mock_ok = build_mock_upstream(default_case="ok")
    mock_dead = build_mock_upstream(default_case="err_500")
    app = create_app(settings=agent_settings, redis_client=redis_client)
    await init_db(app.state.engine)

    agent_a = UpstreamClient(
        "http://agent-a/v1", "", client=httpx.AsyncClient(
            transport=httpx.ASGITransport(app=mock_dead), base_url="http://agent-a/v1"
        )
    )
    agent_b = UpstreamClient(
        "http://agent-b/v1", "", client=httpx.AsyncClient(
            transport=httpx.ASGITransport(app=mock_ok), base_url="http://agent-b/v1"
        )
    )
    app.state.channel_router.register("agent-a", agent_a, ["mock-model"])
    app.state.channel_router.register("agent-b", agent_b, ["mock-model"])

    raw, key_hash, prefix = generate_key()
    async with app.state.session_factory() as db:
        db.add(Tenant(id="t-m4", name="m4"))
        db.add(
            VirtualKey(
                id="k-m4", tenant_id="t-m4", key_hash=key_hash, key_prefix=prefix,
                rpm_limit=10_000, tpm_limit=10_000_000,
            )
        )
        await db.commit()

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://gateway"
    ) as client:
        # 1) admin API 重复注册 → 409(幂等防线)
        r_dup = await client.post(
            "/admin/channels",
            json={"name": "agent-a", "base_url": "http://agent-a/v1", "models": ["mock-model"]},
        )
        assert r_dup.status_code == 409

        # 2) kill agent-a(恒 500)→ 自动 failover 到 agent-b
        resp = await client.post("/v1/chat/completions", json=_body(), headers=auth_headers(raw))
        assert resp.status_code == 200
        assert resp.headers["x-upstream-channel"] == "agent-b"

        # 3) 账本照记且渠道标注正确
        await app.state.task_queue.flush()
        usage = (await client.get("/admin/tenants/t-m4/usage")).json()
        assert usage["requests"] == 1 and usage["total_tokens"] == 18

    await agent_a.aclose()
    await agent_b.aclose()
    await app.state.task_queue.stop()


# ---------- GW-10(评审 2.0)回归:base_url 注册校验 + 坏 URL 走故障转移而非 500 ----------


async def test_register_channel_rejects_bad_base_url(auth_gateway):
    """GW-10 回归(入口闸):base_url 无 scheme/无 host/非 http(s) → 400 统一信封,
    渠道不注册不落库(修复前 'localhost:9000/v1' 直接 200 registered)。"""
    from sqlalchemy import select

    from app.db.models import Channel

    client, app, mock, raw = auth_gateway
    h = {**auth_headers(raw), "X-Admin-Token": ""}  # dev 口径 admin_token 为空
    for bad in ("localhost:9000/v1", "ftp://example.com", "http://", "not a url"):
        r = await client.post(
            "/admin/channels",
            json={"name": f"bad-{bad[:6]}", "base_url": bad, "models": ["mock-model"]},
            headers=h,
        )
        assert r.status_code == 400, f"{bad!r} 应 400,实际 {r.status_code}"
        assert r.json()["error"]["code"] == "invalid_base_url"
        assert f"bad-{bad[:6]}" not in app.state.channel_router.names

    async with app.state.session_factory() as db:
        rows = (
            await db.execute(select(Channel).where(Channel.name.like("bad-%")))
        ).scalars().all()
    assert rows == []  # 坏 URL 不落库

    # 合法 URL 仍可注册(http 与 https)
    for good in ("http://agent-x/v1", "https://agent-y/v1"):
        r = await client.post(
            "/admin/channels",
            json={"name": good.split("//")[1].split("/")[0], "base_url": good, "models": []},
            headers=h,
        )
        assert r.status_code == 200, r.text


async def test_bad_url_channel_fails_over_instead_of_500(auth_gateway):
    """GW-10 回归(调度兜底):绕过入口闸的坏 URL 渠道(模拟存量坏行/手工配置)
    在发送时抛 UnsupportedProtocol —— 修复前不在捕获面内,未处理 500 且
    mark_failure 从未执行(渠道不冷却,备渠永不接管)。修复后:mark_failure
    计入、failover 到健康渠道,请求 200;仅坏渠道时 502 统一信封不裸 500。"""
    from app.upstream.client import UpstreamClient

    client, app, mock, raw = auth_gateway
    bad = UpstreamClient("localhost:9000/v1", "", client=httpx.AsyncClient())
    app.state.channel_router.register("bad-channel", bad, ["mock-model"])

    r = await client.post(
        "/v1/chat/completions",
        json={"model": "mock-model", "messages": [{"role": "user", "content": "hi"}]},
        headers=auth_headers(raw),
    )
    assert r.status_code == 200  # 坏渠道被冷却,健康 mock 渠道接管
    assert r.headers.get("x-upstream-channel") == "mock"
    st = await app.state.channel_router.state_of("bad-channel")
    assert st["fail_streak"] == 1  # mark_failure 确实执行(修复前恒 0)

    # 仅坏渠道可服务另一模型 → 502 统一信封(而非未处理 500)
    worse = UpstreamClient("ftp://nope/v1", "", client=httpx.AsyncClient())
    app.state.channel_router.register("worse", worse, ["only-bad-model"])
    r2 = await client.post(
        "/v1/chat/completions",
        json={"model": "only-bad-model", "messages": [{"role": "user", "content": "hi"}]},
        headers=auth_headers(raw),
    )
    assert r2.status_code == 502
    assert r2.json()["error"]["code"] == "upstream_invalid_url"
    await bad.aclose()
    await worse.aclose()
