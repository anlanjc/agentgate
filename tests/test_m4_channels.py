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
def agent_settings(tmp_path):
    from app.config import Settings

    return Settings(
        upstream_name="legacy-upstream",
        upstream_base_url="http://mock/v1",
        upstream_api_key="sk-x",
        models=["mock-model"],
        db_url=f"sqlite+aiosqlite:///{tmp_path}/m4.db",
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
        await app.state.writer.flush()
        usage = (await client.get("/admin/tenants/t-m4/usage")).json()
        assert usage["requests"] == 1 and usage["total_tokens"] == 18

    await agent_a.aclose()
    await agent_b.aclose()
    await app.state.writer.stop()
