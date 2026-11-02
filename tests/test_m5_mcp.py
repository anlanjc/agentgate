"""M5 验收:MCP stdio server(真子进程 + 真 MCP 握手)。

核心叙事:MCP 工具与网关共享同一状态面 —— MCP 签发的 key 立即可用于网关请求,
用量经 MCP 工具可读。不是旁路实现(与 admin API 同层服务函数)。
"""

import os
import sys
from pathlib import Path

import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from tests.conftest import TENANT_ID, auth_headers

ROOT = Path(__file__).resolve().parents[1]


def _body(**extra) -> dict:
    body = {"model": "mock-model", "messages": [{"role": "user", "content": "hi"}]}
    body.update(extra)
    return body


@pytest.fixture
def mcp_env(tmp_path, pg_url):
    """MCP 子进程与测试 app 共享 DB/Redis(经环境变量注入)。"""
    return {
        **os.environ,
        "AGENTGATE_DB_URL": (pg_url or f"sqlite+aiosqlite:///{tmp_path}/mcp_shared.db"),
        "AGENTGATE_REDIS_URL": "redis://127.0.0.1:6379/1",
        "AGENTGATE_LOG_LEVEL": "WARNING",
        "PYTHONPATH": str(ROOT),
    }


async def test_mcp_tools_share_state_with_gateway(auth_gateway, tmp_path):
    client, app, mock, raw = auth_gateway

    # 1) 经网关发 2 个请求(账本落 2 行)
    for _ in range(2):
        r = await client.post("/v1/chat/completions", json=_body(), headers=auth_headers(raw))
        assert r.status_code == 200
    await app.state.task_queue.flush()

    # MCP 子进程与网关 app 共享同一 DB/Redis(直接取 app 实际配置,避免两套路径不一致)
    settings = app.state.settings
    mcp_env = {
        **os.environ,
        "AGENTGATE_DB_URL": settings.db_url,
        "AGENTGATE_REDIS_URL": settings.redis_url,
        "AGENTGATE_LOG_LEVEL": "WARNING",
        "PYTHONPATH": str(ROOT),
    }

    params = StdioServerParameters(
        command=sys.executable,
        args=["-m", "app.mcp_server"],
        cwd=str(ROOT),
        env=mcp_env,
    )
    async with stdio_client(params) as (read, write), ClientSession(read, write) as session:
        await session.initialize()
        tools = await session.list_tools()
        names = {t.name for t in tools.tools}
        assert {"gateway_channels", "gateway_usage", "gateway_budget", "gateway_issue_key"} <= names

        # 2) MCP 读账本:应看到 2 请求 / 36 token(与网关共享 DB)
        res = await session.call_tool("gateway_usage", {"tenant_id": TENANT_ID})
        import json

        data = json.loads(res.content[0].text)
        assert data["requests"] == 2 and data["total_tokens"] == 36

        # 3) MCP 签发 key → 立即可用于网关(共享状态面的证明)
        res = await session.call_tool(
            "gateway_issue_key",
            {"tenant_id": TENANT_ID, "name": "via-mcp", "rpm_limit": 1000},
        )
        issued = json.loads(res.content[0].text)
        assert issued["key"].startswith("ag-")
        r = await client.post(
            "/v1/chat/completions", json=_body(), headers=auth_headers(issued["key"])
        )
        assert r.status_code == 200

        # 4) MCP 读预算快路径:新 key 应有 1 次扣减 18 token
        res = await session.call_tool("gateway_budget", {"key_id": issued["key_id"]})
        budget = json.loads(res.content[0].text)
        assert budget["budget_spent"] == 18 and budget["tpm_window"] == 18
