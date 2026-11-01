"""MCP stdio server:把网关运维能力开放给任意 MCP 客户端(2026 Agent 生态入口)。

工具面(读为主 + 一个受控写,演示"网关即运维面板"):
- gateway_channels():渠道健康状态(EMA/冷却/fail_streak)—— 排障第一问"慢在哪"
- gateway_usage(tenant_id):租户用量对账(账本聚合)
- gateway_budget(key_id):预算/TPM 窗口实时读数(Redis 快路径)
- gateway_issue_key(tenant_id, ...):签发 virtual key(raw key 只此一次,与管理端同纪律)

运行:python -m app.mcp_server(stdio;Claude Desktop / 任意 MCP 客户端配置本命令)
复用:与 admin API 同一层服务函数,不是旁路实现 —— 单一事实来源。
"""

import asyncio
import json
import threading

from mcp.server.mcpserver import MCPServer

from app.config import get_settings

mcp = MCPServer("agentgate-ops")

# 进程级单一持久 loop:MCPServer 在 worker 线程里跑工具(asyncio.run 会每次开/关新 loop,
# 而缓存的 asyncpg 连接绑在第一个 loop 上 → 第二次调用 "Event loop is closed",PG 口径实测暴露)。
# 单 loop + 锁:连接池与 loop 同生共死,线程安全由锁保证(MCP 工具调用本就串行)。
_loop = asyncio.new_event_loop()
_loop_lock = threading.Lock()


def _run(coro):
    with _loop_lock:
        return _loop.run_until_complete(coro)


_redis = None
_session_factory = None


def _services():
    """惰性建连(进程内单例):redis + session_factory。MCP server 生命周期独立于网关进程。"""
    global _redis, _session_factory
    if _redis is not None and _session_factory is not None:
        return _redis, _session_factory

    import redis.asyncio as aredis

    from app.db.session import build_engine, build_session_factory

    settings = get_settings()
    _redis = aredis.from_url(settings.redis_url, decode_responses=True, protocol=2)
    _session_factory = build_session_factory(build_engine(settings.db_url))
    return _redis, _session_factory


@mcp.tool(description="网关各渠道健康状态:EMA 延迟/冷却截止/连续失败数(排障第一问:慢在哪)")
def gateway_channels() -> str:
    import time

    from app.router.failover import _state_key

    settings = get_settings()
    names = settings.models  # 渠道名单以配置为准的演示口径;运行态注册渠道见 admin API

    async def run():
        redis, _ = _services()
        now_ms = int(time.time() * 1000)
        out = []
        for name in names:
            raw = await redis.hgetall(_state_key(name))
            cooling = int(raw.get("cooldown_until_ms", 0) or 0) > now_ms
            out.append(
                {
                    "channel": name,
                    "ema_ms": round(float(raw.get("ema_ms", 1000) or 1000), 1),
                    "cooling": cooling,
                    "fail_streak": int(raw.get("fail_streak", 0) or 0),
                }
            )
        return out

    return json.dumps(_run(run()), ensure_ascii=False)


@mcp.tool(description="租户用量对账:请求数/总 token/分模型明细(账本聚合)")
def gateway_usage(tenant_id: str) -> str:
    from sqlalchemy import func, select

    from app.db.models import UsageRecord

    async def run():
        _, session_factory = _services()
        async with session_factory() as db:
            total = await db.execute(
                select(func.count(UsageRecord.id), func.coalesce(func.sum(UsageRecord.total_tokens), 0))
                .where(UsageRecord.tenant_id == tenant_id)
            )
            requests_n, tokens_n = total.one()
            by_model = await db.execute(
                select(UsageRecord.model, func.count(UsageRecord.id))
                .where(UsageRecord.tenant_id == tenant_id)
                .group_by(UsageRecord.model)
            )
            return {
                "tenant_id": tenant_id,
                "requests": int(requests_n),
                "total_tokens": int(tokens_n),
                "by_model": {m: int(c) for m, c in by_model.all()},
            }

    return json.dumps(_run(run()), ensure_ascii=False)


@mcp.tool(description="key 的预算与 TPM 窗口实时读数(Redis 快路径;budget_spent=-1 表示 Redis 不可达)")
def gateway_budget(key_id: str) -> str:
    from app.quota import service as quota_service

    async def run():
        redis, _ = _services()
        try:
            spent = await quota_service.spent_tokens(redis, key_id)
            tpm = await quota_service.tpm_window_get(redis, key_id)
        except Exception as e:
            return {"key_id": key_id, "budget_spent": -1, "tpm_window": -1, "error": str(e)}
        return {"key_id": key_id, "budget_spent": spent, "tpm_window": tpm}

    return json.dumps(_run(run()), ensure_ascii=False)


@mcp.tool(description="签发 virtual key(raw key 只返回一次,服务端只存哈希;与管理端同纪律)")
def gateway_issue_key(
    tenant_id: str,
    name: str = "",
    rpm_limit: int = 60,
    tpm_limit: int = 100_000,
    budget_tokens: int | None = None,
) -> str:
    import uuid

    from app.auth.keys import generate_key
    from app.db.models import VirtualKey

    async def run():
        _, session_factory = _services()
        raw, key_hash, prefix = generate_key()
        async with session_factory() as db:
            vk = VirtualKey(
                id=uuid.uuid4().hex[:32],
                tenant_id=tenant_id,
                key_hash=key_hash,
                key_prefix=prefix,
                name=name,
                rpm_limit=rpm_limit,
                tpm_limit=tpm_limit,
                budget_tokens=budget_tokens,
            )
            db.add(vk)
            await db.commit()
            return {"key_id": vk.id, "key": raw, "prefix": prefix}

    return json.dumps(_run(run()), ensure_ascii=False)


if __name__ == "__main__":
    mcp.run()  # stdio transport
