"""精确响应缓存(对标 Higress/LiteLLM 的 cache 层;默认关闭,按部署开启)。

设计要点:
- **租户隔离是安全边界**:缓存键含 tenant_id —— 租户 A 永远读不到租户 B 的缓存答案;
- **只缓确定性请求**:temperature ≤ 0(或未设)+ 非流式 + 无 tools(工具调用有副作用);
- **TTL 惰性失效**:无主动失效(上游答案不可主动作废,语义上是"快照缓存");
  管理端可全量清空(DELETE /admin/cache);
- 命中时记账照走,channel='cache' 标注 —— 租户账本完整,上游成本为零。
"""

import hashlib
import json

from redis.exceptions import RedisError

from app.config import Settings
from app.log import get_logger

logger = get_logger("cache.response")

# 降级语义:缓存是优化不是正确性,Redis 故障一律当 miss 处理(可观测,不阻塞请求)。
# 与准入的 fail-closed 相反 —— 那里涉及计费上限,宁可拒不可漏。


def cache_enabled(settings: Settings, req) -> bool:
    if not settings.response_cache_enabled:
        return False
    if req.stream or req.tools:
        return False
    # 必须显式 temperature=0 才缓存。OpenAI 的默认温度是 1.0,把 None 当确定性
    # 会把随机答案缓存成"精确命中"——这是缓存正确性红线。
    return req.temperature is not None and req.temperature <= 0


def cache_key(tenant_id: str, model: str, payload: dict) -> str:
    """缓存键:租户 + 模型 + 归一化请求体(exclude stream/stream_options 传输字段)。

    user 参与键:同租户下不同终端用户不共享响应缓存(账号级隐私边界)。
    """
    normalized = {k: v for k, v in payload.items() if k not in ("stream", "stream_options")}
    digest = hashlib.sha256(
        json.dumps([tenant_id, model, normalized], sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()
    return f"resp:{digest}"


async def get(redis, key: str) -> dict | None:
    try:
        raw = await redis.get(key)
    except RedisError as exc:
        logger.warning("response_cache_read_degraded", error=str(exc))
        return None
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except ValueError:
        return None


async def put(redis, key: str, body: dict, ttl: int) -> None:
    if ttl <= 0:
        return
    try:
        await redis.set(key, json.dumps(body, ensure_ascii=False), ex=ttl)
    except RedisError as exc:
        logger.warning("response_cache_write_degraded", error=str(exc))


# ---------- 单飞锁(P2 hardening):同 prompt 并发 miss 只放一个去上游 ----------

LOCK_TTL_MS = 3000
POLL_INTERVAL_S = 0.05
POLL_BUDGET_S = 1.0


def _lock_key(key: str) -> str:
    return f"{key}:lock"


async def try_acquire(redis, key: str) -> bool:
    """SETNX 占锁(3s 自愈);持锁者负责去上游并填缓存。"""
    try:
        return bool(await redis.set(_lock_key(key), "1", nx=True, px=LOCK_TTL_MS))
    except RedisError as exc:
        logger.warning("singleflight_acquire_degraded", error=str(exc))
        return True  # 降级:无锁执行(与无单飞行为一致)


async def release(redis, key: str) -> None:
    try:
        await redis.delete(_lock_key(key))
    except RedisError as exc:
        logger.warning("singleflight_release_degraded", error=str(exc))


async def wait_for_fill(redis, key: str, budget_s: float = POLL_BUDGET_S) -> dict | None:
    """短轮询等持锁者填缓存;预算(1s)耗尽则返回 None,调用方自行去上游。

    设计取舍:不无限等待(上游挂了持锁者填不出,等待方不该陪葬),也不在网关层
    挂起合并请求(需要 per-key futures,复杂度不匹配当前规模;诚实边界)。
    """
    import asyncio
    import time as _time

    deadline = _time.monotonic() + budget_s
    while _time.monotonic() < deadline:
        cached = await get(redis, key)
        if cached is not None:
            return cached
        await asyncio.sleep(POLL_INTERVAL_S)
    return None
