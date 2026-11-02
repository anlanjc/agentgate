"""每 key 并发上限(P2 hardening):在途请求数硬上界,RPM/TPM 之外的第三道闸。

设计:
- 计数器 `conc:{key_id}`,acquire=INCR+上限检查(超限 DECR 回滚),release=DECR(下限 0);
- 崩溃泄漏有界:acquire 时 EXPIRE 300s —— 进程死亡留下的 +1 至多让配额收紧到 TTL
  过期;流量停止后自然清零;正常路径由 _account(响应完成后)释放;
- 计数语义是"网关在途",不是"上游并发"——流式长连接同样占坑,这正是要限的东西。
"""

from redis.exceptions import RedisError

from app.log import get_logger

logger = get_logger("concurrency")

LEAK_TTL_SECONDS = 300

ACQUIRE_LUA = """
local v = redis.call('INCR', KEYS[1])
if v > tonumber(ARGV[1]) then
    redis.call('DECR', KEYS[1])
    return 0
end
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
return 1
"""

RELEASE_LUA = """
local v = redis.call('DECR', KEYS[1])
if v < 0 then
    redis.call('SET', KEYS[1], 0)
    return 0
end
return v
"""


def _key(key_id: str) -> str:
    return f"conc:{key_id}"


async def acquire(redis, key_id: str, limit: int) -> bool:
    try:
        return bool(
            await redis.eval(ACQUIRE_LUA, 1, _key(key_id), limit, LEAK_TTL_SECONDS)
        )
    except RedisError as exc:
        # 降级语义:并发闸是保护性优化,Redis 故障时放行(与准入 fail-closed 的
        # 计费硬上界不同 —— 这里的过载由上游自身的背压兜底)。
        logger.warning("concurrency_acquire_degraded", error=str(exc))
        return True


async def release(redis, key_id: str) -> None:
    try:
        await redis.eval(RELEASE_LUA, 1, _key(key_id))
    except RedisError as exc:
        logger.warning("concurrency_release_degraded", error=str(exc))
