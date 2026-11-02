"""客户端 Idempotency-Key(P2 hardening):重试安全 —— 网关侧去重,重试不重调上游、不重计费。

语义(对齐主流支付/网关 API 的 Idempotency-Key 约定):
- 作用域:租户 + key + 客户端给的 key 字符串 —— 不同租户/key 互不干扰;
- 首次请求:SETNX 占锁(60s)→ 正常执行 → 响应(status+body)入库,TTL 可配;
- 重复请求(响应已就绪):直接回放存储的响应,带 `x-idempotent-replay: true`;
  账本记 channel="replay" 且 token=0 —— 可审计但不重计费;
- 重复请求(首次仍在途):409 conflict(客户端稍后重试),不并发执行两次;
- 范围限定:只覆盖非流式。流式响应体不可重放(SSE 已消费),带 Idempotency-Key
  的流式请求照常执行并留痕告警 —— 诚实边界,不假装支持;
- 锁的边界:未捕获异常路径下锁由 TTL(60s)自然过期;正常路径(含错误响应)即时释放。
"""

import json

from redis.exceptions import RedisError

from app.log import get_logger

logger = get_logger("idempotency")

LOCK_TTL_MS = 60_000

# 降级语义同缓存:Redis 故障时跳过去重(退化为"可能重复执行"),不阻塞请求。
# 与准入 fail-closed 相反 —— 这里是优化路径,重复执行的代价由账本可见。


def storage_key(tenant_id: str, key_id: str, idem_key: str) -> str:
    return f"idem:{tenant_id}:{key_id}:{idem_key}"


async def lookup(redis, sk: str) -> dict | None:
    try:
        raw = await redis.get(sk)
    except RedisError as exc:
        logger.warning("idempotency_lookup_degraded", error=str(exc))
        return None
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except ValueError:
        return None


async def acquire(redis, sk: str) -> bool:
    try:
        return bool(await redis.set(f"{sk}:lock", "1", nx=True, px=LOCK_TTL_MS))
    except RedisError as exc:
        logger.warning("idempotency_lock_degraded", error=str(exc))
        return True  # 降级:无锁保护地执行(与无 Idempotency-Key 行为一致)


async def release(redis, sk: str) -> None:
    try:
        await redis.delete(f"{sk}:lock")
    except RedisError as exc:
        logger.warning("idempotency_release_degraded", error=str(exc))


async def store(redis, sk: str, status: int, body: dict, ttl: int) -> None:
    try:
        await redis.set(sk, json.dumps({"status": status, "body": body}, ensure_ascii=False), ex=ttl)
    except RedisError as exc:
        logger.warning("idempotency_store_degraded", error=str(exc))
