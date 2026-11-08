"""客户端 Idempotency-Key(P2 hardening):重试安全 —— 网关侧去重,重试不重调上游、不重计费。

语义(对齐主流支付/网关 API 的 Idempotency-Key 约定):
- 作用域:租户 + key + 客户端给的 key 字符串 —— 不同租户/key 互不干扰;
- 首次请求:SETNX 占锁 → 正常执行 → 响应(status+body)入库;
  锁 TTL 由 settings.idempotency_lock_ttl_seconds 控制(缺省 300s,≥ 渠道
  read_timeout 缺省 —— GW-3:锁必须活过合法执行期,不能比上游超时先死);
  响应重放的留存时长是另一个配置(idempotency_ttl_seconds,86_400s);
- 锁带随机属主 token(GW-3):release 是 Lua compare-and-delete,只删自己持有的锁
  —— 防止"首请求锁过期后,收尾把重试方刚拿到的锁删掉"的交叉释放;
- 释放时发现锁已不属于自己(过期被接管)= 本次执行失去 store 权,调用方据此放弃
  写重放库,避免旧响应覆盖新执行者的响应(重试方会自己 store);
- 重复请求(响应已就绪):直接回放存储的响应,带 `x-idempotent-replay: true`;
  账本记 channel="replay" 且 token=0 —— 可审计但不重计费;
- 重复请求(首次仍在途):409 conflict(客户端稍后重试),不并发执行两次;
- 范围限定:只覆盖非流式。流式响应体不可重放(SSE 已消费),带 Idempotency-Key
  的流式请求照常执行并留痕告警 —— 诚实边界,不假装支持;
- 锁的边界:未捕获异常路径下锁由 TTL 自然过期;正常路径(含错误响应)即时释放。
  心跳续租不做:TTL 配到 ≥ 最大合法执行期即覆盖,续租引入的复杂度不划算。
"""

import json
import uuid

from redis.exceptions import RedisError

from app.log import get_logger

logger = get_logger("idempotency")

# 锁 TTL 缺省 = settings.upstream_read_timeout 缺省(300s);运行时以 settings 为准。
LOCK_TTL_MS = 300_000

# GW-3:compare-and-delete —— 值等于自己的 token 才删,一进制原子(Lua)。
_RELEASE_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
end
return 0
"""

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


async def acquire(redis, sk: str, *, ttl_ms: int = LOCK_TTL_MS) -> tuple[bool, str | None]:
    """SETNX 占锁,值为随机属主 token(GW-3)。

    返回 (是否放行, 属主 token):
    - (True, token)  —— 拿到锁,token 供 release/条件 store 使用;
    - (False, None)  —— 锁被占(首次仍在途),调用方回 409;
    - (True, None)   —— Redis 故障降级:无锁保护地执行(与无 Idempotency-Key 一致),
      token 为 None,release 对此是安全 no-op。
    """
    token = uuid.uuid4().hex
    try:
        ok = await redis.set(f"{sk}:lock", token, nx=True, px=ttl_ms)
    except RedisError as exc:
        logger.warning("idempotency_lock_degraded", error=str(exc))
        return True, None
    return bool(ok), (token if ok else None)


async def release(redis, sk: str, token: str | None) -> bool:
    """释放锁(GW-3):compare-and-delete,只删自己持有的锁。

    返回 True  = 确实持有并已释放 —— 调用方才有资格写重放库(store);
    返回 False = 锁已不属于本次执行(TTL 过期被重试方接管 / 降级路径 / 重复释放)
      —— 调用方应放弃 store,避免旧响应覆盖接管者的响应。
    """
    if not token:
        return False
    try:
        return bool(await redis.eval(_RELEASE_LUA, 1, f"{sk}:lock", token))
    except RedisError as exc:
        logger.warning("idempotency_release_degraded", error=str(exc))
        return False


async def store(redis, sk: str, status: int, body: dict, ttl: int) -> None:
    try:
        await redis.set(sk, json.dumps({"status": status, "body": body}, ensure_ascii=False), ex=ttl)
    except RedisError as exc:
        logger.warning("idempotency_store_degraded", error=str(exc))
