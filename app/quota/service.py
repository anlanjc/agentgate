"""配额服务:预算计数(快路径 Redis,账本 PG)+ 主动重建。

诚实语义(写进 README/EVIDENCE,面试主动讲):
- **限流桶 = 硬上界**(Lua 原子,零超卖);
- **预算 = 计量制**:扣减发生在响应后(真实 token 数),在途并发可短暂超出预算,
  超出上界 ≈ 在途请求数 × 单请求 token —— 这是"计量后付费"系统的通用取舍
  (严格预扣需估算 token,误差同样存在;取舍记录于 ADR-002)。
- 计数原子性由 Redis INCRBY 保证:并发 N 次扣减,计数器恰好等于 N 次之和(零丢失)。
"""

import redis.asyncio as aredis
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import UsageRecord

# TPM 固定 60s 窗口计数:INCRBY + 首次自设置 EXPIRE(原子)
# 诚实边界:固定窗口在窗口交界处可能放过 ~2×窗口量(滑动窗口/令牌桶为后续增强,记录于 EVIDENCE)
TPM_WINDOW_LUA = """
local v = redis.call('INCRBY', KEYS[1], ARGV[1])
if v == tonumber(ARGV[1]) then
    redis.call('EXPIRE', KEYS[1], 60)
end
return v
"""


def budget_key(key_id: str) -> str:
    return f"budget:{key_id}"


def tpm_key(key_id: str) -> str:
    return f"tpm:{key_id}"


async def spent_tokens(redis: aredis.Redis, key_id: str) -> int:
    value = await redis.get(budget_key(key_id))
    return int(value) if value else 0


async def reserve_tokens(redis: aredis.Redis, key_id: str, amount: int) -> int:
    """响应后原子记账;返回扣减后的累计值。"""
    return int(await redis.incrby(budget_key(key_id), amount))


async def budget_exceeded(redis: aredis.Redis, key_id: str, budget_tokens: int | None) -> bool:
    if budget_tokens is None:
        return False
    return await spent_tokens(redis, key_id) >= budget_tokens


async def tpm_window_add(redis: aredis.Redis, key_id: str, tokens: int) -> int:
    """响应后把真实 token 数计入 60s 窗口(原子);返回窗口累计值。"""
    return int(await redis.eval(TPM_WINDOW_LUA, 1, tpm_key(key_id), tokens))


async def tpm_window_get(redis: aredis.Redis, key_id: str) -> int:
    value = await redis.get(tpm_key(key_id))
    return int(value) if value else 0


async def account_tokens(redis, key_id: str, tokens: int) -> None:
    """响应后记账:预算 INCRBY + TPM 窗口,一次 pipeline 往返(调优产物)。"""
    pipe = redis.pipeline(transaction=False)
    pipe.incrby(budget_key(key_id), tokens)
    pipe.eval(TPM_WINDOW_LUA, 1, tpm_key(key_id), tokens)
    await pipe.execute()


async def rebuild_from_db(redis: aredis.Redis, db: AsyncSession, key_id: str) -> int:
    """Redis 计数丢失(重启/淘汰)后,从 PG 账本重建 —— 缓存与账本的一致性路径。"""
    row = await db.execute(
        select(func.coalesce(func.sum(UsageRecord.total_tokens), 0)).where(
            UsageRecord.key_id == key_id, UsageRecord.status == 200
        )
    )
    total = int(row.scalar_one())
    await redis.set(budget_key(key_id), total)
    return total
