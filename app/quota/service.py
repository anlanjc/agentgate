"""配额服务:预算计数(快路径 Redis,账本 PG)+ 主动重建。

诚实语义(写进 README/EVIDENCE,面试主动讲):
- **限流桶 = 硬上界**(Lua 原子,零超卖);
- **预算 = 计量制**:扣减发生在响应后(真实 token 数),在途并发可短暂超出预算,
  超出上界 ≈ 在途请求数 × 单请求 token —— 这是"计量后付费"系统的通用取舍
  (严格预扣需估算 token,误差同样存在;取舍记录于 ADR-002)。
- 计数原子性由 Redis INCRBY 保证:并发 N 次扣减,计数器恰好等于 N 次之和(零丢失)。
- **TPM = 双窗口滑动近似**(2026-11-02 升级,取代固定 60s 窗口):
  当前窗 + 上一窗加权(上一窗按剩余时间线性衰减),窗口交界不再有 ~2× 突刺;
  O(1) 读写、两键轮转,误差上界 = 上一窗在窗首的瞬时贡献(经典 weighted sliding window)。
"""

import redis.asyncio as aredis
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import UsageRecord

# 双窗口滑动 TPM:KEYS[1]=当前窗(hash) KEYS[2]=上一窗(string) KEYS[3]=估计值(string,供 admit 预检 GET)
# ARGV[1]=本次 token 数 ARGV[2]=当前毫秒 ARGV[3]=窗口长(ms)
# 返回:加权估计值(上一窗按剩余占比线性衰减 + 当前窗全额);估计值同步写入 KEYS[3] 字符串键
# (admit 单脚本读字符串键,避免 hash/string 类型冲突)
TPM_SLIDING_LUA = """
local cur   = KEYS[1]
local prev  = KEYS[2]
local est   = KEYS[3]
local add   = tonumber(ARGV[1])
local now   = tonumber(ARGV[2])
local win   = tonumber(ARGV[3])

local prev_count = tonumber(redis.call('GET', prev) or '0')
local cur_meta   = redis.call('HMGET', cur, 'count', 'start')
local cur_count  = tonumber(cur_meta[1]) or 0
local cur_start  = tonumber(cur_meta[2]) or 0

-- 窗口轮转:当前窗过期 → cur 变 prev,开新 cur
if cur_start == 0 or now - cur_start >= win then
    prev_count = cur_count
    redis.call('SET', prev, prev_count, 'PX', win)
    cur_count = 0
    cur_start = now
end

cur_count = cur_count + add
redis.call('HMSET', cur, 'count', cur_count, 'start', cur_start)
redis.call('PEXPIRE', cur, win * 2)

-- 加权滑动估计:上一窗按其在观察窗内的剩余占比衰减
local elapsed = now - cur_start
local prev_weight = 1 - (elapsed / win)
if prev_weight < 0 then prev_weight = 0 end
local estimate = math.floor(prev_count * prev_weight + cur_count)
redis.call('SET', est, estimate, 'PX', win * 2)
return estimate
"""

# 兼容别名(单写者记账与测试经 tpm_window_add 调用;窗口长 60s)
TPM_WINDOW_LUA = TPM_SLIDING_LUA
TPM_WINDOW_MS = 60_000


def budget_key(key_id: str) -> str:
    return f"budget:{key_id}"


def tpm_key(key_id: str) -> str:
    """滑动窗口估计值字符串键(admit 预检与 tpm_window_get 读这里)。"""
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
    """响应后把真实 token 数计入滑动窗口(原子 Lua);返回加权估计值。"""
    import time

    now_ms = int(time.time() * 1000)
    return int(
        await redis.eval(
            TPM_SLIDING_LUA, 3, f"{tpm_key(key_id)}:cur", f"{tpm_key(key_id)}:prev",
            tpm_key(key_id), tokens, now_ms, TPM_WINDOW_MS,
        )
    )


async def tpm_window_get(redis: aredis.Redis, key_id: str) -> int:
    """当前加权估计(准入预检读数;est 键由 tpm_window_add 维护,无流水为 0)。"""
    value = await redis.get(tpm_key(key_id))
    return int(value) if value else 0


async def account_tokens(redis, key_id: str, tokens: int) -> None:
    """响应后记账:预算 INCRBY + TPM 滑动窗口,一次 pipeline 往返(调优产物)。"""
    import time

    now_ms = int(time.time() * 1000)
    pipe = redis.pipeline(transaction=False)
    pipe.incrby(budget_key(key_id), tokens)
    pipe.eval(
        TPM_SLIDING_LUA, 3, f"{tpm_key(key_id)}:cur", f"{tpm_key(key_id)}:prev",
        tpm_key(key_id), tokens, now_ms, TPM_WINDOW_MS,
    )
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
