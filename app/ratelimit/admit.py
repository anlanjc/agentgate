"""准入脚本:限流桶扣减 + TPM 窗口读取 + 预算读取,单次 Lua 往返完成(调优产物)。

调优前:consume(EVAL) + tpm GET + budget GET = 3 次往返;
调优后:1 次 EVALSHA。语义与拆分实现完全一致(有测试钉死)。
"""

from dataclasses import dataclass

from app.ratelimit.lua import eval_cached

# KEYS[1]=限流桶 KEYS[2]=tpm窗口 KEYS[3]=预算
# ARGV[1]=容量 ARGV[2]=速率 ARGV[3]=now_ms ARGV[4]=扣减量
ADMIT_LUA = """
local key    = KEYS[1]
local cap    = tonumber(ARGV[1])
local rate   = tonumber(ARGV[2])
local now    = tonumber(ARGV[3])
local amount = tonumber(ARGV[4])

local bucket = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(bucket[1])
local ts     = tonumber(bucket[2])
if tokens == nil then
    tokens = cap
    ts = now
end

local elapsed = now - ts
if elapsed < 0 then elapsed = 0 end
tokens = math.min(cap, tokens + elapsed / 1000.0 * rate)

local allowed = 0
local retry_after_ms = 0
if tokens >= amount then
    tokens = tokens - amount
    allowed = 1
else
    retry_after_ms = math.ceil((amount - tokens) / rate * 1000)
end

redis.call('HMSET', key, 'tokens', tokens, 'ts', now)
local ttl_ms = math.ceil(cap / rate * 2 * 1000)
if ttl_ms < 1000 then ttl_ms = 1000 end
redis.call('PEXPIRE', key, ttl_ms)

local tpm = tonumber(redis.call('GET', KEYS[2]) or '0')
local budget = tonumber(redis.call('GET', KEYS[3]) or '0')
return {allowed, tostring(math.floor(tokens)), retry_after_ms, tpm, budget}
"""


@dataclass
class AdmitResult:
    rpm_allowed: bool
    rpm_remaining: int
    retry_after_ms: int
    tpm_window: int
    budget_spent: int


async def admit(
    redis,
    *,
    key_id: str,
    capacity: int,
    amount: int = 1,
) -> AdmitResult:
    import time

    from app.quota.service import budget_key, tpm_key

    if capacity <= 0:
        return AdmitResult(False, 0, 60000, 0, 0)
    rate = capacity / 60.0
    now_ms = int(time.time() * 1000)
    result = await eval_cached(
        redis, ADMIT_LUA, 3,
        f"rl:rpm:{key_id}", tpm_key(key_id), budget_key(key_id),
        capacity, rate, now_ms, amount,
    )
    return AdmitResult(
        rpm_allowed=int(result[0]) == 1,
        rpm_remaining=int(result[1]),
        retry_after_ms=int(result[2]),
        tpm_window=int(result[3]),
        budget_spent=int(result[4]),
    )
