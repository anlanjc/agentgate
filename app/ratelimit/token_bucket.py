"""Redis + Lua 令牌桶:检查与扣减在一个 EVAL 内完成,原子性由 Redis 单线程保证。

为什么必须 Redis:单机内存限流在多副本下额度放大 N 倍——4 副本 × 100rpm 实际放行 400rpm
(LiteLLM 官方文档 redis_requirements 明确记载此坑)。本项目的"零超卖"承诺指此桶:
容量为硬上界,并发扣减永不使剩余额度为负。

令牌桶参数语义:rpm_limit 为容量,补给速率 = capacity / 60(每分钟恢复满额)。
TPM 复用同一脚本(amount=实际 token 数),语义为事后计量(Higress issue #2464 记载的
同款约束:上游响应前无法预知真实 token 数;预检只能估算)。
"""

import time
from dataclasses import dataclass

import redis.asyncio as aredis

# KEYS[1]=桶键;ARGV[1]=容量 ARGV[2]=每秒补给速率 ARGV[3]=当前毫秒 ARGV[4]=本次扣减量
TOKEN_BUCKET_LUA = """
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
return {allowed, tostring(math.floor(tokens)), retry_after_ms}
"""


@dataclass
class BucketResult:
    allowed: bool
    remaining: int
    retry_after_ms: int


async def consume(
    redis: aredis.Redis,
    bucket_key: str,
    *,
    capacity: int,
    amount: int = 1,
    per_second: float | None = None,
) -> BucketResult:
    """per_second 缺省 = capacity/60(每分钟回满);测试可注入高速率验证补给。"""
    if capacity <= 0:
        return BucketResult(allowed=False, remaining=0, retry_after_ms=60000)
    rate = per_second if per_second is not None else capacity / 60.0
    now_ms = int(time.time() * 1000)
    result = await redis.eval(
        TOKEN_BUCKET_LUA, 1, bucket_key, capacity, rate, now_ms, amount
    )
    # redis-py 对 Lua 返回数组:number as int, string as str
    allowed, remaining, retry_after_ms = int(result[0]), int(result[1]), int(result[2])
    return BucketResult(allowed=allowed == 1, remaining=remaining, retry_after_ms=retry_after_ms)
