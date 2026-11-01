# 手撕卡 · agentgate 核心三段(白板面试用)

> 用法:面试官说"写一下"时,这三段是本项目最值得手写的核心——每段都配"白板讲法"
> (先说哪句话,再落哪行笔)。与仓库真实代码逐字同源,不是背的伪代码。

## 卡 1 · Redis+Lua 令牌桶(限流硬上界)

```lua
-- KEYS[1]=桶键;ARGV: 容量/每秒补给/当前毫秒/扣减量
local key  = KEYS[1]
local cap  = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local now  = tonumber(ARGV[3])
local amt  = tonumber(ARGV[4])

local bucket = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(bucket[1]) or cap      -- 首次:满桶
local ts     = tonumber(bucket[2]) or now

tokens = math.min(cap, tokens + (now - ts) / 1000.0 * rate)  -- 惰性补给

local allowed, retry_ms = 0, 0
if tokens >= amt then
    tokens = tokens - amt
    allowed = 1
else
    retry_ms = math.ceil((amt - tokens) / rate * 1000)  -- 精确 Retry-After
end

redis.call('HMSET', key, 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', key, math.max(1000, cap / rate * 2000))  -- 2×补给窗口自清理
return {allowed, tostring(math.floor(tokens)), retry_ms}
```

**白板讲法**:①先画桶——为什么令牌桶不是计数器(支持突发+平滑补给);
②指 HMGET→HMSET 一段:"检查和扣减在一个脚本里,Redis 单线程保证原子——
这就是多副本下额度不会放大 N 倍的原因,单机内存限流 4 副本就是 4 倍额度,
LiteLLM 官方文档记载的这个坑是我们上 Redis 的理由";
③指 retry_ms:"拒绝时把精确等待时间还给客户端,不是拍脑袋 429";
④指 PEXPIRE:"桶自己会过期,冷租户不占内存"。
**仓库出处**:`app/ratelimit/token_bucket.py`;测试 `test_bucket_concurrent_zero_oversell`(并发 100 恰放行 50)。

## 卡 2 · 双窗口滑动 TPM(计量窗口)

```lua
-- KEYS: 当前窗(hash) / 上一窗(string) / 估计值(string)
local add  = tonumber(ARGV[1]); local now = tonumber(ARGV[2])
local win  = tonumber(ARGV[3])

local prev  = tonumber(redis.call('GET',     KEYS[2]) or '0')
local meta  = redis.call('HMGET', KEYS[1], 'count', 'start')
local count = tonumber(meta[1]) or 0
local start = tonumber(meta[2]) or 0

if start == 0 or now - start >= win then      -- 窗口轮转
    prev  = count
    redis.call('SET', KEYS[2], prev, 'PX', win)
    count, start = 0, now
end

count = count + add
redis.call('HMSET', KEYS[1], 'count', count, 'start', start)
redis.call('PEXPIRE', KEYS[1], win * 2)

local w = 1 - (now - start) / win             -- 上一窗按剩余占比衰减
local est = math.floor(prev * w + count)
redis.call('SET', KEYS[3], est, 'PX', win * 2) -- 估计值供准入预检 GET
return est
```

**白板讲法**:①先说为什么从固定窗口升级——"固定 60s 窗口在交界处能放过 ~2×,
这是我文档里诚实标注的局限,后来自己把它做掉了"——局限→实测→修复的完整叙事;
②画两个窗:"上一窗按剩余时间线性衰减 + 当前窗全额,近似滑动窗口,O(1) 两键轮转";
③指 KEYS[3]:"准入预检只读一个字符串键,记账脚本同步维护它——预检和记账永远一致"。
**仓库出处**:`app/quota/service.py`;测试 `test_tpm_window_429`(18×3=54 后第 4 发 429)。

## 卡 3 · Streams 崩溃恢复(at-least-once → 幂等 = exactly-once)

```python
async def _reclaim_stale(self):
    # 崩溃恢复:认领 idle 超时的 pending 消息(崩溃的幽灵消费者留下的)
    pending = await self._redis.xpending_range(
        TASK_STREAM, GROUP, min="-", max="+", count=self._batch)
    stale = [p["message_id"] for p in pending
             if p["time_since_delivered"] >= self._claim_idle_ms]
    if not stale:
        return
    claimed = await self._redis.xclaim(
        TASK_STREAM, GROUP, self._consumer,
        min_idle_time=self._claim_idle_ms, message_ids=stale)
    for msg_id, fields in claimed:
        await self._process(msg_id, fields, reclaimed=True)

async def _process(self, msg_id, fields, reclaimed=False):
    record = await self._load_task(fields["task_id"])
    if record.status == "done":                     # 幂等:已完成直接 ACK 丢重复
        await self._redis.xack(TASK_STREAM, GROUP, msg_id)
        return
    record.attempts += 1
    try:
        await self._handler(fields["type"], json.loads(fields["payload"]))
        record.status = "done"; await db.commit()
        await self._redis.xack(TASK_STREAM, GROUP, msg_id)   # 成功才 ACK
    except Exception as exc:
        if record.attempts >= self._max_attempts:
            record.status = "dead"                  # 死信:进 DLQ 再 ACK
            await self._redis.xadd(DLQ_STREAM, {...})
            await self._redis.xack(TASK_STREAM, GROUP, msg_id)
        # 否则不 ACK:留在 PEL,等下一轮 reclaim 重试
```

**白板讲法**:①先立语义:"投递是 at-least-once,不丢的代价是可能重复——所以不重的
责任在消费端:状态机 done 检查 + 账本幂等键 UNIQUE,两层兜底";
②指 reclaim:"失败和崩溃统一成一条恢复路径——没 ACK 的都躺在 PEL,XCLAIM 按
idle 时间认领,不需要单独的失败处理机";③指死信:"重试到上限进 DLQ 再 ACK,
原流不留垃圾"。④被问"为什么不用 Celery":"这套语义用 Celery 是三个配置项,
黑盒讲不透;自研 200 行每行可讲——规模到了接口不变换实现"。
**仓库出处**:`app/queue/streams.py`;测试三件套(重复投递吞掉/幽灵消费者收编/死信)。
