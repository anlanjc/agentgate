# ADR-004 记账通道:进程内单写者 → Streams 持久队列(自吃狗粮)

状态:已接受 · 2026-11-03 · 取代 ADR-003 中"账本走进程内单写者"的 M1 形态 · 关联:[ADR-003](ADR-003-redis-streams.md)

## 背景

M1 的账本落库走进程内 `UsageWriter`(asyncio.Queue + 单消费者)。它满足了"请求路径
零 DB 写",但有一个诚实标注过的残留:**队列在进程内存里,网关崩溃 = 在途账本条目丢失**
——Redis 计数(预算/TPM)在,账本丢,重建(rebuild)后两者漂移。

而 M2 已经自研了 Streams 持久队列(at-least-once + 幂等键 + 死信),却只服务 webhook——
**核心记账反而走在弱通道上**。生产审计里这是最大的"设计降级点"。

## 决策

账本落库改走 Streams:

- `_account`(BackgroundTask)→ `enqueue("usage_write", record, idempotency_key="usage:{request_id}")`;
- 消费者 = `write_usage` 处理器(共享 session_factory 注入),插入 usage_records;
- 幂等:task_records.idempotency_key UNIQUE —— at-least-once 投递下同一请求最多落一行;
- 进程内 `UsageWriter` 退役(队列溢出丢弃、崩溃丢批两处降级语义随之消失)。

## 后果

- 正面:崩溃不丢账(消息在 Redis,AOF 持久化下重启恢复);死信兜底落库失败;验收链完整——
  "自研队列"从演示件变成核心通道(吃自己的狗粮);
- 负面:每请求多一行 task_records(任务账本,可清理);账本可见性从"flush 即达"变为
  "消费延迟毫秒级"(测试以 flush() 语义等待);
- 测试影响:`writer.flush → task_queue.flush` 全量替换;幽灵消费者时序测试改用
  `auto_start=False`(enqueue 不再隐式拉起 worker)。

## 多副本注记

同一测试批次补上"双副本限流一致性"直接证据:两 app 实例共享 Redis,合并并发 100 ×
容量 50 → 放行 50+回补(令牌桶惰性回补的正确语义,±2 容差并注明),而非放大到 100。
