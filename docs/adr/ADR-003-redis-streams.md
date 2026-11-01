# ADR-003 异步任务队列:Redis Streams 自研,而非 Celery/RQ

状态:已接受 · 2026-11-01 · 关联:[ADR-002](ADR-002-storage-strategy.md)

## 背景

网关需要异步任务:用量记账落库、长任务(报表/批量)回调 webhook。Dify 用 Celery+Redis,是 AI 平台主流方案。

## 决策

自研 Redis Streams 队列(`app/queue/streams.py`):

- 生产:`XADD stream * {task_id, type, payload}`,任务账本 PG `task_records`(幂等键 UNIQUE)。
- 消费:consumer group(`XGROUP CREATE` + `XREADGROUP`),处理成功 `XACK`;失败重试 N 次后 `XADD` 进死信流 `gate:tasks:dlq`。
- 兜底:`XAUTOCLAIM` 认领超时未 ACK 的 pending 消息(min-idle-time)——崩溃恢复路径,必须有测试。
- 语义:**at-least-once**。由此推导消费端必须幂等:幂等键(请求/任务携带)在账本表 UNIQUE 约束下 INSERT OR IGNORE,重复投递不重复扣费——这是"不丢不重"故事的完整链条:at-least-once 投递 + 幂等消费 = 效果上的 exactly-once。

## 为什么不用 Celery

1. **面试证据链**:投递语义、死信、崩溃恢复是本项目的核心卖点;用黑盒框架,这些变成"Celery 配置项",追问一层就空。自研 200 行,每一行可讲解。
2. **依赖面**:Celery + kombu + billiard 引入进程模型复杂度(Windows 上 Celery 支持还差),Redis Streams 一个连接搞定。
3. **规模匹配**:网关任务量 = 每请求一条记账,Streams 单流 10k+/s 写入能力远超需求;真到 Celery/Kafka 的量级时,接口(`enqueue/claim/ack/dlq`)不变,换实现即可——在 ADR 中预留边界。

## 后果

- 正面:三件套(group/ack/auto-claim)全部可测试、可故障注入;consumer 崩溃恢复有真实测试;与限流/鉴权共用 Redis,组件面小。
- 负面:没有 Celery 的任务重试装饰器/优先级队列等现成能力——本项目用不到(重试语义在路由层做);需要自己写 consumer 循环(~100 行)与优雅停机(SIGTERM 处理)。
- 风险:Streams 消费者在 Windows 便携 Redis 5.0 上验证(XAUTOCLAIM 为 6.2+ 才有)——**便携 Redis 只验证 group/ack/pending 读路径,XAUTOCLAIM 用 fakeredis 单测覆盖 + CI redis:7 集成覆盖**,口径分列记录。
