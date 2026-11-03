# ADR-007 语义缓存双后端:Redis 暴力余弦(默认)与 pgvector KNN

状态:已接受 · 2026-11-04 · 关联:[ADR-002](ADR-002-storage-strategy.md)(双口径纪律)

## 背景

语义缓存(P5+)需要"问题向量化 + 相似度召回"。可用方案:Redis 暴力余弦(现状,Redis 5.0 便携版无 RediSearch 模块)、RediSearch KNN(需 Redis Stack)、pgvector KNN(需 PG 扩展)。

## 决策

**双后端并存,配置切换**(`AGENTGATE_SEMANTIC_BACKEND=redis|pgvector`,默认 redis):

| 后端 | 机制 | 规模 | 口径 |
|---|---|---|---|
| redis(默认) | ZSET 索引 + 暴力余弦 O(N),上限 200 条 | 演示/小流量 | SQLite 测试 + 便携 Redis 均可跑 |
| pgvector | PG `vector` 列 + `<=>` 余弦距离 KNN + HNSW 索引 | 百万级 | 仅 PG 口径(SQLite 无该扩展,测试诚实 skip) |

- 两后端**同一契约**(embedder 协议、门控、租户隔离键、context_digest 串答案防护、降级当 miss),调用侧按配置分发——换后端不动业务代码。
- pgvector 表由 `ensure_table` 幂等管理(`CREATE EXTENSION IF NOT EXISTS vector` + HNSW),**不进 alembic**:vector 维度是 embedder 的属性,运行时才可知;显式配置 `semantic_pg_vector_dim`,不从首条数据猜——错维度的 HNSW 索引比没有索引更糟。
- pgvector 实测口径:内嵌 PG 16.2 自带 vector 0.6.2;KNN 往返 + 跨租户不召回有真库测试。

## 为什么不整体迁到 pgvector

1. **双口径纪律**:测试默认 SQLite 方言(ADR-002),pgvector 会把语义缓存测试锁死在 PG 口径——保留 Redis 后端让全套测试在两种方言下都可复现。
2. **部署面**:默认后端零新增依赖(不需要 PG 装扩展);pgvector 是生产可选,不是门槛。
3. **规模诚实**:演示/校招项目的问答重复度,200 条暴力扫描 P99 在毫秒级;真到百万条,切配置即可,接口不变(预留边界)。

## 为什么不是 RediSearch

需要 Redis Stack(模块),与"便携 Redis 5.0 也能跑"的本机口径冲突;pgvector 搭 PG(生产本就有的存储)不引入新组件。若生产 Redis 已是 Stack,加第三后端的接口位已预留。

## 后果

- 正面:规模化路径从 backlog 变成已实现且可配置;"两个后端同一语义"本身就是面试讲点(抽象边界+双口径纪律的落地案例)。
- 负面:两套实现要同步维护语义(契约注释 + 各自测试钉死);pgvector 的 TTL 靠查询侧 `created_at` 过滤(数据不过期,需定期清理——backlog: pg_cron 或应用侧 sweep)。
- 风险:HashEmbedder(词面 bigram)在两个后端下的相似度分布不同,阈值(默认 0.92)需按 embedder 调——测试用 0.75 保证确定性,生产接 DashScopeEmbedder 后按真实分布校准。
