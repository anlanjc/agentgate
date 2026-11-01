# ADR-002 存储策略:SQLAlchemy 方言中立,SQLite 测试 + PostgreSQL 生产

状态:已接受 · 2026-11-01 · 关联:[PROGRESS.md](../../PROGRESS.md) 环境约束节

## 背景

2026-11-01 环境侦察:开发机无 Docker、无 PostgreSQL、无 Redis(pip 走阿里镜像,网络慢,PG 便携包 ~350MB 下载不现实)。而"去 demo 化"红线里 SQLite-only 是反面信号;项目卖点之一是多副本一致性(需要真实 Redis)。

## 决策

1. **ORM 层**:SQLAlchemy 2.0 async,模型只用通用类型(String/Integer/BigInteger/Boolean/DateTime/JSON),**禁止 PG 专属特性**(JSONB/ARRAY/PG 方言函数),保证 aiosqlite 与 asyncpg 双驱动可跑。`DB_URL` 从配置注入:默认 `sqlite+aiosqlite:///./agentgate.db`(零依赖起步),生产 `postgresql+asyncpg://...`(docker-compose 交付)。
2. **迁移**:Alembic 以 PG 为基准生成;测试环境用 `metadata.create_all`(快、够用),两者并存,CI 里跑 PG(compose service)。
3. **Redis**:redis-py asyncio,协议层使用 Redis 5+ 能力(Streams/Lua);本机用便携 redis-server 5.0.14(tporadowski 构建,独立目录、不改系统)跑集成测试;CI 与生产用官方 redis:7 镜像。单测可用 fakeredis(+lupa 跑 Lua),但**集成口径以真 Redis 为准**,fakeredis 只做离线兜底——两者结果分列标注。
4. **诚实边界**:并发 1000 零超卖等数字,今日跑在 SQLite + 便携 Redis 上;Docker 就绪后同一测试在 PG 上复跑,两口径分列不混标(README/EVIDENCE 标注环境)。

## 后果

- 正面:今日全流程可测试、可交付;切换 PG 是配置而非代码;测试快(文件 DB)。
- 负面:不能用 PG 原生 UPSERT/JSONB(用 SQLAlchemy 通用 upsert 语义实现);账单表分区等 PG 特性进 backlog(M3 后)。
- 风险:并发行为 SQLite 与 PG 有差异(SQLite 写锁)——并发扣减测试的权威口径放 Redis Lua(限流)+ 余额扣减走单写者后台任务,规避 SQLite 写并发盲区,并在测试注释中说明。
