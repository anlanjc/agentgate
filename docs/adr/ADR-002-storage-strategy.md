# ADR-002 存储策略:SQLAlchemy 方言中立,SQLite 测试 + PostgreSQL 生产(双口径已验证)

状态:已接受 · 2026-11-01 · 2026-11-02 升级(真 PG 口径落地) · 关联:[PROGRESS.md](../../PROGRESS.md) 环境约束节

## 背景

2026-11-01 环境侦察:开发机无 Docker、无 PostgreSQL、无 Redis(pip 走阿里镜像,网络慢,PG 便携包 ~350MB 下载不现实)。而"去 demo 化"红线里 SQLite-only 是反面信号;项目卖点之一是多副本一致性(需要真实 Redis)。

## 决策

1. **ORM 层**:SQLAlchemy 2.0 async,模型只用通用类型(String/Integer/BigInteger/Boolean/DateTime/JSON),**禁止 PG 专属特性**(JSONB/ARRAY/PG 方言函数),保证 aiosqlite 与 asyncpg 双驱动可跑。`DB_URL` 从配置注入:默认 `sqlite+aiosqlite:///./agentgate.db`(零依赖起步),生产 `postgresql+asyncpg://...`(docker-compose 交付)。
2. **迁移**:Alembic 以 PG 为基准生成;测试环境用 `metadata.create_all`(快、够用),两者并存,CI 里跑 PG(compose service)。
3. **Redis**:redis-py asyncio,协议层使用 Redis 5+ 能力(Streams/Lua);本机用便携 redis-server 5.0.14(tporadowski 构建,独立目录、不改系统)跑集成测试;CI 与生产用官方 redis:7 镜像。单测可用 fakeredis(+lupa 跑 Lua),但**集成口径以真 Redis 为准**,fakeredis 只做离线兜底——两者结果分列标注。
4. **双口径落地(2026-11-02)**:`AGENTGATE_TEST_PG=1` 时 pytest 经 **pgserver**(内嵌 PostgreSQL 16.2 二进制,pip 可装)全量复跑;`scripts/verify_pg_migration.py` 在真 PG 上验证 Alembic 迁移。结果:**41/41 SQLite + 41/41 PostgreSQL 双口径全绿,迁移真 PG 通过**(EVIDENCE §①)。生产部署仍走 compose 的 PG16+Redis7。

## 后果

- 正面:方言中立已被双口径实测背书(不再是"理论上可切");测试快(文件 DB)+ 真 PG 覆盖兼得。
- 负面:不能用 PG 原生 UPSERT/JSONB(用 SQLAlchemy 通用 upsert 语义实现);账单表分区等 PG 特性进 backlog(M3 后)。
- 风险:并发行为 SQLite 与 PG 有差异(SQLite 写锁)——并发扣减测试的权威口径放 Redis Lua(限流)+ 余额扣减走单写者后台任务,规避 SQLite 写并发盲区,并在测试注释中说明;PG 口径下同一套并发测试同样通过。
- 运维注记:pgserver 首启 initdb 约 5s;被强杀后可能残留 postmaster.pid/进程锁,处理:taskkill postgres + 删 .pgdata 重建(测试库,无状态)。
