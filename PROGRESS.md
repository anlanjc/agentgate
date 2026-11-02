# PROGRESS · agentgate(多租户 LLM/Agent 网关)

> 项目定位:**自研 LiteLLM 核心链路的多租户 LLM/Agent 网关**——统一鉴权、限流、配额计费、流式代理、异步任务与故障转移。
> 目标岗位:AI 应用后端 / Agent 工程岗(简历第三层:检索 PolicyCompass → 多 Agent AskBI → 平台化 agentgate)。
> 设计依据:2026-11 三路实证调研(开源架构 LiteLLM/Dify/OneAPI/Higress/APISIX + 30 条 JD 词频 + 面经追问链),详见 `docs/ARCHITECTURE.md` 师承标注。

## 里程碑与验收数字(总窗口:2026-11-01 起,9h 主体框架 + 后续增量)

| 阶段 | 内容 | 验收标准 | 状态 |
|---|---|---|---|
| M0 协议代理 | OpenAI 兼容 `/v1/chat/completions` → 上游(DashScope/OpenAI 兼容),SSE 透传,request_id 贯穿,`/health` | mock 上游单测/集成测试全绿;附加延迟指标可测(<10ms 目标,口径见 EVIDENCE) | ✅ 11 测试 |
| M1 租户与配额 | PG 租户/virtual key/配额表 + Alembic;缓存优先鉴权(Redis→DB);Redis+Lua 令牌桶;异步记账(请求路径零 DB 写) | 并发 1000 次扣减对账零超卖;单机限流多副本放大问题有文档+测试 | ✅ 并发零超卖/零丢失(100/1000 量级)+4 集成测试(真 Redis) |
| M2 可靠性 | 故障转移状态机(重试→冷却/429 立即+Retry-After→fallback→延迟优先路由);Redis Streams 异步任务(consumer group/XACK/死信/幂等键) | 故障注入:kill Redis 降级不丢任务;at-least-once 下幂等不重扣;429 冷却状态机单测 | ✅ 8 测试(含真 HTTP webhook e2e + ghost consumer 崩溃恢复) |
| M3 观测与压测 | Prometheus 指标(TTFT/token/分租户分渠道)+ asyncio 压测脚本 + docker-compose + CI + README | QPS/P99/TTFT 报告落 EVIDENCE(口径:机器配置/并发模型/时长);瓶颈调优前后对比 | ✅ 附加开销 3.70ms(c=1 口径)+ 调优叙事(Redis 往返 8→5) |
| M4 生态接入 | 运行时渠道注册(admin API)+ 多进程故障演练 + Agent 服务 shim 模板;AskBI/PC 实接与 MCP server 暴露 | 演示:自网关管理自 Agent,kill 上游进程网关行为可验证 | ✅ 演练通过(kill -9 → 10ms 切换);实接留部署后一行配置 |

**总计:39 测试全绿 + DashScope 真实冒烟通过(2026-11-02,见 EVIDENCE §⑤)**

## 今日(2026-11-01)9h 窗口计划

1. 立项文档(本文件 + ADR×3 + 架构文档)→ 2. M0 全绿 → 3. M1 全绿(并发测试)→ 4. M2 全绿(故障注入)→ 5. M3(视剩余时间,至少指标+compose+README 落盘)。
顺序原则:**每个里程碑测试绿了再进下一个**;时间不够砍 M3 的压测报告深度(可次日补),不砍 M2 的故障注入。

## 环境约束(2026-11-01 侦察,诚实记录)

- Python 3.12.4 ✅;**无 Docker、无 PostgreSQL、无 Redis**(端口 5432/6379 无监听)。
- 网络:清华镜像 403、pypi.org 极慢(36KB/s)、**阿里镜像可用**;系统代理 127.0.0.1:65533 今日未开(连接拒绝)。
- 对策(ADR-002):SQLAlchemy 2.0 async 统一方言,**今日测试跑 SQLite(aiosqlite)+ 便携 Redis(已下载 tporadowski 5.0.14.1,含 Streams/Lua)**;PG via docker-compose.yml 随仓库交付,`DB_URL` 一键切换;接口层禁止任何 SQLite/PG 专属特性(用 SQLAlchemy 通用类型)。诚实边界:今日的"并发 1000 零超卖"跑在 SQLite 上,PG 口径待 Docker 就绪后用同一测试复跑——**两个口径分列,不混标**。

## 红线

- DashScope 真模型冒烟花费自设上限 **¥2**,超限先停再报;key 读 policy-compass/.env,任何输出/日志不得带 key;调用走 `trust_env=False`(该机注册表代理坑,PC 会话教训)。
- mock 与 real 数字严格分列(沿用 workspace 纪律);测试默认 mock 上游(免费、确定),real 冒烟单独标记。
- 不碰 workspace 其他项目目录;黑板(根 PROGRESS.md)登记用 [AG-1] 前缀。
- 单测覆盖的 Lua 脚本、Streams 逻辑必须可解释("为什么不用 Celery"→ ADR-003)。

## 已完成明细(追加区)

- [AG-1 2026-11-01] 立项:环境侦察(无 Docker/PG/Redis,网络三条路实测)+ 本文件 + ADR-001/002/003 + ARCHITECTURE.md。便携 Redis 下载中、依赖经阿里镜像安装中。
- [AG-2 2026-11-03] hardening 批次审阅与锁存(ds-update 轮 27 文件由 AG-2 复核后分 5 笔入库):
  ①test: PG 口径 schema 漂移防护(.pgdata 持久库旧 schema 残留致 14 假失败,会话开始 drop_all+create_all 重建;迁移验证职责分离)→ **AGENTGATE_TEST_PG=1 修复前 14 failed → 修复后 62 passed**;
  ②feat: 请求路径 hardening(流式指标挪 relay finally·TTFT 起点=进网关·499/502 语义·强制 include_usage·账本 (tenant_id,request_id) UNIQUE+租户域幂等键·显式 temperature 门控·context_digest·预算+TPM 单 Lua·prod fail-closed·迁移 7c1f4a9b2d10);
  ③feat: 队列 hardening(outbox 水位补投·条件 UPDATE 原子抢占·visibility 500ms→30s·XACK 后 XDEL 真实 backlog);
  ④feat: 管理面安全(admin 常量时间 token·渠道超时持久化·路由 model 过滤·MCP 渠道同源·管理台 XSS 转义·Dockerfile 非 root+healthcheck·compose 禁默认 token·CI 加 PG 迁移+Docker build);
  ⑤docs: 证据与审阅记录入册(ds-update.md/ROADMAP.md/pg_migration.txt 含 7c1f4a9b2d10 产物验证+幂等重跑)。
  终态:**62 passed(SQLite)+ 62 passed(PG 16.2)+ ruff 全绿 + 迁移真 PG 验证**;遗留见 ROADMAP.md(P0-B 文档口径统一/Grafana 面板、P0-C outbox 注入测试/Redis 挂 fail-closed、P1 压测重跑)。
