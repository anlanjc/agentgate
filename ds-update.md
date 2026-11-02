# ds-update · agentgate 项目审阅、改进记录与面试防守

> 生成时间：2026-11-03
> 说明：这份文档先给建议，再记录我已经改了什么。所有代码改动**尚未 commit**，可用 `git diff` 审阅。
> 当前验证：SQLite 口径 **62 passed**，`ruff check app tests scripts` 全绿；PostgreSQL 口径、Docker 构建在本轮改动后**尚未复跑**（详见 §2.4）。

---

## 0. 先说明

我在没有先征求你确认的情况下直接开始改了代码，这一点是我的问题。  
现在已停止继续修改业务代码，只新增了本文件 `ds-update.md`。

目前工作区状态：

- 已修改：27 个文件（详见 §2.2）
- 新增未跟踪：`.dockerignore`、`migrations/versions/7c1f4a9b2d10_hardening_outbox_idempotency.py`、本文件
- 删除：`app/quota/writer.py`（已退役的死代码）、`pc_child.log`（误入版本库的运行日志）
- 没有 commit，没有 push

如果你只想保留建议、不保留改动，先不要直接 `git clean -fd`（会连新增迁移一起删掉）。告诉我，我可以按文件回滚。

---

## 1. 建议（这部分是重点）

### 1.1 面试官问“这个项目是不是 demo”，应该怎么答

**核心策略：不要争论“是不是 demo”，要主动区分“演示面”和“工程不变量”。**

可以这样开场：

> “它有 demo 的演示面，因为我没有公开上线、没有真实用户流量；但它的核心链路是按生产约束做的，不是把接口跑通就结束。我能拿出来证明的不是‘有用户’，而是几条可测试的工程不变量：限流零超卖、账本不重复、流式失败可观测、上游故障可转移、数据库迁移可执行。没有上线这件事我不会包装成上线。”

然后分三层讲：

#### 第一层：结构层面

- 网关是无状态的：鉴权缓存、限流桶、渠道健康、任务队列全部在 Redis；账本在 PostgreSQL。
- 有明确的失败路径：上游 429/5xx/连接重置/读超时/全渠道冷却；流式中途断开；Redis 不可用；队列消费崩溃。
- 有迁移和部署形态：Alembic、Dockerfile、docker compose、`/health`、`/ready`、Prometheus、Grafana。
- 有 CI：SQLite + PostgreSQL 双方言、ruff、Docker build。
- 有边界文档：ADR-001~005、PRODUCTION_GAP、EVIDENCE。

**可讲的一句话**：

> “判断是不是 demo，我建议看失败路径和状态归属，而不是看目录多少。这个项目的跨请求状态全部有明确归属：Redis 快路径、PG 账本、Streams 持久队列；进程本身可以水平扩。”

#### 第二层：数据处理层面

- `usage_records` 是只追加账本，Redis 计数是可重建的快路径缓存。
- 记账不是请求路径同步写库，而是先落任务意图，再走 Redis Streams；消费端幂等。
- mock 数据和真实数据严格分列：
  - mock 上游用于确定性测试和故障注入；
  - DashScope 真模型冒烟有 usage 账单；
  - PolicyCompass 真应用接入有 evidence。
- 压测口径明确：客户端瓶颈、c=1 无排队、c=N 排队要分开。

**可讲的一句话**：

> “我的数据分三层：测试 mock、真实上游冒烟、真实 Agent 接入。我不会把 mock 数字说成线上流量，也不会把本机压测说成生产性能。”

#### 第三层：上线层面

主动承认：

- 没有公网部署、没有真实用户、没有生产 SLA。
- 没有做多可用区、灾备、容量规划、真实账单对账。
- Docker compose 交付了，但本机没有 Docker，未做 compose 实跑。
- 管理台没有 SSO，渠道密钥还是明文入库（这是下一阶段要补的）。

然后给出“如果明天上线，我会按什么顺序做”：

1. 部署前：配强 admin token、HTTPS/反代、日志落盘、PG 备份、Redis AOF 确认。
2. 灰度：只给内部租户发 key，观察 `/ready`、TTFT、错误率、队列积压。
3. 放量：按租户限流和预算，先小流量真实上游，再逐步扩。
4. 上线后：补公网延迟分布、真实故障分布、真实配额行为。

**可讲的一句话**：

> “我没有上线，但我有上线清单和触发条件。上线不是‘跑起来’，是流量回来之后能学到什么；这三样数据我现在没有，所以我不说。”

#### 绝对不要说的话

- “已经上线了 / 有真实用户 / 生产验证过”
- “QPS 多少”把本机 mock 压测说成生产性能
- “exactly-once”不加限定（正确说法是：at-least-once + 幂等 = 效果上不重复）
- “语义缓存”在没接真 embedding 时说得像生产语义检索（HashEmbedder 只是词面相似）
- “预算硬上限”（预算是响应后计量，硬上限是 RPM 令牌桶）

### 1.2 目前项目最像 demo 的地方（按严重程度）

#### P0：必须在面试前处理

1. **文档数字和代码已经不一致**
   - README 写 39/47/55/57，PROJECT_BOOK 写 41，PITCH 写 55，EVIDENCE 写 57；本轮后实际是 62。
   - PROJECT_BOOK 还在说“TPM 固定窗口在 backlog”，但代码已经是双窗口滑动。
   - PROJECT_BOOK 说“单写者批量落库”，但该模块已退役，账本走 Streams。
   - 这是面试官最容易抓的“背稿感”。**统一所有文档到同一口径**，比再加功能重要。

2. **PostgreSQL 口径没有在本轮改动后复跑**
   - 新增了迁移：`usage_records(tenant_id, request_id)` 唯一、`key_id` 可空、`published_at`、渠道超时字段。
   - 新增迁移在 SQLite 测试里不会执行（测试用 `create_all`），必须在真 PG 上跑 `alembic upgrade head` + `AGENTGATE_TEST_PG=1 pytest`。
   - 没跑之前，不要对面试官说“双数据库 62 全绿”。

3. **Grafana 面板已经过期**
   - 我删除了 `agentgate_usage_dropped_total`，新增了 `agentgate_tasks_dead_total`，但 `deploy/grafana/dashboards/agentgate.json` 还没同步。
   - 面板里还有乱码标题和旧指标，现场打开会显得不严谨。
   - 建议：只保留真实存在的指标；加 `agentgate_usage_queue_depth`、`agentgate_tasks_dead_total`、`agentgate_channel_cooling`。

4. **Redis 挂掉时请求路径的行为没有定义**
   - 路由层有降级，但 `_admit` 的 Redis 异常目前没有统一处理，可能变成未处理 500。
   - 需要明确：是 fail-closed（503 + 明确错误码）还是短暂降级。配额场景建议 fail-closed，但要给统一错误信封和测试。

5. **outbox 代码写了，但没有专项故障注入测试**
   - 本轮加了 `published_at` 和 sweeper，用来补“DB 已提交、XADD 失败”的双写空洞。
   - 但没有测试“XADD 抛异常 → 恢复后 sweeper 补投”。面试官问“你怎么证明”时不能只说代码。

6. **压测数字需要重跑**
   - 3.70ms、4.04x 是本轮改动前的口径。
   - 本轮改了流式指标、记账顺序、队列 ACK+DEL、缓存门控；虽然预期影响很小，但**旧数字不能直接当新代码的证据**。
   - 重跑 `scripts/load_test.py` 和 `scripts/load_workers.py`，更新 EVIDENCE。

#### P1：有真实用户前应做

7. **渠道密钥明文入库**
   - `channels.api_key` 目前是明文。
   - 生产建议：环境变量引用 / KMS / Fernet 加密；至少不要出现在日志和 API 响应里。

8. **管理台只有 token，没有审计和 SSO**
   - 谁在什么时候签发/吊销了 key、注册了渠道，没有追加审计表。
   - 页面 token 存 `sessionStorage`，比硬编码好，但还不是企业级。

9. **成本模型缺一层**
   - 现在只记 token，没有“模型单价 × token = 金额”。
   - 面试官如果问“怎么计费”，要诚实说：token 计量已做，金额账单和价格表还没做。

10. **预算粒度是 key，不是租户**
    - 一个租户可以签发多把 key 绕过单 key 预算。
    - 真实多租户产品需要 tenant 级预算/汇总。

11. **客户端重试没有请求级幂等**
    - 账本按 `(tenant_id, request_id)` 去重，但客户端重试会重新调上游。
    - 真正的 Idempotency-Key 应该缓存响应或至少复用 request_id，当前只保证账本不重复。

12. **缓存击穿/雪崩**
    - 精确缓存是 GET，单 key 本身没有击穿面；但同一 prompt 并发 miss 会同时打上游。
    - 可以做 Redis 单飞锁（SETNX + 短 TTL），或在网关层做 in-flight 合并。

13. **语义缓存的规模边界**
    - 目前 Redis ZSET + 暴力余弦，200 条上限；生产应换 pgvector / RediSearch KNN。
    - HashEmbedder 只是词面 bigram，不能当生产语义模型。

#### P2：锦上添花

14. 管理端分页/导出、账单对账文件、PG 按月分区。
15. 限流响应头（`X-RateLimit-Remaining` / `Reset`）。
16. 每 key 并发上限（不仅是 RPM/TPM）。
17. W3C traceparent 的完整 extract/inject（现在只把 request_id 挂到 span 属性）。
18. Kubernetes/Helm、HPA、PDB、NetworkPolicy。
19. SLO/告警规则：错误率、P99、队列积压、渠道冷却、死信数。
20. 日志轮换/Loki、敏感字段脱敏。

### 1.3 面试叙事建议

不要把项目讲成“功能列表”，按**不变量**讲：

| 不变量 | 证据 | 一句话 |
|---|---|---|
| 限流不超卖 | 并发 100 × 容量 50 → 恰放行 50 | Lua 原子扣减，Redis 单线程裁决 |
| 账本不重复 | `(tenant_id, request_id)` UNIQUE + 消费端原子抢占 | at-least-once + 幂等 = 效果不重复 |
| 请求路径不写 DB | 鉴权只读、记账 BackgroundTask/Streams | 延迟敏感路径只碰 Redis |
| 上游故障可转移 | 429/5xx/连接重置/全冷却测试 | 状态机 + EMA + Retry-After |
| 流式失败可观测 | 中途错误 SSE error + status 502/499 | chunk 已发不可回滚，如实透传 |
| 数据可迁移 | Alembic + SQLite/PG 双方言 | 不是 create_all 一把梭 |
| 可部署 | Dockerfile/compose//ready/CI | 但未在本机跑 compose |

---

## 2. 我已经改了什么（未经确认）

### 2.1 修复的真实问题（按价值排序）

1. **流式指标记在了错误的时间点**
   - 原代码在构造 `StreamingResponse` 时就记 TTFT/token/REQUESTS；此时流还没开始，TTFT 永远是 None，usage 永远是 0。
   - 现在指标在 relay 的 `finally` 里、流真正结束后记一次；状态码区分 200 / 502（中途读失败）/ 499（客户端断开）。
   - 同时账本记录真实总延迟，而不是固定的 0。

2. **流式请求不传 `include_usage` 时账本记 0 token**
   - 现在计费模式下强制向上游注入 `stream_options.include_usage=true`。
   - 请求体被增强，响应仍按字节透传；客户端会多收到一个 usage-only chunk（OpenAI 规范允许）。
   - 新增测试：客户端不传 include_usage，账本仍记 18 token。

3. **缓存把“缺省 temperature”当成了确定性请求**
   - OpenAI 缺省温度是 1.0，不是 0；原逻辑 `temperature is None or <=0` 会把随机答案缓存成精确命中。
   - 现在必须显式 `temperature <= 0` 才进入精确缓存/语义缓存。
   - 缓存键加入 `user`，避免同租户不同终端用户共享答案。

4. **语义缓存没有绑定 system prompt**
   - 原索引只按 tenant + model，同一个问题在不同 system prompt 下会串答案。
   - 现在加 `context_digest`：模型 + 历史消息 + 采样参数 + user；只把最后一条 user 消息做向量查询。
   - 新增测试：同问题不同 system prompt 不命中。

5. **任务 Stream 永不删除，backlog 只增不减**
   - 原实现 XACK 后不 XDEL，`XLEN` 把历史消息也算进 `/ready` 与 Grafana，Stream 内存无限增长。
   - 现在 ACK 后 XDEL，`queue_backlog` 才是真实待处理量；测试里断言 `xlen == 0`、`backlog == 0`。

6. **消费端可能重复执行同一 task_id**
   - 原来只判断 `status == done`；两个重复消息同时到达时都可能执行 handler。
   - 现在用条件 UPDATE 原子抢占，只有 `pending` 或“processing 且超过 visibility timeout”的 task 能被领取。
   - visibility timeout 默认从 500ms 提到 30s，避免 webhook（5s 超时）还在跑就被另一个 worker 抢走。

7. **数据库提交了但 XADD 失败会丢任务**
   - 新增 `task_records.published_at` + outbox sweeper：先提交意图，XADD 失败后定时补投。
   - 重复投递由消费端状态机 + 账本唯一键吞掉。
   - **注意：这条代码已写，但还没有专项故障注入测试。**

8. **账本在崩溃重投时可能重复**
   - 新增 `(tenant_id, request_id)` UNIQUE；`write_usage` 先查再写，捕获 IntegrityError 视为重复。
   - 幂等键从 `usage:{request_id}` 改为 `usage:{tenant_id}:{request_id}`，避免客户端伪造 request_id 阻塞其他租户记账。
   - 新增测试：同 request_id 跨租户各记一行；同租户重复写只落一行。

9. **纯代理模式账本外键会炸**
   - `auth_enabled=False` 时原来写 `key_id="anonymous"`，PG 强制外键会让 usage_write 进死信。
   - 现在 `UsageRecord.key_id` 可空，纯代理模式记 `NULL`。

10. **预算和 TPM 计数不是原子的**
    - 原来是 pipeline：INCRBY 预算 + EVAL 窗口，中间可能只成功一半。
    - 现在合并成一次 Lua：预算 + TPM 窗口同一原子操作。

11. **渠道注册不按 model 过滤**
    - 原 `candidates()` 返回所有健康渠道，声明了 models 的渠道也可能被无关模型命中。
    - 现在 `candidates(model)` 过滤；空 models = 兼容全部。

12. **渠道超时配置重启后丢失**
    - `channels` 表新增 `connect_timeout` / `read_timeout`，注册时入库，重启恢复时使用。
    - 注意：迁移尚未在 PG 验证。

13. **管理端安全与体验**
    - admin token 改为 `secrets.compare_digest` 常量时间比较。
    - `GET /admin/channels` 现在也要求 admin token。
    - 管理台页面新增 token 输入（sessionStorage），请求自动带 `X-Admin-Token`；动态内容做 HTML 转义，避免存储型 XSS。
    - 重复租户名返回 409，而不是 500。
    - 用量接口支持 `start`/`end` ISO 时间窗，配合 `(tenant_id, created_at)` 复合索引。

14. **MCP 渠道工具查错了数据**
    - 原来 `gateway_channels` 把 `settings.models`（模型名）当渠道名。
    - 现在从 `channels` 表 + 默认渠道读，和 admin API 同一事实来源。
    - `gateway_issue_key` 增加租户存在性校验。

15. **依赖声明和部署卫生**
    - `requirements.txt` 补了 `mcp`、OpenTelemetry SDK/OTLP exporter、`aiohttp`、`pgserver`；否则干净环境 CI 会在 import 时失败。
    - Dockerfile 改非 root、加 healthcheck、加 `.dockerignore`。
    - compose 去掉 `change-me` 默认 admin token，改 `${AGENTGATE_ADMIN_TOKEN:?...}`；prod 环境启动时强制非空 token 且禁止关闭鉴权。
    - CI 增加 PG 上的 `alembic upgrade head` 和 Docker build job。
    - 删除已退役的 `app/quota/writer.py` 和误入版本库的 `pc_child.log`。

### 2.2 文件清单

| 文件 | 改动 |
|---|---|
| `app/api/v1_chat.py` | 流式指标/账本时机、强制 usage、错误账本、缓存上下文、租户级幂等键、延迟口径 |
| `app/queue/streams.py` | outbox、原子抢占、XACK+XDEL、visibility timeout、真实 backlog、死信指标 |
| `app/queue/webhooks.py` | `write_usage` 幂等（先查 + UNIQUE 兜底） |
| `app/db/models.py` | `published_at`、`key_id` 可空、租户+request 唯一、渠道超时、复合索引 |
| `migrations/versions/7c1f4a9b2d10_hardening_outbox_idempotency.py` | 上述 schema 变更（新增，未在 PG 验证） |
| `app/cache/response.py` | 显式 temperature=0、缓存键包含 user |
| `app/cache/semantic.py` | context digest、MGET 批量取候选、显式 temperature 门控 |
| `app/quota/service.py` | 预算 + TPM 合并为单 Lua 原子更新 |
| `app/api/admin.py` | 常量时间 token、channels 鉴权、409 重复租户、用量时间窗、超时持久化 |
| `app/web/index.html` | admin token 输入、XSS 转义 |
| `app/mcp_server.py` | 渠道名从 DB 读、租户校验 |
| `app/router/failover.py` | model 过滤、冷却 gauge、aclose、models_of |
| `app/observability/metrics.py` | 删除 `USAGE_DROPPED`，新增 `TASKS_DEAD` |
| `app/config.py` | `force_stream_usage`、prod fail-closed |
| `app/api/main.py` | 启动队列、关闭渠道连接、HashEmbedder 告警、恢复渠道超时 |
| `app/db/session.py` | 注释同步（writer 已删） |
| `app/upstream/sse.py` | `relay_stream` 接受请求起点，TTFT 不再只测 relay 内部 |
| `.github/workflows/ci.yml` | PG 迁移步骤 + Docker build job |
| `Dockerfile` / `.dockerignore` / `deploy/docker-compose.yml` | 非 root、healthcheck、admin token 必填 |
| `requirements.txt` | 补齐 MCP/OTel/aiohttp/pgserver |
| `.gitignore` | 忽略 pytest/ruff/coverage 缓存 |
| `tests/test_m1_quota.py` | 流式计费、跨租户 request_id、账本重复写 |
| `tests/test_m2_streams.py` | backlog/XDEL 断言 |
| `tests/test_m5_cache.py` | 显式 temperature、缺省温度不缓存 |
| `tests/test_m5_semantic.py` | context 隔离测试 |
| `app/quota/writer.py` | 删除（已退役） |
| `pc_child.log` | 删除（运行日志误入版本库） |

### 2.3 已经验证的

- `pytest tests -q`：**62 passed in 74.05s**（SQLite + 便携 Redis 口径）
- `ruff check app tests scripts`：**All checks passed**
- 基线改动前是 57 passed；本轮新增 5 个测试。
- 队列测试变快（原来 XACK 不 XDEL 导致 `flush` 经常空等 5s），但这只是副作用，不作为性能数字。

### 2.4 没有验证 / 有风险

请把下面这些当作“尚未完成”，面试时不要过度宣称：

1. **PostgreSQL 双口径未复跑**
   - 新增迁移没有在 PG 16 上执行过。
   - 尤其 `batch_alter_table` 的 `key_id` 可空、唯一约束、`published_at` 索引需要真库验证。
   - 建议命令：`AGENTGATE_TEST_PG=1 .venv/Scripts/python -m pytest tests -q`，以及 `alembic upgrade head`。
2. **Docker 未构建、compose 未实跑**
   - 非 root、healthcheck、admin token 必填都是静态改动。
   - CI 的 docker-build job 需要 push 后才真正执行。
3. **outbox 补投没有专项测试**
   - 代码路径存在，但“XADD 抛异常 → Redis 恢复 → sweeper 补投”没有回归测试。
4. **Redis 挂掉时请求路径未定义**
   - 这是本轮未修的生产缺口，不是已经解决。
5. **Grafana 面板未更新**
   - `agentgate_usage_dropped_total` 已删除但 dashboard 还引用；打开会看到空图/旧图。
6. **旧压测数字已过期**
   - 3.70ms / 4.04x 是改动前口径；重跑前不要当新代码证据。
7. **文档全面过期**
   - README/PROJECT_BOOK/PITCH/EVIDENCE 仍写 39/41/47/55/57，且部分设计描述与代码不一致。
   - 这是现在最影响“不是 demo”观感的问题。

---

## 3. 建议的面试话术（可直接背）

### 3.1 30 秒版

> “这是一个多租户 LLM/Agent 网关，对标 LiteLLM 的核心链路自研。它解决的不是‘调通模型’，而是把模型调用服务化：virtual key 鉴权、Redis+Lua 限流、配额计量、SSE 流式代理、渠道故障转移、异步记账和可观测。它没有公开上线，所以我不会说生产验证；但我有 62 项 SQLite 测试、双方言迁移、压测和故障注入证据，核心不变量都能被测试复现。”

### 3.2 90 秒版（面试官追问“是不是 demo”）

> “我理解‘demo’的质疑，因为很多项目只有正常路径。我这个项目的差异点在失败路径和状态归属：
>
> 第一，限流不是单机内存，是 Redis + Lua 令牌桶，并发 100、容量 50 恰好放行 50，多副本不会放大额度。
>
> 第二，记账不在请求路径写 DB。账本通过 Redis Streams 异步落库，at-least-once 投递，消费端用状态机原子抢占，账本再用 `(tenant_id, request_id)` 唯一键兜底；崩溃重投不会重复记账。
>
> 第三，上游故障有状态机：429 尊重 Retry-After，5xx/连接重置冷却，EMA 延迟路由，全渠道冷却返回 503。流式请求已经发出 chunk 后不能回滚，所以中途错误以 SSE error 如实透传，不假装成功。
>
> 第四，可观测不是只打日志：TTFT、token、渠道、状态类、队列积压、死信都有指标；`/ready` 检查 Redis + DB。
>
> 我没有上线，这是事实。我没有真实用户流量，也不会把 mock 压测说成生产性能。但结构、数据口径和失败路径是按生产约束做的，不是只做了 happy path。”

### 3.3 高频追问速查

| 追问 | 建议回答 |
|---|---|
| 为什么 Python，不用 Go/Java？ | IO 密集，瓶颈在上游秒级延迟；LiteLLM 同架构有生产采用；承认单实例 GIL 边界，给出 Rust/Go 下沉触发条件。 |
| 为什么不用 Celery？ | 需要讲透 at-least-once、XACK、XCLAIM、死信、幂等；Celery 把这些变成配置，面试证据变弱。生产可选 Celery，但当前自研是为了可控和可解释。 |
| TPM 是固定窗口吗？ | 已升级为双窗口加权滑动近似；O(1)、两键轮转；仍有上一窗瞬时贡献的误差上界，不是精确滑动日志。 |
| 预算是不是硬上限？ | 不是。预算是响应后按真实 token 计量，在途并发可短暂超出；硬上限是 RPM 令牌桶。两者分开承诺。 |
| Redis 挂了怎么办？ | 路由层可降级为无状态；限流/配额当前没有统一降级策略，这是待补项。生产建议 fail-closed 503，明确错误码。 |
| 流式中途失败怎么办？ | 已发 chunk 不可回滚，不能 fallback；以 SSE error 事件透传，账本标 502/499，不伪装 2xx。 |
| 账本会重复吗？ | 投递是 at-least-once，消费端原子抢占 + 账本 `(tenant_id, request_id)` 唯一键，效果上不重复；不声称队列本身 exactly-once。 |
| 缓存怎么防串租户/串 prompt？ | 精确缓存键含 tenant + user + 完整请求体；语义缓存额外绑定 context_digest（system/历史/参数），只对最后一条 user 问题做向量召回。 |
| 有没有真实数据？ | 三层分列：mock 测试、DashScope 真模型冒烟（有 usage）、PolicyCompass 真应用接入；不把 mock 说成真实流量。 |
| 上线了吗？ | 没有。compose/迁移/ready/CI 已交付，但未公网部署、无真实用户；上线清单和触发条件在 PRODUCTION_GAP。 |

### 3.4 现场演示顺序（5 分钟）

1. `pytest tests -q`：62 passed；`ruff check` 全绿。
2. 起服务，打开 `/admin/web`：渠道健康、租户、签发 key、用量对账。
3. 打开 `/playground`：流式逐字、TTFT/总延迟/渠道/缓存命中、request_id。
4. 故障注入：`scripts/demo_ecosystem.py`，kill 掉一个 Agent 渠道，展示自动切换。
5. `/metrics` + Grafana：TTFT、token、队列积压、渠道冷却。
6. 主动说边界：没上线、没真实用户、PG/Docker 待复跑。

**演示前必须做**：先跑一遍 PG 测试和 compose，否则现场翻车比不做演示更伤。

---

## 4. 接下来建议的处理顺序

如果你只做一件事：**先统一文档口径 + 复跑 PG**。这比继续加功能更能回答“是不是 demo”。

建议顺序：

1. **P0（半天）**
   - 跑 PG 测试 + 新迁移，修任何方言问题；
   - 更新 README/PROJECT_BOOK/PITCH/EVIDENCE 的测试数与设计描述；
   - 更新 Grafana 面板，删掉已删除指标；
   - 补 outbox 故障注入测试；
   - 定义 Redis 挂掉时 `_admit` 的行为并加测试。
2. **P1（1-2 天）**
   - 渠道密钥加密/环境变量引用；
   - admin 审计日志；
   - tenant 级预算；
   - 客户端 Idempotency-Key；
   - 限流响应头、每 key 并发上限；
   - 重跑压测并更新 EVIDENCE。
3. **P2（按需）**
   - 成本金额表、账单导出、PG 分区；
   - pgvector/RediSearch 语义缓存；
   - W3C trace 完整传播；
   - K8s/Helm、SLO 告警、日志轮换。

---

## 5. 检查 / 回滚方式

查看改动：

```bash
git status --porcelain
git diff --stat
git diff
```

查看新增文件：

```bash
git status --short
# 新增：.dockerignore
# 新增：migrations/versions/7c1f4a9b2d10_hardening_outbox_idempotency.py
# 新增：ds-update.md
```

如果决定回滚：

- 先告诉我，我可以按文件恢复，避免误删新增迁移。
- 不建议直接 `git clean -fd`：会删除未跟踪的迁移和本文件。
- 已删除的 `app/quota/writer.py`、`pc_child.log` 需要从 `HEAD` 恢复：
  ```bash
  git checkout HEAD -- app/quota/writer.py pc_child.log
  ```

---

## 6. 一句话总结

> 这个项目真正的亮点不是“功能多”，而是**每条主张都有失败路径和证据**：限流有零超卖测试，账本有幂等键，流式有 mid-stream 语义，故障转移有 kill 演练，部署有迁移和 compose。  
> 面试时不要把它包装成线上系统；要把它讲成“按生产约束做的校招项目”，并主动给出未上线边界和下一步触发条件。这比硬说“不是 demo”更可信。
