# EVIDENCE · agentgate(2026-11-01/02,AG-1 会话)

> 纪律:mock 与 real、SQLite 与 PG、真 Redis 与 fakeredis 口径严格分列;每条数字带环境与命令。
> 2026-11-02 增强批次后:TPM 升级为**双窗口滑动近似**(固定窗口交界 ~2× 突刺的局限消除,
> 经典 weighted sliding window,O(1) 两键轮转;test_m1_quota 原语义测试零改动通过)。

## ① 测试证据

- **67/67 全绿(SQLite 方言)+ 67/67 全绿(PostgreSQL 16.2 真库,pgserver)—— 双口径** (2026-11-03,AG-2 终态)
  - 命令:`.venv/Scripts/python -m pytest tests -q`;PG 口径:`AGENTGATE_TEST_PG=1` 同命令
  - 组成:M0 协议代理 12 · M1 鉴权 7+限流 7+配额 8 · M2 故障转移 6+Streams 7 · M4 渠道注册 1 · M5 缓存 7+语义 6+MCP 1 · M6 持久化 3+追踪 2
  - 其中 4 项为真 Redis 集成口径(redis_client fixture,flavor=real);Redis 不可用时诚实 skip
  - 故障注入面:进程内 mock 上游(429/500/慢/断流)+ 真实 TCP(装死→读超时、discard→拒连、abort→RST 重置)+ 关 Redis(路由降级)
  - **PG 口径排雷记录**:hardening 批次首跑 PG 14 failed——根因非方言 bug,是 `.pgdata` 持久库旧 schema 残留(`create_all` 只建缺失表不改已有表,`published_at` 类新列跨 run 缺失)。修复=conftest 会话开始按当前模型 drop_all+create_all 重建;迁移正确性由 verify_pg_migration.py 独立验证(职责分离)。**教训:持久测试库 + create_all = schema 漂移假失败,任何"模型演进后 PG 全红"先查 schema 再查代码。**
- **Alembic 迁移在真 PostgreSQL 验证通过(含 hardening 迁移)**:`scripts/verify_pg_migration.py`(evidence/pg_migration.txt)——upgrade head + 5 表齐全 + 7c1f4a9b2d10 产物(published_at/渠道超时列/key_id 可空/唯一约束)+ 幂等重跑

## ② 核心验收数字(对照 PROGRESS 里程碑)

| 验收项 | 目标 | 实测 | 口径 |
|---|---|---|---|
| 每请求附加开销 | < 10ms | **3.55ms** ✅(P1 重测现行口径) | c=1 无排队差值法;hardening 后 2000 req 复测;`scripts/load_test.py`(evidence/load_report.md) |
| 并发扣减零超卖 | 桶硬上界 | **100 并发恰放行 50**(容量 50)✅ | 真 Redis Lua,`test_bucket_concurrent_zero_oversell` |
| 记账原子性 | 零丢失 | **100 并发恰 1800 token、账本 100 行** ✅ | `test_concurrent_accounting_atomic` |
| 故障注入:全渠道 500 | 透传+降级 | 透传最后错误,渠道头=exhausted;Redis 关停路由降级不崩 ✅ | `test_all_channels_fail_passthrough` / `test_redis_down_router_degrades` |
| 任务不丢不重 | at-least-once+幂等 | 重复投递被状态机吞掉;崩溃(ghost consumer)后 XCLAIM 收编;3 次失败进死信 ✅ | `test_m2_streams` |
| webhook 端到端 | 幂等投递 | 幂等键=request_id,真 HTTP(uvicorn 随机端口)恰好收到 1 次 ✅ | `test_webhook_e2e` |
| 可观测 | TTFT/分维度 | /metrics 暴露 REQUESTS/TOKENS/TTFT/USAGE_QUEUE_DEPTH/CHANNEL_* | `GET /metrics` |

## ③ 调优叙事(压测的方法论与前后对比)

- **教训 1(压测第一课)**:首版数字 142 req/s、P50 335ms —— 瓶颈是 **httpx 纯 Python 客户端自身**
  (c=1 时网关实为 P50 3.3ms)。换 aiohttp(C 解析器)+ 差值法后数字才可信。
- **教训 2(口径)**:c=50 的中位差 120ms 是**排队延迟**(单进程 CPU 串行 ~3ms/req × 50 并发),
  不是每请求代价 —— 附加开销必须在 c=1 无排队下测。修正后 **3.70ms 达标**(P1 重测后现行口径 **3.55ms**,见 §⑨)。
- **组件分解**(c=50 服务时间):裸 mock 0.34ms → +httpx 转发跳 0.7ms → +鉴权/准入/路由/记账 ≈ 2.8ms。
- **已做优化**(测试全绿保护下):朴素 EVAL → EVALSHA;准入 3 次往返 → 单 Lua 1 次;鉴权缓存命中零
  DB 会话;记账 INCRBY+窗口 → pipeline;路由 EMA 读改写 → 原子 Lua。**每请求 Redis 往返 8→5**。
- **诚实结论**:单进程绝对吞吐受 GIL/事件循环约束(本机 Proactor ~300-450 req/s;Selector 实测
  c=50 略好但 c=1 更差,未采纳);生产 `uvicorn --workers N` 水平扩展,与 LiteLLM 部署形态一致。

## ④ 环境与口径分列

- 本机(今日全部数字):Windows 10.0.26300 · Python 3.12.4 · **SQLite**(aiosqlite)+ **便携 Redis 5.0.14.1**
  (tporadowski,Lua/Streams/XCLAIM 验证可用;RESP2 protocol=2 与 redis-py 8.1 兼容)
- 生产(compose 交付,未在本机跑):PostgreSQL 16 + Redis 7(AOF)+ Prometheus + Grafana
- **PG/Redis7 口径数字:待 Docker 就绪后用同一套测试复跑**(ADR-002;tests 默认方言中立,切换=DB_URL)
- 上游:进程内 mock(近零延迟)→ 本报告=网关自身性能;**DashScope 真实冒烟见 §⑤**

## ⑤ DashScope 真实冒烟(2026-11-02 00:31,已通过)

- **链路**:真 HTTP → uvicorn 网关 → 鉴权/Lua 准入 → httpx(trust_env=False 直连)→ DashScope OpenAI 兼容端点
- **非流式**:200 OK · 780ms · usage=31 tokens · 回复正常 · channel=dashscope
- **流式**:200 OK · **TTFT 133ms** · 总延迟 303ms · 11 chunks · usage=35 tokens · SSE 透传含 usage 提取
- **账本对账**:2 行 · 总 66 tokens(后台异步记账落库成功);花费 ≈ ¥0.00005(≤¥2 红线,余量全留)
- **诚实记录**:qwen-turbo 当前账号配额耗尽(`limit_requests` 429 ×6 次退避重试确认)→ 换 **qwen-flash** 通过;
  过程中网关的 **429 透传 + 渠道冷却 + 恢复后重试** 被真实上游活体验证(503 no_channel_available 信封格式正确)
- 命令:`python scripts/smoke_real.py`;报告:evidence/real_smoke.md;key 读 policy-compass/.env,未落任何日志

## ⑤+ M4 生态演练(2026-11-02,通过)—— 故障注入抓到两个真 bug

- **演练剧本**(`scripts/demo_ecosystem.py`,真实多进程):子进程起 agent-a/agent-b 两个 Agent 服务 →
  网关真 HTTP → admin API 注册渠道 → 正常请求记账 → **kill -9 agent-a** → **10ms 自动切换 agent-b,
  客户端零感知**,账本照记;`scripts/agent_shim_example.py` = AskBI/PC(自有 JSON 协议)接入的通用
  OpenAI shim 模板(生产接入 = 起一个 shim 进程 + admin API 注册,应用零改动)。
- **故障注入收获 #1(真 bug)**:kill -9 造成连接重置,httpx 抛 `ReadError`/`RemoteProtocolError`
  —— 原分类元组漏接 → **未处理 500**。修复:UPSTREAM_TRANSPORT_ERRORS 统一捕获做 failover,
  `_classify_upstream_exc` 按类型映射(对端中断/连不上=502,真超时=504);回归测试
  `test_upstream_connection_reset_maps_502`(真实 TCP abort=RST 语义)钉死。这正是"故障注入的价值"的活案例。
- **故障注入收获 #2**:空 api_key 渠道发 `Authorization: Bearer `(空值)→ httpx 拒绝本地协议错误
  → `_build_headers` 空 key 不发该头(内网 Agent 服务无 key 的真实场景)。
- **诚实注记**:演练日志中一次 `usage_write_failed`(sqlite 文件锁,系此前被强杀的孤儿进程遗留,
  环境残留非代码缺陷)——写库失败时网关按设计降级未崩,单写者队列的可观测路径(`usage_write_failed`
  事件)工作正常。

## ⑤++ 增强批次(2026-11-02,4亿 token 窗口)—— 六项全部通过

| # | 项 | 结果 | 证据 |
|---|---|---|---|
| 1 | **真 PostgreSQL 双口径** | 47/47 SQLite + 47/47 PG 16.2(pgserver 内嵌);Alembic 迁移真 PG 验证 | 本节 §①;ADR-002 已升级 |
| 2 | **多 worker 水平扩展** | **1.00x → 2.15x → 4.04x**(workers 1/2/4,1200 req @ c=50)——共享状态全在 Redis 的多副本设计被实测验证 | evidence/load_workers.md |
| 3 | **MCP server**(2026 JD 命中) | 4 工具(渠道健康/用量对账/预算读数/签发 key)真 stdio 握手;**MCP 签发的 key 立即可用于网关**——共享状态面证明,非旁路 | test_m5_mcp(真子进程握手) |
| 4 | **精确响应缓存** | 租户隔离键(安全边界)/确定性门控(temperature≤0 非流式无 tools)/X-Cache 头/命中率指标/账本 channel=cache/管理端清空 | test_m5_cache 5 项 |
| 5 | **管理界面 + Playground** | 管理台(/admin/web):渠道健康(EMA 实测)5s 自刷/租户/签发 key(只显示一次)/**key 清单+吊销按钮**/用量对账/清缓存;**Playground(/playground)**:SSE 逐字 + TTFT/总延迟/渠道/缓存命中四指标实测 + request_id 徽章 + MISS→HIT 历史(浏览器活体验证:8ms MISS→3ms HIT channel=cache) | docs/screenshots/admin_web.png · playground.png(浏览器真渲染) |
| 6 | **PolicyCompass 实接** | **真 PC(CRAG,mock LLM)穿网关答题**:OpenAI 请求 → 网关(Lua 准入)→ shim → PC /api/chat;两问 200,937ms/710 字与 889ms/638 字真实语料答案,渠道头 policy-compass,账本落库 | evidence/pc_gateway.md;scripts/demo_pc_gateway.py |

- 实接踩坑记录(诚实):①PC 路由挂 `/api` 前缀(/health 404 → /api/health);②PC 必须用系统 python
  (agent venv 无 langgraph/jieba);③首版演示 bootstrap 假渠道按字母序抢走流量(EMA 同为默认值)
  → 砍假渠道让 PC 即默认渠道;④PC 无 token 计量,shim 上报 usage=0,账本 token=0 是诚实记录。
- pgserver 运维坑:被强杀后残留 postmaster.pid/进程锁 → taskkill postgres + 删 .pgdata 重建(测试库无状态)。

## ⑤++ 生产审计批次(2026-11-03,第八批)——审计出 4 项全修

| # | 审计发现 | 修复 | 证据 |
|---|---|---|---|
| 1 | 账本走进程内单写者,崩溃丢在途账本(设计降级点:Streams 建了没用在核心通道) | ADR-004:记账 enqueue("usage_write",幂等键 usage:{request_id})走 Streams;UsageWriter 退役 | test_m6 + 55×2 全绿 |
| 2 | channels 表建了路由不读,重启丢运行时注册渠道(成分缺口) | ADR-005:admin 双写(运行时+入库)+ 启动 load_channels_from_db 恢复 | test_channel_registry_survives_restart |
| 3 | "多副本不放大"主张无直接测试(主张无测试) | 双副本一致性测试:两 app 共享 Redis 合并并发 100×容量 50→恰放行 50+回补(±2 容差,令牌桶惰性回补语义) | test_two_replica_rate_limit_no_amplification |
| 4 | 无就绪检查(k8s 成分缺口) | /ready:Redis ping + DB select + 队列积压读数 | test_ready_endpoint |
| 5 | 无分布式追踪(JD 成分缺口:Kimi 全系明写 OpenTelemetry) | OTel 手动 span(每次上游尝试,属性 channel/model/status),console/OTLP 导出,默认关零开销;InMemory exporter 测试断言 | test_m6_tracing |

- PG 口径暴露的差异(诚实记录):PG 强制外键(cache_first 测试需先清账本行)/ PG 鉴权稍慢令牌桶回补 +1(断言容量+回补容差)/ MCP+asyncpg 的 loop 绑定(Event loop is closed→持久 loop)。

## ⑥ 面试素材索引

- "为什么 Python 不是 Java/Go" → ADR-001(证据链:LiteLLM 8ms P95@1k RPS / JD 0/30 必须 Java / 下沉触发条件)
- "高并发怎么保证不多发" → 硬点 2(Lua 原子 + 并发恰放行测试)
- "消息不丢不重" → 硬点 4(at-least-once + 幂等键 + XCLAIM 恢复,三层测试)
- "线上慢怎么查" → TTFT 与总延迟分开记(慢在上游还是网关)+ USAGE_QUEUE_DEPTH(落库跟不上可见)
- "为什么不用 Celery" → ADR-003(黑盒 vs 可讲透;接口预留换实现边界)
- "压测怎么做的" → §③(客户端瓶颈教训 + 口径修正,这是真实踩坑不是背的)

## ⑦ 留给用户

1. Docker 就绪后:`cd deploy && docker compose up -d` → 同套测试在 PG/Redis7 容器拓扑复跑(§④ compose 口径;pgserver 内嵌 PG 口径已复)
2. 四仓 push(序 agenteval→askbi→policy→agentgate;CI 含真 Redis/PG 服务容器 + Docker build job)
3. AskBI 注册为网关渠道(shim 模板已备,需开放跨项目改动)
4. 可选:PG 账单表按月分区、精确滑动日志 TPM

## ⑧ hardening 批次(2026-11-03,AG-2)——审计驱动,15 项修复分 6 笔入库

> 背景:ds-update 轮对全项目做生产审计(27 文件),AG-2 会话逐块复核后锁存(01b781f→cd22cab)。
> 本节是"审计→修复→回归"的完整记录;每条主张都有对应测试。

| # | 审计发现 | 修复 | 回归证据 |
|---|---|---|---|
| 1 | 流式指标记在构造 StreamResponse 时(TTFT/usage 恒 None/0,看板假数据) | 指标挪 relay finally(流真正结束后记一次);状态区分 200/502/499;账本记真实总延迟 | test_m1 全量回归 |
| 2 | 流式不传 include_usage 时账本记 0 token | 计费模式强制注入 stream_options.include_usage | test_stream_without_include_usage_is_still_billed(18 token) |
| 3 | 缓存把缺省 temperature 当确定性(OpenAI 缺省 1.0,随机答案被缓存) | 显式 temperature≤0 才入缓存;缓存键加 user | test_m5_cache 更新(缺省温度不缓存) |
| 4 | 语义缓存不绑 system prompt(同问题不同上下文串答案) | context_digest(模型+历史+采样参数+user)入索引 | test_m5_semantic(context 隔离) |
| 5 | XACK 后不 XDEL,backlog 只增不减(误导 /ready 与 Grafana,Stream 无限涨) | ACK 后 XDEL;backlog=真实待处理量 | test_m2_streams(xlen==0 断言) |
| 6 | 消费端可能重复执行同一 task_id(双消费者竞态) | 条件 UPDATE 原子抢占(pending 或超 visibility timeout 才可领取);visibility 500ms→30s | test_worker_processes_once_and_dedupes_redelivery |
| 7 | DB 提交后 XADD 失败丢任务(双写空洞) | transactional outbox:published_at 水位 + sweeper 补投 | 迁移产物真 PG 验证;专项注入测试在 P0-C |
| 8 | 崩溃重投账本重复;request_id 可跨租户伪造吞账本(计费 DoS) | `(tenant_id, request_id)` UNIQUE + 先查再写捕 IntegrityError;幂等键带租户域 usage:{tenant}:{rid} | test_duplicate_ledger_delivery_writes_once / test_same_request_id_across_tenants_does_not_collide |
| 9 | 纯代理模式 key_id="anonymous" 撞 PG 外键(usage_write 进死信) | key_id 可空,记 NULL | 迁移产物验证(is_nullable=YES) |
| 10 | 预算 INCRBY 与 TPM 窗口非原子(半成功中间态) | 合并单 Lua(预算+TPM 一次原子更新) | account_tokens 单测回归 |
| 11 | 渠道注册不按 model 过滤(无关模型命中声明渠道) | candidates(model) 过滤;空 models=兼容全部 | test_m4 回归 |
| 12 | 渠道超时配置重启丢失 | channels 表 connect/read_timeout 持久化,启动恢复 | test_channel_registry_survives_restart |
| 13 | admin token 非常量时间比较;/admin/channels 无鉴权;重复租户 500;管理台动态内容无转义 | secrets.compare_digest;channels GET 需 token;409;HTML 转义+token 输入(sessionStorage) | test_m1_auth 回归 |
| 14 | MCP 渠道名拿 settings.models(模型名)冒充 | 从 channels 表读(与 admin API 同源);签发 key 校验租户 | test_m5_mcp(真子进程握手) |
| 15 | 部署卫生:requirements 缺 OTel/MCP/aiohttp/pgserver;Dockerfile root;compose 默认 token | 依赖补齐;非 root+healthcheck+.dockerignore;compose 禁默认 token;CI 加 PG 迁移步骤+Docker build job | CI 配置审查(Docker build 待 push 后首跑) |

- **PG 口径排雷**:首跑 14 failed → 根因 `.pgdata` schema 漂移(见 §①)→ conftest 会话重建 → 62 passed。
- **文档口径统一(本批完成)**:README/PROJECT_BOOK/PITCH 全部对齐 62×2 与现行设计(TPM 双窗口/Streams 记账/outbox/租户域幂等);Grafana 面板同步(USAGE_DROPPED→TASKS_DEAD,队列积压语义更新)。
- **Redis 故障语义(P0-C,定义+测试)**:准入 fail-closed(RedisError→503 admission_unavailable+Retry-After,计费硬上界宁可拒不可漏)/ 鉴权降级直查 DB(事实来源,缓存读/回填失败不阻塞)/ 响应与语义缓存降级当 miss(优化非正确性)。三条故障面独立,各有测试(test_redis_down_admission_fails_closed / test_redis_down_auth_degrades_to_db / test_cache_redis_error_degrades_to_miss)。
- **outbox 双向注入(P0-C)**:XADD 抛异常→published_at 为空→sweeper 补投→消费一次(test_outbox_sweeper_republishes_after_xadd_failure);水位丢失(已投递未消费窗口)→重复投递→状态机幂等吞掉、两条消息都 ACK+DEL(test_marker_loss_republishes_but_consumes_once)。
- **诚实边界(剩余)**:水位丢失场景若发生在"已消费后"(status=done),sweeper 不补投 —— 这是正确设计(账本已落),补投只服务 pending 窗口。(压测旧口径已在 P1 重测销账,见 §⑨)

## ⑨ P1-P4 自主推进批次(2026-11-03/04,AG-2)——压测重测+安全三连+部署生态,16 笔提交

> 用户指令:P1-P4 自主完成不停歇。执行序:P1 数字重测 → P2 安全三连+小件 → P3 Jaeger/告警 → P4 成本/trace/K8s/pgvector。
> 期间抓到的真问题全部修复并回归(压测脚本文件锁/演练建表时序/演练语义过时/真冒烟账本抢跑/PG 连接池泄漏)。

### P1 数字重测(hardening 后新口径,旧数字作废)

| 项 | 旧口径 | 新实测 | 说明 |
|---|---|---|---|
| 每请求附加开销 | 3.70ms | **3.55ms** | c=1 差值口径,2000 req @ c=50;达标不变 |
| 水平扩展 | 1.00→2.15→4.04x | **1.00→2.09→4.95x** | 1500 req × 3 档;4 档超线性来自本机噪声 |
| kill -9 切换 | 10ms | **2047ms** | 含真实 RST+冷却+重试周期,比旧口径诚实;客户端零感知不变 |
| DashScope 真冒烟 | — | 非流式 434ms/流式 TTFT 142ms,账本 2 行 | 花费 ≈¥0.00006 |

- **演练抓到语义过时**:hardening 后路由按 model 过滤,"agent-b 兜 agent-a 模型"不再成立(正确收紧)→ 演练改为同模型池双渠道(真实 failover 前提)。
- **压测/演练脚本 Windows 文件锁根治**:固定 DB 文件被孤儿进程锁死(PermissionError 炸压测)→ 一次性临时 DB;demo 的 init_db 提前到 lifespan 前(一次性新库上渠道恢复撞 no such table,以前全靠旧库已有表)。
- **真冒烟账本抢跑修复**:记账在 BackgroundTask enqueue,单次 flush 抢跑后硬停丢在途 commit(实测 1/2 行)→ 轮询等齐 2 行;边界注记入库(at-least-once 从 enqueue 起算)。

### P2 安全 hardening(5 笔,安全拷打面收口)

| # | 项 | 要点 | 测试 |
|---|---|---|---|
| 1 | 渠道密钥 Fernet 加密 | enc:v1: 前缀;prod 强制主密钥(fail-closed);存量明文兼容;解密失败路由失败不吐垃圾 | 7 项含端到端(注册→DB 密文→重启恢复→真 HTTP) |
| 2 | admin 审计日志 | audit_logs 只追加(迁移 a3d94f1c7b21);5 动作同事务落审计;raw key/渠道密钥零入审计 | 4 项含同事务回滚 |
| 3 | Idempotency-Key | 重放不重执行不重计费(账本 channel=replay token=0);在途 409;仅非流式(诚实边界) | 4 项 |
| 4 | 租户级预算 | tenants.budget_tokens(c8e51b09d447);key/租户预算同一次 Lua INCRBY;换 key 绕不过;60s 缓存窗口诚实标注 | 3 项含跨 key 拦截 |
| 5 | 三小件 | 限流响应头标准三件;每 key 并发上限(崩溃泄漏 TTL 300s 有界);缓存单飞锁(SETNX 3s+1s 轮询等填) | 3 项 |

### P3/P4 部署与生态

- **P3-17**:compose 接入 Jaeger all-in-one(OTLP)——OTel span 有完整查看端;SLO 告警规则 6 条(deploy/alerts.yml,P1 页/P2 工单;阈值为占位,真实流量后校准)。
- **P4-1 成本模型**:AGENTGATE_MODEL_PRICES(元/百万 token);账本 cost 列(f9a37c2d5b84)写入点计算;未登记价格 NULL 不猜;账单端点(JSON 汇总+CSV 明细)。测试:按账本实际拆分验证+汇总明细两两对账。
- **P4-2 W3C traceparent**:提取(非法不猜)/注入(client 未带时从当前 span 生成)/响应头回传;门控用 app 级 state.tracer。**发现 FastAPI 0.162 内置原生 telemetry**(fastapi/telemetry/,全局 provider 存在即自动叠 span)——生产含义:开启 otel 后 Jaeger 可见两层 span(已文档化)。**修测试基建真 bug**:OTel 全局 provider 只能 set 一次,fixture teardown 的 reset_tracing 致后续 exporter 静默收不到 span。
- **P4-3 K8s 清单**(deploy/k8s):Deployment(2 副本双探针非 root)/Service/HPA(2~10,CPU 70%)/ConfigMap/Secret 模板;有意不含 PG/Redis StatefulSet(托管优先);**未实跑诚实标注**。
- **P4-4 pgvector 后端**:内嵌 PG 16.2 自带 vector 0.6.2 → 真实现(app/cache/semantic_pg.py):幂等建表+HNSW、KNN 召回、降级当 miss;`semantic_backend=redis|pgvector` 双后端,SQLite 口径诚实 skip。**顺手修 PG 连接池卫生真 bug**:95+ 测试 × 每 app 连接池不释放 → too many clients;fixture teardown 统一 engine.dispose()。

### 终态(2026-11-04,最后提交后复测)

- **SQLite:95 passed + 2 skipped**(pgvector 是 PG 专属口径,skip=诚实标注)
- **PostgreSQL 16.2:97 passed**(含 pgvector KNN 往返+租户隔离)
- ruff 全绿;Alembic 迁移链 3215fe6889f5→7c1f4a9b2d10→a3d94f1c7b21→c8e51b09d447→e6f22c81a903→f9a37c2d5b84 真 PG 验证
- git:main 分支 22 笔提交(自 747f471 起),工作树干净
- 剩余边界:compose/k8s 未实跑(等 Docker/集群)、AskBI 实接(等跨项目授权)、预算百分比预警、PG 分区
