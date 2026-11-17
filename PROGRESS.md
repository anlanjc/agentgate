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

- [1B4-AG 2026-11-18] W5 车道三任务串行(真实化与性能),基线 ae1ba62 未 push,三笔提交,**SQLite 149+3skip(真 Redis 6379 口径)/ PG 152(61555 外部实例)全绿 + ruff 全绿**:
  - **任务一 pgvector 0.8 升级路径实验(9c3075b,清算 11-09 挂账)**:装置=pginstall 副本+源码编译 pgvector 0.8.7(mingw gcc 8.1,19 obj 链 libpostgres.a,`.tooling/agentgate-pg08/` 独立 pgdata :61556)对照 venv pginstall 0.6.2 常驻 :61555,同 PG 16.2 真 A/B;**归因修正:11-11 的"Seq Scan/HNSW 形同虚设"实为 TRUNCATE→灌数→立刻查的陈旧统计 regime(N 线性延迟 4.84/33.2/158.9ms 本轮复现),非 0.6.2 版本必然**——其 EXPLAIN 用列对列 ORDER BY 结构上出不了索引计划,方法失真;真缺陷坐实=generic plan(生产预编译语句 regime)下 0.6.2 租户过滤选 HNSW 后过滤,LIMIT 200 只填 14.2/34.6 行(截断 83-93%),观测 1 例 top-1 丢失,ef_search=400 应急可填满(2.05ms);**0.8.7 零查询改动**优化器自动改走 btree 精确路径(行数恰满),内核/写入 1.2-3.4x 提速,iterative_scan 在 2% 选择性下未被选中(btree 更优,收益未复现如实记录);S3 过滤外提=0.6.2 唯一让 HNSW 走索引的查询改造但召回保证语料性;生产本体 18 tests 全绿(0.8.7 实例口径);**决策建议=维持 redis 默认,启用 pgvector 则升级 ≥0.8 优于查询改造,产线不热改留主人批**,evidence/pgvector08_experiment_20261118.md;
  - **任务二 性能档案 round 2(13cc5d9)**:同脚本同参数重跑——c=1 附加开销 **2.37ms**(旧 3.55)/c=50 369 req/s(旧 306)/多 worker **1.00→2.16→4.55x**(旧 4.04x,P1 重测 4.95x,同量级);**热路径往返数实证**(commandstats 两窗差分扣 background)=非流式前台恰 4 往返(auth GET+admit 单 Lua+candidates pipeline+mark_success EMA Lua,记账/消费全在 background);GATE-2/5/9 修复零热路径往返增量,实测无回归;c=1 改善归因机器态漂移(裸基线 1851→2282=+23% 对照锚),不宣称代码性改善;真瓶颈搜索三候选(语义缓存/准入 Lua/audit 写)均无新瓶颈,不触发优化分支,evidence/load_report_r2_20261118.md + load_workers_r2_20261118.md + EVIDENCE §③ round 2 节;
  - **任务三 混沌 round 3(df44f18,5/5 PASS + 1 真缺陷当日修)**:独立便携 Redis 6396(与压测 6379 隔离)+ 61555 PG;**真缺陷=SIGKILL worker 的 gauge 残留**:gauge_all MMAP 文件被每次抓取永久并入,USAGE_QUEUE_DEPTH/CHANNEL_COOLING 冻结在死前值(sum 类告警永不恢复),上游 prometheus_client 0.26.0 mark_process_dead 只清 live 系(实测证伪直觉),GATE-6 优雅停运/Dockerfile rm -rf 都覆盖不到 k8s master 进程内重启 worker——**修复=cleanup_dead_process_files() 启动收尸**(死 pid+60s mtime 守卫,counter/histogram 不碰,跨平台零依赖)+ 回归×2;S2 幂等 409×10 并发两轮槽恰归零(GATE-2 修复语义并发实测,对照 GO-S3 手法,TTL 自愈 429→200;装置教训=BackgroundTask 收尾读时序假象非泄漏)/S3 cleanup_expired 6×500 有界推进至清零存活 200 全保/S4 DLQ 250 死信风暴 XLEN 恰 200 保最近、账本 dead=250/S5 Redis TIME Lua 抖动降级不炸+candidates 降级放行全渠道(文档化后果)+NOSCRIPT 重载恢复,evidence/chaos_r3_20261118.md;
  - 收尾清点:实验实例全停(61555/6379/6396/61556 零监听,TIME_WAIT 不计),后续 PG 口径需重启 61555 或改用 pgserver 自动 initdb;终态测试=SQLite **149+3skip**(+2 混沌回归,真 Redis)/PG **152**(+2 同),ruff 全绿;遗留=无(范围注记:60s 守卫内刚崩溃 worker 的 gauge 文件由下一次 worker 启动收,单 worker 冷备场景留容器重启,已入册)。
- [1B3-ECO 2026-11-17] GATE-9/10 P2 证伪复核收口 + 生态闭环演示(askbi→agentgate→mock,纯 config 零跨仓代码),基线 8385741 未 push,两笔提交:
  - **GATE-9(P2)916b0ff**:复核属实——failover.py 候选过滤 now 与 mark_rate_limited/mark_failure 冷却 deadline 全用本地墙钟,deploy/k8s/deployment.yaml replicas=2 下副本时钟偏差平移冷却窗(快钟副本往他副本刚冷却的渠道打流量),streams.py GW-4 同缺陷类修复未覆盖路由层——冷却写入与比较统一取 Redis TIME(对齐 GW-4 口径),选型留档:candidates 的 TIME 挂进既有状态 pipeline(零额外往返)、mark_* 用 TIME 内嵌 Lua 单往返原子完成(mark_failure 的 fail_streak 读改写顺带收进脚本,消跨副本丢计数竞态,往返 2→1);否决启动对时+偏移补偿(NTP 步进漂)与 TIME 节流采样(节流窗内两源仍分叉);回归先红后绿×3(快钟 +120s 注入:漏判冷却窗/deadline 偏离 Redis 时钟,注入点=旧实现时钟源 failover.time,修复后不再消费本地墙钟即判别点),**SQLite 147+3skip / PG 150 全绿(147=基线 144+3)**;
  - **GATE-10(P2)a182a44(入 agentgate-go,纯文档)**:五项逐条核对两侧现行实现(含 GO 车道 GO-4/8/10 修复批)——①429 缺省 Retry-After 两侧已对齐 60s、③冷却耗尽 503 bad_gateway 已对齐(GO-10)、④校验顺序已诚实声明 ⚠️、⑤租户预算闸 Go 侧已由 GO-4 补齐(admit KEYS[4]+main.go 402 同码),Python 侧五项行为核对本就如实、零代码改动;修正仍在失真的负载均衡行(原"✅ 逐条对齐"把 Go 的 <20ms rand.Intn(2) 打散误写给 Python——Python failover.py 纯 sorted 从未实现,未试渠道画像两版方向相反:Python DEFAULT_EMA_MS=1000 排后/Go emaLatency=0 排前)→ 改如实声明+§五⑪,另补 §五⑫(尝试后耗尽:Python 透传最后上游响应 vs Go 合成 502 gateway_bad_gateway+重试 3 vs 渠道数)与 §五⑬(租户预算闸 GO-4 补档),两向统一都动热路径选路语义不硬改;
  - **生态闭环演示 7fac451**:scripts/ecosystem_loop_demo.sh(+steps.py/mock_upstream.py)——askbi(:8200,PROFILE=full+LLM_PROVIDER=openai_compatible,env 名实读 .env.example/factory.py,askbi 仓零改动)→ agentgate(:8100,SQLite+便携 Redis:6399+记账)→ 节点感知 mock 上游(:8101,按 system 提示词标记词返回各节点契约最小合法输出,JSON+SSE 双形态);场景 ALL OK:S1 askbi 两问全链路(每问 4 次 LLM 调用逐节点记账,final answer 含真实 duckdb 数据行,usage 聚合=账行之和 8 行 2391 token)、S2 直连网关 SSE(chunk+usage 帧+[DONE],账行 TTFT 20.57ms)、S3 白名单 403、S4 租户预算 402(DEL 鉴权缓存即时生效,生产等 TTL≤60s)+askbi LLMError 诚实降级 refusal、S5 采证(usage 9/2516+metrics+账本 9 行全 200);调试留痕:mock 初版流式收尾帧 kwarg 错→网关按 GW-9 语义正确记 502+error 帧(最小复现定位 mock 自身,网关零改动)——意外成为流中错误语义现场实证;Git Bash 中文参数改码坑→场景层 httpx;产物 evidence/ecosystem_loop_20261116.md(raw 全量留痕 ecosystem_loop_raw_20261116.txt);口径:mock only 零现金/同机单进程非生产部署/回答质量无意义只验链路闸门计费;演示进程清点全关(6379/6399/8100/8101/8200 零监听)。
- [1B3-GATE 2026-11-16] 评审 4.0 修复轮(报告 interview/reviews/review_report_4.md)名下 GATE-1~7 confirmed 全收口,一问题一提交先红后绿,**SQLite 144+3skip / PG 147 全绿(2026-11-16 实跑;基线 133+2skip/135,净增 11 项其中 1 项 PG 门控)**,基线 9192879 未 push:
  - **GATE-1(P1)af48181**:k8s configmap 七键无 ENVIRONMENT → Settings 缺省 'dev' 落 NON_PROD 白名单直接 return,部署面三道 fail-closed 安全闸静默失效(compose 已 prod,k8s fail-open)——configmap 显式 `AGENTGATE_ENVIRONMENT: "prod"` + secret 占位值全改 MUST-REPLACE 起头(grep 可审计)+ deploy/k8s/README 同步;静态回归钉 configmap 必含非白名单 ENVIRONMENT;
  - **GATE-2(P1)fbd10bb**:幂等 409 冲突路径泄漏并发槽(409 raise 直达 errors.py 全局 handler 无释放点,ACQUIRE_LUA 每次 acquire 刷 EXPIRE 300s → 持续重试侵蚀额度直至整 key 429 锁死)——`_slot_guard` 请求级一次性收尾闸(released 单向旗防双重 DECR)+ 准入后区域统一 try/except BaseException 逃逸兜底,_enqueue_error 4 处调用点全量走闸;对照 agentgate-go main.go chatCompletions 统一收尾 defer+slotHeld(Go 结构性避开,回归 m2_test.go);回归:预置在途槽+幂等锁 → 409 → conc 归位 + 第三请求可占槽(修复前红);
  - **GATE-4(P1)1d17ef7**:失败任务重试/死信推进只依赖空轮(_read_new 返回 0 才 reclaim),持续负载(≥5 任务/s)下永远饿死,失败消息滞留 PEL 无上界——reclaim 挂进 _periodic_maintenance(2s 周期),空轮即时 reclaim 保留;回归:预置 300 条积压使 drain 期每轮 handled>0(结构性排除空轮),断言死信在新消息排空前推进(真 Redis 口径,修复前红 dead=0);
  - **GATE-5(P1)159b9bb**:①pgvector 语义缓存只 INSERT 无淘汰(过期行连 HNSW 索引无界增长拖垮 PG)——cleanup_expired 按 created_at<now-ttl 有界批量 DELETE(500/次删最旧),store_pg 消费点节流触发(60s 周期,失败降级);②DLQ 只 XADD 从不裁剪——XTRIM 精确硬上界 DLQ_MAX_ENTRIES=200(对齐 Redis 语义缓存 200 条纪律,死信审计仍以 task_records.status='dead' 全量为准);回归双份(PG 口径回填过期行清理断言 / patch 上限 5 后 8 死信流内恰保最近 5 条);
  - **GATE-6(P1)6f182c7**:Dockerfile --workers 2 共享监听 socket × /metrics 进程内默认 registry(全仓无 multiproc 配置)→ 指标轮流取半份+虚假 counter reset,告警系统性漏报——metrics.render_metrics() 按形态分派(PROMETHEUS_MULTIPROC_DIR 已设 → MultiProcessCollector 合并 MMAP;未设回退默认 registry 行为不变),Dockerfile ENV + CMD 启动前 rm -rf 清残留,lifespan 停运 mark_process_dead,README 水平扩展段同步;回归:双子进程递增 → 合并抓取 requests_total=总和(修复前轮流取半份)+ TTFT 桶跨进程合并 + env 未设回退 + lifespan 驱动断言 + Dockerfile 静态闸;
  - **GATE-7(P1)9e749e8**:requirements.lock 自述 freeze 全量却缺 otlp exporter 及 grpcio/protobuf(生成环境本身缺依赖)→ 按 lock 重建环境带生产 OTEL 配置启动即 ModuleNotFoundError 崩溃循环——本地 .venv pip install 成功(清华镜像:exporter 1.45.2/grpcio 1.85.1/protobuf 7.36.2/opentelemetry-proto/googleapis-common-protos/otlp-common)后 freeze 重生成 lock(恢复 pywin32 sys_platform 标记,freeze 不再输出否则 Linux CI 装锁会炸);双闸:静态 lock⊇requirements.txt 顶层声明(防再漂移,对旧 lock 红)+ otlp setup_tracing 装配冒烟(OTLPSpanExporter 惰性 channel 离线可跑);
  - GATE-8 归 Go 侧不动;GATE-3 已驳回不涉及。诚实边界:①回归红绿验证在真 Redis(便携 5.0.14.1)+ pgserver 16.2(vector 0.6.2,AGENTGATE_TEST_PG_URL 外部 61555 实例)口径;②fakeredis 口径下既有 test_outbox_sweeper_republishes_after_xadd_failure 存在时序 flaky(基线代码同样复现,非本轮引入,真 Redis 下稳定)——与轮内改动无关,如实留档;③GATE-7 import 冒烟本机因 venv 已装包恒绿,红态在 lock 复现环境(CI)显形。

- [AG-1 2026-11-01] 立项:环境侦察(无 Docker/PG/Redis,网络三条路实测)+ 本文件 + ADR-001/002/003 + ARCHITECTURE.md。便携 Redis 下载中、依赖经阿里镜像安装中。
- [AG-2 2026-11-03] hardening 批次审阅与锁存(ds-update 轮 27 文件由 AG-2 复核后分 5 笔入库):
  ①test: PG 口径 schema 漂移防护(.pgdata 持久库旧 schema 残留致 14 假失败,会话开始 drop_all+create_all 重建;迁移验证职责分离)→ **AGENTGATE_TEST_PG=1 修复前 14 failed → 修复后 62 passed**;
  ②feat: 请求路径 hardening(流式指标挪 relay finally·TTFT 起点=进网关·499/502 语义·强制 include_usage·账本 (tenant_id,request_id) UNIQUE+租户域幂等键·显式 temperature 门控·context_digest·预算+TPM 单 Lua·prod fail-closed·迁移 7c1f4a9b2d10);
  ③feat: 队列 hardening(outbox 水位补投·条件 UPDATE 原子抢占·visibility 500ms→30s·XACK 后 XDEL 真实 backlog);
  ④feat: 管理面安全(admin 常量时间 token·渠道超时持久化·路由 model 过滤·MCP 渠道同源·管理台 XSS 转义·Dockerfile 非 root+healthcheck·compose 禁默认 token·CI 加 PG 迁移+Docker build);
  ⑤docs: 证据与审阅记录入册(ds-update.md/ROADMAP.md/pg_migration.txt 含 7c1f4a9b2d10 产物验证+幂等重跑)。
  终态:**62 passed(SQLite)+ 62 passed(PG 16.2)+ ruff 全绿 + 迁移真 PG 验证**;遗留见 ROADMAP.md(P0-B 文档口径统一/Grafana 面板、P0-C outbox 注入测试/Redis 挂 fail-closed、P1 压测重跑)。
- [S9-OPT1 2026-11-11] PG 故障域混沌 + 语义缓存 pgvector 基准(两任务两 commit:7094c6e / 0a9c95f):
  ①chaos:PG 故障域 12/12(chaos_pg 装置 .tooling 不入库,chaos_matrix 先例)——真缺陷:鉴权 DB 断连裸 500(缓存 miss 时 asyncpg 异常穿透 _authenticate,真 uvicorn 实锤)→ 修复 503 auth_unavailable 统一信封(三分语义第三腿:200 缓存/401 判定无效/503 无法判定;KeyStoreUnavailable 显式放行 KeyNotFound/KeyInactive)+ 回归×2;发现-边界:故障窗内缓存命中请求 200 但账本行缺失(outbox 前置 DB 提交的推论,token 计量不丢,补强留设计轮);P3 不丢账语义成立:stream 事件 PEL 留存(不 ACK 不死信)→ 恢复后 reclaim 重试恰落 1 行;装置教训×5 入册(Windows 上 postmaster 进程级重启不可信→降档 DB 层注入 pg_hba reject+terminate/ pg_ctl capture_output 挂死/恢复 fsync 共享冲突/探针 URL timeout TypeError 自欺/admin 通道先建),evidence/chaos_pg_20261111.md;
  ②bench:语义缓存 redis 暴力余弦 vs pgvector KNN(N=100/1000/5000,生产代码本体+mock embedder,命中答案校验答非所问全 0):N≤200 暴力扫描够用(延迟打平 4.8ms,写入便宜 6 倍);N>200 redis 命中率崩塌 0.10/0.067(索引容量 200 结构性截断)而 pgvector 1.00——但出厂查询形态(过滤+LIMIT 200)在 pgvector 0.6.2 上 Seq Scan,N=5000 p50 158.9ms 比暴力扫描慢 17 倍,HNSW 形同虚设(无过滤纯 KNN 走索引 1.5ms;ef_search 200/400 不改计划)→ 维持默认 redis 后端,启用 pgvector 前需查询形态改造或升级 ≥0.8,evidence/semantic_pg_bench_20261111.md;
  终态:**SQLite 133+2skip / PG 16.2 135 passed + ruff 全绿**(两口径全量复测;实 redis 6379 口径——6379 无监听时 fakeredis 兜底口径会出 2 failed+5 skip 的假象,已复核为环境而非代码)。
