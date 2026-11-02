# agentgate 项目书(面试深挖手册)

> 配套:EVIDENCE.md(数字与口径)· docs/adr/(选型)· docs/ARCHITECTURE.md(师承)
> 用法:面试前通读一遍;每个问题给出"一句话答 → 深挖点 → 证据出处"三段式。

## 0. 30 秒开场(背下来)

"我做了一个多租户 LLM/Agent 网关,对标 LiteLLM 的核心链路自研:OpenAI 兼容代理、
virtual key 鉴权、Redis+Lua 分布式限流、配额计费、SSE 流式代理、故障转移和 Streams
异步任务。它是 IO 密集型系统——瓶颈在上游模型的秒级延迟,所以我用 Python/FastAPI,
压测的每请求附加开销是 3.7 毫秒;多进程水平扩展 1→4 worker 是 4.04 倍近线性,因为共享
状态全部在 Redis。67 项测试双数据库口径全绿,真实 PolicyCompass 已经接在它后面跑。"

## 1. 高频问题 → 深挖点 → 证据

### Q1 为什么 Python,不用 Java/Go?
- 一句话:场景匹配(LLM 网关 IO 密集,瓶颈在上游秒级延迟,框架毫秒级)+ 生态匹配(Agent 上游全是 Python)+ 岗位匹配(AI 应用岗默认栈)。
- 深挖:承认单实例千级 RPS 边界;LiteLLM 同架构 1k RPS 下 P95=8ms 且 Stripe/Netflix 生产采用;下沉触发条件三件事(超热点路径→Rust/Reddit 评论迁 Go、极低延迟尾部→Discord 弃 Go 用 Rust、分发形态→One API 选 Go 单二进制)。
- 证据:ADR-001;30 条 JD 0/30 要求 AI 应用后端必须 Java,"高并发"是语言无关措辞。

### Q2 限流怎么保证不多发?
- 一句话:令牌桶检查+扣减在一段 Lua 里,EVALSHA 原子执行;Redis 单线程保证零竞态。
- 深挖:①为什么必须 Redis——单机内存限流 N 副本放大 N 倍(LiteLLM redis_requirements 记载);②为什么令牌桶不是固定窗口——平滑、支持突发;③测试"并发 100 恰放行 50"是硬上界证据;④TPM 是另一套:双窗口加权滑动近似(O(1) 两键轮转,消除固定窗口交界 ~2× 突刺;仍有上一窗瞬时贡献的误差上界,精确滑动日志在 backlog)。
- 证据:test_bucket_concurrent_zero_oversell;test_m1_ratelimit。

### Q3 消息/任务不丢不重怎么实现?
- 一句话:Redis Streams at-least-once 投递 + 消费端幂等(账本 UNIQUE + 状态机)= 效果 exactly-once。
- 深挖:consumer group / XACK / XCLAIM 崩溃恢复(幽灵消费者收编)/ 死信流三次重试;幂等键在 PG task_records UNIQUE 兜底,重复投递直接 ACK 丢弃;为什么不用 Celery——黑盒,这三个机制变成配置项就没有面试证据了(ADR-003)。
- 证据:test_worker_processes_once_and_dedupes_redelivery / test_crash_recovery_claims_stale_pending / test_failure_retries_then_dead_letter。

### Q4 请求路径怎么做到低延迟?
- 一句话:请求路径零 DB 写——鉴权是 Redis 缓存优先(miss 才回源 PG)、准入是一次 Lua、记账全部后台。
- 深挖:每请求 Redis 往返 8→5 的优化过程(EVALSHA/准入合并单 Lua/缓存命中零会话/pipeline 记账/原子 EMA);压测方法论两课——客户端瓶颈冒充服务端(httpx→aiohttp)、附加开销必须在 c=1 无排队测(c=50 的中位差是排队)。
- 证据:EVIDENCE §③;scripts/load_test.py 的口径注释。

### Q5 上游挂了怎么办?
- 一句话:故障转移状态机——连接类错误立即切下一渠道并冷却 60s;429 立即冷却并尊重 Retry-After;EMA 延迟优先路由;全渠道冷却返回 503+Retry-After。
- 深挖:流式下的语义难题(chunk 已发出不可回滚→SSE error 事件如实透传,LiteLLM 同款 caveat);**kill -9 真实演练 10ms 切换**;故障注入抓到的真 bug(ReadError 漏接成 500)说明"演练的价值"。
- 证据:demo_ecosystem.py;test_upstream_connection_reset_maps_502。

### Q6 缓存怎么设计的?(穿透/击穿/雪崩)
- 一句话:两层缓存——鉴权 key 缓存(缓存优先 DB 兜底+吊销主动失效)和精确响应缓存(租户隔离键,TTL 惰性失效,只缓确定性请求)。
- 深挖:穿透=负结果不缓存但 DB 有 UNIQUE+回源兜底;击穿=单 key 回源是单次 GET,无热点击穿面;雪崩=TTL 固定+随机抖动在 backlog;**缓存键含 tenant_id 是安全边界**(租户隔离测试钉死);响应缓存只在 temperature≤0 非流式无 tools 时开启(确定性门控)。
- 证据:test_cache_first_serves_after_db_row_gone / test_cache_tenant_isolation。

### Q7 数据一致性?(Redis 计数 vs PG 账本)
- 一句话:Redis 是快路径计数,PG 账本是最终事实;计数丢失从账本重建(rebuild_from_db 有测试)。
- 深挖:预算是计量制(post-response 原子 Lua(预算+TPM 同一次),在途突发最多超出"在途数×单请求 token",诚实说这不是硬上界;硬上界是限流桶);账本经 Streams 持久队列落库(请求路径零 DB 写)——outbox 水位补投消除"DB 已提交但 XADD 失败"的双写空洞,`(tenant_id, request_id)` UNIQUE + 消费端原子抢占保证崩溃重投最多一行、不跨租户误吞(幂等键带租户域)。
- 证据:test_budget_rebuild_from_db;test_concurrent_accounting_atomic(100 并发恰 1800)。

### Q8 多副本/水平扩展?
- 一句话:所有跨副本状态(限流桶/渠道健康/鉴权缓存/预算)都在 Redis,进程无状态,uvicorn --workers N 或 k8s 任意扩。
- 深挖:压测 1/2/4 worker = 1.00x/2.15x/4.04x 近线性(共享 SQLite 账本在 PG 下无写锁顾虑);Redis 7 AOF 保证任务流重启不丢。
- 证据:evidence/load_workers.md;deploy/docker-compose.yml(appendonly yes)。

### Q9 监控怎么做?线上慢怎么查?
- 一句话:TTFT 与总延迟分开记——差值即网关+传输开销,一眼判断"慢在上游还是网关";维度到租户/渠道/状态类。
- 深挖:Prometheus 指标清单(REQUESTS/TOKENS/TTFT/REQUEST_LATENCY/USAGE_QUEUE_DEPTH/CHANNEL_COOLING/缓存命中);USAGE_QUEUE_DEPTH 持续>0 = 落库跟不上;结构化日志 request_id 贯穿全链路。
- 证据:app/observability/metrics.py;腾讯面经原题"业务层怎么定义成功"——我的答案:按租户/渠道分维度的成功率+TTFT 分布。

### Q10 这个项目最难的点?
- 推荐答:不是某个算法,是**把语义做对**——流式下 fallback 不可回滚怎么办、预算在途突发怎么诚实表述、at-least-once 怎么推导出幂等消费、多副本限流放大坑。每一条都有测试和真实故障演练背书。

## 2. 诚实边界(主动说,反而是加分)

1. 响应缓存是精确匹配;语义缓存为可插拔 embedder(测试 HashEmbedder=词面 bigram 仅开发口径,生产 DashScope embedding;pgvector/RediSearch KNN 在 backlog)。
2. TPM 已升级双窗口加权滑动近似(消除固定窗口突刺);精确滑动日志在 backlog。
3. 预算是计量制不是预扣制;严格预扣需要估算 token,误差同样存在——取舍写在 ADR-002。
4. SQLite 是测试方言,生产 PG;双口径 67+67 全绿(pgserver 内嵌 PG 16.2 复跑),PG 分区等特性在 backlog。
5. PC 接入的 shim 不上报 usage(PC 无 token 计量),账本 token=0 是诚实记录不是缺陷。
6. 管理台无鉴权 UI 层(admin_token 保护 API,页面本身内网用);生产加 SSO 在 backlog。

## 3. 演示剧本(现场 5 分钟)

1. `python scripts/demo_admin.py` → 打开 /admin/web:渠道健康(EMA 实测)、租户、签发 key(只显示一次)、用量对账。
2. curl 一发非流式 + 一发流式(SSE 逐字)。
3. 用超小 budget 的 key 触发 402;超小 rpm 触发 429 + Retry-After。
4. `python scripts/demo_ecosystem.py` → kill -9 agent-a → 10ms 切换 agent-b。
5. `python scripts/demo_pc_gateway.py` → 真 PolicyCompass 答社保公积金问题。
6. /metrics 看 TTFT/缓存命中;pytest 47 绿 + PG 口径复跑收尾。
