# 评审 2.0 修复讲解手册(GW-3 ~ GW-12)

> 2026-11-09 · S9-AG 车道 · 面向项目作者本人的复习材料。
> 每条:现象/危害 → 根因(file+函数,行号为**修复前**锚点)→ 修法与被否掉的替代方案 → 回归测试 → 面试 30 秒讲法 → 关联八股。
> 评审报告:`interview/reviews/review_report_2.md`(只读);本轮 commit:f9c92d5 / 8dbd8dd / c4cf6ba / d4cbf3b / 142e2f3 / 57ec9b6 / df29b13 / f4db0fb / 8c7358a / ce793dd(+lint 3afb728、scripts 7065a67)。
> 终态口径:SQLite **121 passed + 2 skipped** / PostgreSQL 16 **123 passed** / ruff 全绿 / 迁移链真 PG 验证通过。

---

## GW-3 幂等锁:属主 token + TTL 对齐执行期(f9c92d5)

**现象/危害**:带 Idempotency-Key 的非流式请求打慢上游(>60s),锁先过期;客户端重试 SETNX 成功,与首请求**并发执行**——两行账本、预算双扣。更阴的是首请求收尾时无条件 `delete` 锁,删掉的是**重试方**的锁,第三次重试又能拿锁,交叉释放滚雪球。

**根因**:`app/idempotency.py` 的 `acquire()`(修复前 :46-51)SETNX 值恒 `'1'`——没有属主身份;`release()`(:54-58)无条件 delete——不知道锁是不是自己的。`LOCK_TTL_MS = 60_000`(:22)硬编码,而网关自己的渠道读超时默认 300s(config.py:27)——锁的寿命比合法执行期短,过期是必然不是异常。

**修法**:
1. SETNX 值改成 `uuid4().hex` 随机属主 token;`acquire` 返回 `(是否放行, token)` 三态:拿到锁 / 被占(409)/ Redis 故障降级(token=None,仍放行)。
2. `release` 改 Lua **compare-and-delete**:`get == 自己的 token 才 del`,原子且只删自己的锁;返回 bool 表示"是否仍持有"。
3. TTL 从 60s 提到 300s,并且新增配置 `idempotency_lock_ttl_seconds`(config.py),约定 **锁 TTL ≥ 渠道 read_timeout 上限**。心跳续租做过的方案被否掉:续租引入后台定时器与取消语义,复杂度远超"把 TTL 配足",而且 TTL=300s 已覆盖最长合法执行期。
4. 级联修一处:`_non_stream_core` 里 2xx 路径先 `release` 再按返回值决定是否 `store`——release 返回 False 说明锁已被重试方接管,本次执行**放弃写重放库**,避免旧响应覆盖新执行者的响应。否掉的替代:先 store 再 release+检测后删除——那个方案在"重试方比我先完成"的窗口会把对方存好的响应连带删掉。

**回归测试**(`tests/test_idempotency.py`):
- `test_release_does_not_delete_taken_over_lock`:50ms 小 TTL 模拟过期 → 重试方接管 → 首请求 release 返回 False 且锁值仍是重试方的 token;属主本人 release 返回 True 并删干净。
- `test_release_without_token_is_safe_noop`:降级路径 token=None 的 release 不误删别人的锁。
- `test_lock_ttl_default_covers_read_timeout`:钉住 `idempotency_lock_ttl_seconds >= upstream_read_timeout` 的不变量。

**面试 30 秒**:分布式锁有两个经典坑我们全踩了:一是没有属主 token,释放是无条件 delete——A 超时后 B 接管,A 收尾把 B 的锁删了,第三个人又能进来,这就是"误删他人锁";二是 TTL 短于业务执行期,60s 的锁跑 300s 的上游请求,过期是必然。修法是 Redis 官方推荐的标准姿势:SETNX 值写随机 token,释放用 Lua 做 compare-and-delete 原子校验,同时把 TTL 配到不小于最长执行期。释放时发现锁不属于自己,还顺带解决了"旧响应覆盖新响应"的存储竞态。

**关联八股**:分布式锁 TTL 与看门狗(Redisson watchdog 为什么存在);Lua 保证 compare-and-delete 原子性(get+del 两条命令之间可以插队);幂等键语义(Stripe/Payment API 的 Idempotency-Key 约定)。

---

## GW-4 任务终态回写:状态守卫 + 租约时钟统一(8dbd8dd)

**现象/危害**:k8s 清单 replicas=2。B 副本时钟比 A 快 ≥30s(或 A 的 handler 超 30s),B 经 XCLAIM 接管同一任务,**双方并发跑 handler**;后写者无条件翻转终态——done 被覆盖回 pending 再跑一遍,或者反向把 done 翻成 dead 并投**假死信**。task_records 的状态与事实脱节。

**根因**:`app/queue/streams.py` 的 `_process()`:抢占是条件 UPDATE(status='processing' 前置守卫,rowcount 判定),但三处终态回写——done(修复前 :372-377)、dead+DLQ(:384-393)、pending 重试(:408-413)——都只按 id 无条件 UPDATE,没有"我还持有处理权吗"的守卫。另外 `stale_cutoff = _utcnow() - claim_idle`(:345)用**本进程本地时钟**去比较**别副本写入**的 updated_at,时钟不同源,偏差直接变成误接管。

**修法**:
1. 三处终态回写全部加 `TaskRecord.status == "processing"` 守卫;rowcount != 1 说明另一副本已接管或已写终态——记 warning 后**直接 return,不 ACK**。消息留在 PEL,下一轮 reclaim 会看到"已 done/dead"走幂等清理(:335 的状态检查),或看到 pending 正常重投。不 ACK 是刻意的:at-least-once 纪律下,只有确认结果已落库或已被幂等吞掉才能 ACK。
2. 租约时钟统一 Redis TIME:新增 `_redis_now()`,抢占的 stale_cutoff 和所有 updated_at 写入都用它——跨副本比较的是同一个钟。Redis 拿不到时退回本地(单副本/测试口径无跨副本问题,降级安全)。否掉的替代:DB `func.now()`——SQLite 的 CURRENT_TIMESTAMP 是无时区秒级字符串,和 Python 写入的带微秒 UTC ISO 串格式不一致,双方言比较语义不可靠,这是个 SQLite/PG 双方言项目。

**回归测试**(`tests/test_m2_streams.py`):
- `test_terminal_write_lost_race_keeps_winner_result`:handler 执行中另一副本抢先写 done → 本副本的终态写 rowcount=0 → 接管者的 result 不被覆盖,消息最终被幂等清理(xlen 归零)。
- `test_dead_write_lost_race_no_false_dlq`:另一副本已写 done,失败副本(attempts=3)不再投假死信、不把 done 翻成 dead(DLQ 为空)。
- `test_lease_timestamp_uses_redis_clock_not_local`:monkeypatch 把本地 `_utcnow` 拨快 120s,updated_at 仍等于 Redis 时钟(±5s)——修复前会带着 2 分钟偏差。

**面试 30 秒**:我们的任务队列是 at-least-once + 幂等消费,消费端靠条件 UPDATE 原子抢占。评审发现抢占是守卫的,但终态回写是裸的——双副本接管场景下,后写者会把前写者的终态翻掉,甚至投假死信。修法是把状态机贯彻到底:所有状态迁移都带前置条件 UPDATE,rowcount=0 就放弃写入且不 ACK,让 reclaim 的幂等检查去收尾。第二个问题是租约判定用各进程本地时钟,快钟副本会提前接管——把时间戳写入和比较全部统一到 Redis TIME,跨副本才真正同源。

**关联八股**:at-least-once vs exactly-once(投递语义 + 幂等消费 = 效果上不重复);可见性超时/租约(visibility timeout 的含义);分布式系统为什么不能用本地时钟做互斥判定(Lamport/物理时钟不可靠);CAS(条件 UPDATE 的 rowcount 就是数据库版 compare-and-set)。

---

## GW-5 流式断开漏计:finally 顺序 + shield + 显式记账(c4cf6ba)

**现象/危害**:客户端在流中途断开,取消经 anyio cancel scope 级联——`relay()` 的 finally 里**第一个 await**(`resp.aclose()`)自己再抛 CancelledError,排在它后面的 usage/latency 赋值和 TTFT 指标**永远执行不到**。上游已产生的 token 在 TPM 计数、预算、PG 账本三处同漏。更糟:ASGI spec≥2.4 的服务器上,断开=下游 send 抛 OSError → background 整个不跑,连账本行和并发槽释放都没有。

**根因**:`app/api/v1_chat.py` 的 `relay()`(修复前 :569-587)finally 顺序错误——先 `await resp.aclose()`(:570)再写 `holder`(:571-582)。取消作用域内任何 await 都是取消点,把同步赋值排在 await 之后等于把它们排在"大概率不会执行"的路径上。记账挂在 BackgroundTask(:606),而 background 是否执行取决于服务器 spec 版本分支(starlette `StreamingResponse.__call__`):spec<2.4 断开后仍跑,spec≥2.4 直接跳过——记账的正确性不能建立在服务器实现细节上。

**修法**(三层):
1. **纯内存赋值前移**:finally 里先做 usage/latency/metrics/log(全部同步操作),之后才碰 await。
2. **shield 包裹**:`resp.aclose()` 和显式记账都 `asyncio.shield()` 包裹——取消作用域内的普通 await 会再抛 CancelledError,shield 让它们在独立任务里跑完,await 本身抛的 CancelledError 捕获后放行(外层 except 分支的 raise 保证取消语义不丢)。
3. **记账不依赖 background**:holder 加 `completed`/`accounted` 两个标志。生成器没自然跑完 = 客户端断开(spec 2.3 走 CancelledError 分支标 499;spec 2.4 的 GeneratorExit 路径由 completed=False 探测,同样改判 499,不伪装 200)→ finally 里 shield 显式调用 `_account`;`accounted` 是单向闸——background 如果仍执行(spec<2.4),`make_record()` 返回 None,`_account` 见 None 让位,双口径下都恰好记一次。

被否掉的方案:①记账全挪进 finally、彻底删 background——正常路径完全没必要,background 语义在正常完成时是对的;②用 try/except OSError 包 send 来探测 2.4 断开——那是 starlette 内部的事,生成器侧只能靠 GeneratorExit/completed 探测。

**回归测试**(`tests/test_stream_reliability.py`,裸 ASGI 驱动 + **真 TCP 上游**——httpx 的 ASGITransport 会缓冲整条上游流,aiter_raw 永不真挂起,取消没有检查点可投递,必须真 TCP 才能复现):
- `test_stream_disconnect_spec23_still_bills`:uvicorn 0.54 生产口径(spec 2.3,anyio 取消),usage 块到达后断开 → 账本 1 行、status=499、total_tokens=18、ttft 非 None(修复前 usage 恒 None/token 恒 0)。
- `test_stream_disconnect_spec24_still_bills`:spec 2.4(send 抛 OSError → ClientDisconnect)→ background 不跑,relay 收尾显式记账兜底,账本仍有 499+18 行(修复前连行都没有)。测试里有个坑:ClientDisconnect 的 traceback 引用 StreamingResponse 帧链,生成器不 GC、GeneratorExit 收尾就不发生——引用必须隔离释放后才轮询账本。
- `test_stream_normal_completion_no_double_accounting`:正常完成仍走 background,恰好一行(钉住 accounted 闸不会误伤正常路径)。

**面试 30 秒**:流式代理的记账发生在生成器 finally 里,而客户端断开是取消——anyio 的取消作用域内,finally 里第一个 await 就会再抛 CancelledError,排在其后的同步赋值等于死代码,这就是断开漏计。修复三板斧:同步赋值全部前移;连接关闭和记账用 asyncio.shield 包进独立任务,不被取消风暴打断;记账不再押注 background 会不会执行——ASGI spec 2.4 前后服务器行为不同,我们在生成器收尾显式记账,用单向闸保证与 background 恰好记一次。

**关联八股**:asyncio/anyio 取消语义(CancelledError 会打断取消作用域内的每个 await;finally 里吞 CancelledError 的边界);asyncio.shield 的语义(护住内层任务,不护住本次 await);ASGI spec 2.3/2.4 断开通知差异;GeneratorExit 与 async generator 收尾。

---

## GW-6 prod 校验器:fail-closed 反转(d4cbf3b)

**现象/危害**:`AGENTGATE_ENVIRONMENT=production`(或 live/main/任何变体)启动——校验器精确匹配 `'prod'` 不中,直接 return:admin_token 留空则 `/admin/**` 全不设防、auth 可关、渠道密钥明文,三道安全闸静默全灭,无任何告警。误配即裸奔。

**根因**:`app/config.py` 的 `_fail_closed_in_prod()`(修复前 :72-87):`if self.environment.lower() != "prod": return self`——**黑名单思维**(只认得一个 prod),而正确的是**白名单思维**(只放行确认过的 dev 环境)。

**修法**:定义 `NON_PROD_ENVIRONMENTS = {dev, test, testing, staging, local}`(:8-10),校验器反转:`in 白名单 → 豁免;否则(含 prod/production/live/main/大小写变体/空串)→ 三道闸全查`。错误信息带环境名与主密钥生成命令(顺手服务 GW-7)。否掉的替代:"未识别环境打 error 要求显式确认"——多一个环境变量等于多一个能配错的东西,fail-closed 的本意就是让默认路径不安全地失败。

**回归测试**(`tests/test_security.py`):`test_environment_fail_closed_unknown_names_treated_as_prod`——8 个未识别值(含 PROD/Production/空串)缺 admin_token 一律 ValidationError;白名单 6 个值正常豁免;补齐安全配置后 production 可启动(校验的是闸门存在,不是一刀切禁用)。

**面试 30 秒**:安全校验最忌精确匹配——我们原来 `!= 'prod'` 就跳过校验,运维写个 'production' 三道闸全静默失效。修法是把默认反转:只有显式白名单里的开发环境名豁免,一切未识别值按生产校验。这是 fail-closed 的标准应用:不确定就不放行,让误配在启动时爆炸,而不是在公网上爆炸。

**关联八股**:fail-open vs fail-closed(优化路径可降级、安全闸必须 fail-closed——和本仓准入链的 Redis 故障 503 同一原则);配置校验在启动时(12-factor,配置错误要在 deploys 边界暴露)。

---

## GW-7 compose 缺 Fernet 主密钥:强制注入位 + fail-fast(142e2f3)

**现象/危害**:`cd deploy && docker compose up -d`——compose 置 ENVIRONMENT=prod 但没有 CHANNEL_ENC_KEY 注入位 → prod 校验器拒绝启动 → gateway crash-loop;运维最省事的"修复"是删掉 ENVIRONMENT=prod——与 GW-6 叠加,三道安全闸随之全灭。PRODUCTION_GAP 还宣称"装完 Docker、配一个 env 即可生产部署"。

**根因**:`deploy/docker-compose.yml`(修复前 :11-23)gateway environment 块有 ADMIN_TOKEN 的 `:?` 强制项,漏了 CHANNEL_ENC_KEY——两个密钥域(admin token/加密主密钥)只强制了一个。

**修法**:compose 增加 `AGENTGATE_CHANNEL_ENC_KEY: ${AGENTGATE_CHANNEL_ENC_KEY:?...}`(与 admin_token 同款,缺一个 `docker compose up` 直接报错,不给 crash-loop 机会);PRODUCTION_GAP 结论改为"配齐**两个** env"附生成命令;网关侧 fail-fast 复用 Settings 校验器(GW-6 强化后的报错含生成指引)。**诚实边界:本机无 Docker,验证是静态口径**——写了 `tests/test_deploy_config.py` 三条静态回归(compose 结构断言 `:?` 必填、k8s secret 占位存在、Settings 缺密钥拒绝启动且报错含 Fernet 指引),容器真启动冒烟留待部署日,commit 与 PRODUCTION_GAP 均已注记。否掉的替代:compose 给默认密钥——等于把主密钥提交进仓库,比没有更糟。

**回归测试**(`tests/test_deploy_config.py`):`test_compose_production_topology_requires_master_key` / `test_k8s_secret_template_has_master_key_placeholder` / `test_gateway_startup_fails_fast_without_master_key_in_prod`。

**面试 30 秒**:部署配置的完整性要靠"缺了就起不来"来保证,而不是靠 README 嘱咐。compose 里两个密钥都写成 `${VAR:?message}` 强制项——shell 语法层面的 fail-fast,变量缺失时 `docker compose up` 直接报错,不会出现容器 crash-loop 后运维删掉 prod 标志裸奔上线的连锁反应。本机没有 Docker,所以静态回归先把 compose 的结构钉死,真启动冒烟留给部署日,文档里诚实注记。

**关联八股**:基础设施即代码的配置校验(compose 变量插值 `:?` vs `-`);secrets 管理(密钥不进 VCS,Fernet 主密钥的轮换边界);crash-loop 与健康检查(为什么 fail-fast 要发生在调度层而不是容器层)。

---

## GW-8 webhook_secret 明文散落:加密落库 + payload 净化(57ec9b6)

**现象/危害**:任一租户配了 webhook_secret,每次 200 请求把它明文塞进任务 payload → XADD 进 Redis 流(**AOF 落盘**)并永久存 task_records.payload;DB 列本身也是明文。拖一份备份/RDB 即得全部租户的验签密钥——可伪造签名冒充网关回调。与渠道密钥"入库即密文"的纪律双标。

**根因**:`app/api/admin.py` 的 `create_tenant`(:74)/`rotate_webhook_secret`(:104)直接赋值,没走 `encrypt_secret`;`app/api/v1_chat.py` 的 `_account`(:433)把 `tenant.webhook_secret` 整个塞进队列 payload。

**修法**:
1. 入口:`encrypt_secret()` 包一层,创建与轮换都落密文(`enc:v1:` 前缀,复用渠道密钥的同一套 Fernet 机制);列宽 128→384(128 字符明文的密文约 243 字符),新增迁移 `d7e5a9c31b42`(真 PG 验证过)。
2. 队列:**payload 只带 tenant_id**——webhook_url/secret 投递时才查库解密(`webhooks.py` 新增 `_resolve_webhook_target`)。密钥(无论明文密文)从此不进 Redis 流/AOF/任务账本。旧格式 payload(直带 url/secret)保留兼容,部署窗口内未消费的历史消息不丢。
3. 租户已删/未配 url:投递按 skipped 完成,不重试不留死信。否掉的替代:payload 带密文——密文也在 Redis/AOF 里躺着,拖备份+拿到主密钥才泄露,但"密钥资料不该出现在消息层"是更清晰的边界,而且顺带修复了轮换竞态(投递时永远用最新 secret)。

**回归测试**(`tests/test_webhook_signature.py`):
- `test_webhook_secret_encrypted_at_rest_and_payload_clean`:创建+轮换后 DB 行是 `enc:v1:` 密文且可解回;任务 payload 无 `webhook_secret` 键、无明文;payload 带 tenant_id。
- `test_webhook_delivery_decrypts_stored_secret`:密文直落库 → 投递时解密 → HMAC 验签仍通过(端到端不因加密断裂);任务全部 done 无死信。
- 存量 `test_webhook_signature_e2e` 不动而通过——它直插明文行,走"无前缀=存量明文兼容"路径,签名语义不变。

**面试 30 秒**:同一类秘密在同一个系统里必须同一套纪律。渠道密钥早就入库即 Fernet 密文了,webhook 签名密钥却明文躺在任务表里、还随每条消息广播进 Redis 流和 AOF——拖个备份就拿到全部租户的验签能力。修法两层:存储层入库即密文(复用同一主密钥机制,加一个列宽迁移);传输层消息只带租户 ID,投递时查库解密——密钥彻底退出消息面,顺带把"轮换后投递用新密钥"的竞态一起修了。

**关联八股**:静态加密 at-rest(信封加密为什么常用数据密钥+主密钥两层);秘密的传播面控制(最小暴露:消息只带标识不带凭据);HMAC 时序安全比较(hmac.compare_digest)。

---

## GW-9 SSE 异常帧:统一信封 + [DONE] 探测(df29b13)

**现象/危害**:两种坏形态。①mid-stream 上游读错误只发一帧自定义 `data: {"message": ...}`——既不是网关统一信封也不是 OpenAI error 帧,客户端要为流式再写一套解析;②更隐蔽的**干净截断**:上游发一半关流、没发 [DONE] 也没异常——relay 正常走完,无错误帧,账本**静默记 200**,客户端把截断当完成。

**根因**:`v1_chat.py` 的 relay READ_ERRORS 分支(修复前 :565-568)硬编码自定义帧;`app/upstream/sse.py` 的解析器明确跳过 `[DONE]`(:66)却从不记录"见过没有"——终止标记的语义信息被丢弃;README 只说"SSE error 事件如实透传"没定义契约。

**修法**:
1. `SSELineParser` 记录 `saw_done`(顺手让 `DONE_MARKER` 常量真正被引用),`UsageAccumulator` 透出属性——解析器只旁观,转发仍零改动。
2. 新增 `_sse_error_frame()`:异常帧 = `event: error` + **网关统一错误信封**(message/type/code/request_id,与 HTTP 侧 GatewayErrorBody 同构)。
3. relay:自然结束后 `if not acc.saw_done` → 补发 error 帧(code=`stream_truncated_no_done`)且 `holder["status"]=502`;READ_ERRORS 分支换统一帧(code=`upstream_read_error_midstream`)。
4. 契约写进 README 硬点 6:**正常完成=有 [DONE] 无 error 帧;异常终止=无 [DONE],最后一帧是 event: error + 统一信封;账本同步 502**。否掉的替代:异常后再补发 [DONE]——[DONE] 必须保留"正常完成"的唯一语义,补发会让按 [DONE] 判完成的客户端把失败当成功。

**回归测试**(`tests/test_stream_reliability.py`):
- `test_stream_break_yields_unified_error_frame_and_502_ledger`:mock stream_break(发一半关流)→ 响应无 [DONE]、含 event: error、末帧 data 解析出统一信封(code=request_id 齐全)、账本 1 行 status=502(修复前 200)。
- `test_sse_error_frame_shape_is_gateway_envelope`:帧构造器输出的就是 GatewayErrorBody 形状(单元钉形状)。

**面试 30 秒**:SSE 流式代理有个诚实性问题:响应头 200 提交之后,失败只能靠帧语义表达。我们把契约钉死——[DONE] 是"正常完成"的唯一判据,mid-stream 异常和上游干净截断都不发 [DONE],最后一帧统一是 error 事件加网关标准信封,和 HTTP 错误同一套 schema,账本同步记 502。实现上解析器本来就是旁观的,加一个"见过 [DONE] 没"的布尔位就够了,转发路径仍是零改动透传。

**关联八股**:SSE 语义(event/data 字段、终止标记设计);统一错误信封(一套错误契约的客户端成本);透传与旁路解析分离(字节零改动 + 元数据旁观)。

---

## GW-10 渠道 base_url:入口校验 + 调度兜底(f4db0fb)

**现象/危害**:POST /admin/channels 传 `base_url: "localhost:9000/v1"` → 200 registered。此后该模型请求:坏 URL 的异常(httpx.UnsupportedProtocol/InvalidURL)在**发送时**才抛、不在捕获元组里 → 未处理 500;且因为异常没走 `mark_failure`,渠道**永不冷却**持续留在候选——健康备渠永不接管,"全渠道冷却→503"的故障转移契约整个失效。

**根因**:`app/api/admin.py` 的 `ChannelCreate.base_url` 裸 str(:316),`register_channel` 直接构造 UpstreamClient;`app/api/v1_chat.py` 的 `_dispatch_*` 与外层只捕 `UPSTREAM_TRANSPORT_ERRORS`(client.py:19-21),URL 类异常在面外。

**修法**:
1. 入口闸:`_validate_base_url()`——urlparse 后要求 scheme ∈ {http,https} 且有 netloc,否则 400 统一信封(code=`invalid_base_url`),不注册不落库。不用 pydantic `AnyHttpUrl`:它会把 URL 归一化(补尾斜杠等),改变入库原值,显式校验保持"存什么是什么"。
2. 调度兜底(存量坏行/手工配置仍会走到运行时):client.py 新增 `UPSTREAM_URL_ERRORS = (InvalidURL, UnsupportedProtocol)` 与 `ROUTE_ERRORS = 传输异常 + URL 异常`;两处 dispatch 与两处外层捕获全部换 ROUTE_ERRORS——先 `mark_failure`(渠道冷却,备渠接管)再继续;全坏时 `_classify_upstream_exc` 映射 502/code=`upstream_invalid_url` 的统一信封,不再是裸 500。

**回归测试**(`tests/test_m4_channels.py`):
- `test_register_channel_rejects_bad_base_url`:四种坏 URL 全 400 + code 正确 + 不注册不落库;http/https 正常注册。
- `test_bad_url_channel_fails_over_instead_of_500`:绕过入口闸直register坏渠道(模拟存量坏行)→ 请求 200 且命中健康渠道、坏渠道 fail_streak=1(mark_failure 确实执行);仅坏渠道时 502 统一信封而非 500。

**面试 30 秒**:这是"校验时机 + 异常面"双失误的典型:URL 合法性该在注册边界校验,我们拖到发送时才被 httpx 打脸;而发送时的异常又不在故障转移的捕获面里,于是未处理 500 之外更糟——渠道永不进冷却,故障转移状态机对这类故障整个失明。修法两层:注册时 urlparse 校验回 400;运行时捕获面扩到 URL 类异常,让坏渠道和 5xx 一样走 mark_failure → failover → 502 的同一状态机。

**关联八股**:输入校验在边界(Fail Fast at the boundary);SSRF 的反面教材(scheme 白名单同样是防内网外联的第一道闸);异常分类与故障转移的健康信号(mark_failure 是状态机的输入)。

---

## GW-11 422 默认信封:统一错误契约(8c7358a)

**现象/危害**:请求体校验失败(缺 messages 等)走 FastAPI 默认 422 + `detail` 数组——无 type/code/request_id;同网关 401/403/429/402/503 全是 `{"error": {...}}` 信封。两套契约并存,客户端错误处理写两遍;现有测试只断言状态码,信封形状没有测试钉,谁改了都不知道。

**根因**:`app/api/main.py` 的 `create_app`(修复前 :159)只注册了 `GatewayError` 处理器;`RequestValidationError` 是 FastAPI 内置路径,不注册就用默认。

**修法**:errors.py 新增 `validation_error_handler`——输出统一信封(type=`invalid_request_error`、code=`invalid_request_body`、request_id),message 里带字段定位(前 5 条);**状态码选 400** 对齐 OpenAI(无效参数即 400),注册到 `add_exception_handler`。否掉的替代:保留 422 只换信封体——OpenAI 生态的客户端普遍按 400 处理参数错误,422 是 FastAPI 的实现细节不是领域语义;既选了"OpenAI 兼容"这个定位,状态码也该对齐。测试从只断言 422 改为断言完整信封形状(四个键齐全、字段定位在 message 里)。

**回归测试**(`tests/test_m0_proxy.py`):`test_validation_error_unified_envelope`(原地改造)——400 + `{"error": {message/type/code/request_id}}` 全键断言。

**面试 30 秒**:API 设计里"一个错误一个形状"是客户端友好度的底线。我们的网关主张统一错误信封,但 FastAPI 的 422 校验错误没接进这套体系——客户端对同一个"请求错了"要写两种解析。修法是给 RequestValidationError 注册处理器,统一输出信封并把状态码对齐到 OpenAI 的 400;顺手把信封形状用测试钉死,契约从此有防回归护栏。

**关联八股**:统一错误信封设计(type/code/request_id 三件套为什么都有用);422 vs 400 的语义边界(RFC 7231:400 malformed syntax / 422 unprocessable——实践里对齐生态比抠 RFC 更重要);异常处理器是契约的一部分。

---

## GW-12 文档口径:测试数统一为实跑值(ce793dd)

**现象/危害**:同一事实三套数字并存——README 95+97 / badge 95+2skip / PROJECT_BOOK 一处 95+97 一处"pytest 47 绿" / EVIDENCE :9 "67/67 终态"与 :199 "97+2skip 终态"互打脸;PROJECT_BOOK 的 kill -9 "10ms 切换"是演练第一天的旧口径(EVIDENCE :159 早已重测 2047ms)。面试前背剧本背到旧数字,会被自己的 EVIDENCE 反证。

**根因**:不是哪个文档写错了,而是**流程缺失**:每轮扩测试/重测数字后,同步面靠记忆列清单。文档批的根因从来是"单一出处"缺位。

**修法**:数字全部以**本日实跑**为准——SQLite 121 passed + 2 skipped、PG 123 passed(先跑出数字再动笔,一个旧值都不照抄);README badge 与验收节、PROJECT_BOOK 三处(含 kill -9 10ms→2047ms 且注记"同模型池双渠道"场景修正)、PITCH 两处、RESUME 两处全部刷新;"终态"收敛到 EVIDENCE 一处(2026-11-09 节,并注明迁移链扩展至 d7e5a9c31b42),67/67 与 97+2skip 降级为**带日期的历史快照**保留(历史记录不篡改,只去掉"终态"字样并指向现口径)。过程中用 grep 扫残留,抓到三处清单外的漏网(PROJECT_BOOK:89、README badge、PITCH 口径行)——正好复现了 EVIDENCE :194 那条教训:"同步面必须 grep 扫全文档,不能凭记忆列清单"。

**回归测试**:文档批次无代码测试——守护手段是流程:数字单一出处(EVIDENCE 终态节)+ grep 全扫。

**面试 30 秒**:文档口径漂移的本质是同一个数字有 N 个出处。我们把测试总数收敛到 EVIDENCE 的"终态"节做唯一出处,其他文档一律引用;每次数字变化用 grep 扫全库同步,而不是凭记忆列清单——这轮清扫自己就抓到了三处清单外的漏网。旧数字不删,降级成带日期的历史快照:历史可追溯,现口径唯一。

**关联八股**:single source of truth(文档与配置同理);文档即代码(数字改动进 PR、可 grep、可评审);诚实边界(历史快照不篡改,只标注归属)。

---

## 附:本轮顺手销账与验证口径

- `scripts/verify_pg_migration.py` 的 DROP 清单漏 `audit_logs`,存量 .pgdata 上迁移验证假红(预存坑,与本批无关但挡住了 GW-8 迁移的真 PG 验证)——补齐后迁移链 `3215fe6889f5→…→b4c19d2e6f78→d7e5a9c31b42` 真实 PG 16.2 验证通过(7065a67)。
- ruff 清理:migrations 模板旧式 typing + 测试残留导入,`ruff check .` 全绿(3afb728)。
- 测试增长对账:基线 102+2skip → GW-3 +3 → GW-4 +3 → GW-5 +3 → GW-6 +1 → GW-7 +3 → GW-8 +2 → GW-9 +2 → GW-10 +2 → GW-11 原地改造 = **121+2skip**;PG 口径同套件 123(pgvector 2 项在 PG 真跑)。
- 全程未做真模型调用、未 push、未写 PROGRESS.md、未提交任何真实密钥。
