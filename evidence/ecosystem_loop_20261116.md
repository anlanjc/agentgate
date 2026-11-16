# 生态闭环演示:askbi → agentgateway → mock OpenAI 上游(2026-11-16 批次)

> 一句话:让 askbi(多 Agent ChatBI)的 LLM 客户端**真的**穿过 agentgate 网关完成鉴权、
> 限流、白名单、预算与记账 —— 全链路同机四进程,LLM 上游是本仓 mock,**零现金**。
> 原始采证(脚本自动生成,逐字留痕):`evidence/ecosystem_loop_raw_20261116.txt`。
> 复跑:`bash scripts/ecosystem_loop_demo.sh`(结束自动清点并杀掉全部演示进程)。

## 一、口径声明(先读)

- **mock only 零现金**:上游是 `scripts/ecosystem_mock_upstream.py`(本仓自建 OpenAI 兼容 mock),
  无任何真实 LLM 计费调用;askbi 的回答内容**质量无意义**(mock 按节点契约返回最小合法输出),
  本演示只验证**调用链、鉴权/白名单/预算三道闸、记账口径**。
- **同机单进程演示口径,非生产部署**:单 worker、SQLite、便携 Redis 5.0.14(独立端口 6399、
  无持久化)、四进程全在本机回环;RPM/TPM/并发等限额是演示值。
- **纯 config 接入**:askbi 仓**零代码改动**,只经环境变量切 provider —— env 名实读自
  askbi `.env.example` 与 `app/llm/factory.py`(`LLM_PROVIDER=openai_compatible` +
  `OPENAI_BASE_URL/OPENAI_API_KEY/OPENAI_MODEL`,pydantic-settings 环境变量优先于 .env 文件)。
  vk raw key 只活在进程环境与运行目录(退出即删),**不入库不入证据**。
- **流式形态如实注记**:askbi 的 openai_compatible 客户端(`app/llm/deepseek.py`)只用
  **非流式** `chat.completions.create`;askbi 的 `/api/ask` SSE 是 askbi→它的客户端方向的
  事件流,不是 LLM 上游流式调用。SSE 形态由 **S2 直连网关的流式请求**单独补证
  (chunk 流 + usage 帧 + `[DONE]`,账本记 TTFT)。
- 采证时间:2026-11-17 01:01(系统时钟;文件名沿用任务批次标签 20261116)。

## 二、架构(文字版)

```
[场景调用方]                      [网关]                                  [上游]
 httpx(steps.py) ─S2/S3/S4a─▶ agentgate :8100 ─ httpx AsyncClient ──▶ mock-openai :8101
                              │  AGENTGATE_MODELS=["demo-chat"]        (FastAPI+uvicorn,
                              │  SQLite(alembic 建表) 账本/账行          /v1/chat/completions
                              │  Redis:6399 db0 鉴权缓存/Lua准入/Streams  JSON+SSE 双形态,
 askbi :8200 ──LLM 调用(S1/S4b)▶│  记账:请求侧 _account → Streams → 落账   节点感知 mock)
 PROFILE=full                  │  admin API: seed vk / usage / budget
 LLM_PROVIDER=openai_compatible┘
 OPENAI_BASE_URL=http://127.0.0.1:8100/v1
 OPENAI_API_KEY=<seeded raw vk>   OPENAI_MODEL=demo-chat
```

数据流(S1 为例):askbi /api/ask → 多 Agent 图(澄清→NL2SQL→执行 duckdb→解读→质检),
每个节点经 OpenAI SDK 调 `http://127.0.0.1:8100/v1/chat/completions` → 网关鉴权(vk 缓存)
→ Lua 准入(rpm/tpm/budget)→ 模型白名单 → 转发 mock → mock 按 system 提示词标记词返回
该节点契约的最小合法输出 → 网关透传并记账(每节点一行账)→ askbi 汇成结构化答案。

## 三、启动步骤(scripts/ecosystem_loop_demo.sh 自动执行)

1. 起便携 Redis(:6399,`--save '' --appendonly no`);
2. 起 mock 上游(:8101,`scripts/ecosystem_mock_upstream.py`);
3. 起 agentgate(:8100):`alembic upgrade head` + `uvicorn app.api.main:app`,env =
   `AGENTGATE_ENVIRONMENT=dev / DB_URL=sqlite / REDIS_URL=redis://…:6399/0 /
   UPSTREAM_NAME=mock-openai / UPSTREAM_BASE_URL=http://127.0.0.1:8101/v1 /
   MODELS=["demo-chat"] / AUTH_ENABLED=true / ADMIN_TOKEN=…`;
4. seed(经 admin API):`POST /admin/tenants`(name=askbi)→ `POST /admin/tenants/{id}/keys`
   (model_whitelist=["demo-chat"],rpm=120,tpm=200k)→ raw key 只落运行目录;
5. 起 askbi(:8200):`PROFILE=full LLM_PROVIDER=openai_compatible OPENAI_BASE_URL=…:8100/v1
   OPENAI_API_KEY=<raw> OPENAI_MODEL=demo-chat uvicorn app.api.main:app`;
6. 场景 + 采证(`scripts/ecosystem_loop_steps.py`,httpx 规避 Git Bash 中文参数改码);
7. 清理:kill 四进程 + redis shutdown,端口逐一清点(全部"已关闭"),运行目录删除。

## 四、逐场景时间线(采证摘要,全文见 raw 文件)

| # | 场景 | 结果 | 关键证据 |
|---|---|---|---|
| S0 | 四进程就绪 + seed | ✅ | tenant_id=78929ccbf7d944cdbfb4a529c112bc5d;vk prefix=ag-_5ISswW;askbi /api/health 报 `llm: openai_compatible` |
| S1-Q1 | askbi 问「上月各渠道销售额是多少」 | ✅ 全链路 | 4 次 LLM 调用(澄清 284 / SQL 459 / QA 148 / 解读 190 token),askbi final = **answer**,table 5 行真实 duckdb 数据(官网 1,088,496.97 … 社交 556,504.71),QA passed;网关 usage:requests 4 / tokens 1081 |
| S1-Q2 | askbi 问「最近三个月销售额趋势如何」 | ✅ | 4 次调用,table 3 行(2026-07/08/09),网关累计 requests 8 / tokens 2391 |
| S2 | 直连网关 SSE(stream=true + include_usage) | ✅ | 18 帧含 usage 帧 + `data: [DONE]`;账本记 **ttft 20.57ms** / latency 21ms / tokens 125,状态 200 |
| S3 | 白名单闸(model=gpt-4o ∉ vk 白名单) | ✅ 403 | `{"error":{…,"code":"model_not_allowed"}}`,准入前失败不记账 |
| S4a | 租户预算兜底闸(预算设为已花费 2516 → 下一调用) | ✅ 402 | `{"code":"tenant_budget_exceeded"}`;DEL vk 鉴权缓存使新预算立即生效(生产等 TTL≤60s,演示口径) |
| S4b | 预算耗尽后 askbi 行为 | ✅ 诚实降级 | 402 → LLMError 不重试 → 澄清节点放行降级、SQL 节点拒答;askbi final = **refusal**「查询生成服务暂时不可用」,仍返回结构化答案(网关不死、askbi 不 500);402 无账行(4xx 不记账纪律) |
| S5 | 采证 | ✅ | usage 终态 requests=9/total_tokens=2516;/metrics:agentgate_requests_total{2xx}=9、tokens prompt 1747/completion 769、TTFT 直方图;账本 9 行全 200 |

## 五、账行样本(usage_records;request_id 已截前 8 位展示,raw 留全量)

```
REQUEST_ID  | MODEL     | PROMPT | COMPLETION | TOTAL | TTFT_MS | LATENCY_MS | STATUS | CHANNEL
e0947290…   | demo-chat | 198    | 86         | 284   |         | 31         | 200    | mock-openai   ← S1-Q1 澄清
d8c354ef…   | demo-chat | 349    | 110        | 459   |         | 7          | 200    | mock-openai   ← S1-Q1 NL2SQL
e5f4374e…   | demo-chat | 77     | 71         | 148   |         | 3          | 200    | mock-openai   ← S1-Q1 质检
ea0c9477…   | demo-chat | 109    | 81         | 190   |         | 6          | 200    | mock-openai   ← S1-Q1 解读
(…S1-Q2 同构 4 行…)
427f98ba…   | demo-chat | 50     | 75         | 125   | 20.57   | 21         | 200    | mock-openai   ← S2 流式(TTFT 入账)
-- 共 9 行,S4a/S4b 的 402 不产生账行(4xx 不记账)
```

要点:channel 恒 `mock-openai`(渠道可观测);`usage` 聚合(9/2516)与账本逐行之和一致;
token 与 askbi 侧节点 usage 报告逐节点吻合(如 SQL 节点 prompt 349 = 网关账行 349);
cost 列为空 = demo 模型未登记价目(不猜价纪律)。

## 六、调试留痕(诚实记录,两处)

1. **mock SSE 帧 bug → 意外成了 GW-9 的现场实证**:mock 初版的流式收尾帧把 usage 帧参数
   传错(`chunk()` 不接受该 kwarg),生成器在"stop+usage 帧"处抛 TypeError 中断 —— uvicorn
   以未完结 chunked 响应关流,网关收到 `RemoteProtocolError: incomplete chunked read`。
   网关行为与设计完全一致:账行记 **502** + 向客户端补发统一信封 error 帧
   (`event: error`,`upstream_read_error_midstream`,request_id 贯穿),metrics 里
   `status_class="5xx"` 计数 +1。排查路径:最小复现(sync/async httpx × 新建/复用连接 全 OK)
   → 复刻网关 UpstreamClient 配置 → uvicorn 报错栈定位到 mock 自身 → 修 mock 后 10/10 全绿。
   **结论:502 账行是网关流中错误语义(对齐 Python GW-9 三分支)的正确行为,非网关缺陷;
   修的是演示 mock 本身。**
2. **Git Bash 中文参数改码**:场景脚本最初用 bash+curl 直发中文 JSON 体,MSYS 向原生 exe
   传参走活动代码页,FastAPI 报 "error parsing the body"(ASCII 体正常)。场景层改由
   `scripts/ecosystem_loop_steps.py` 用 httpx 全程 Unicode 发送 —— 这是"必要的辅助 py"的由来。

## 七、诚实边界

- 本演示**不是生产部署**:单进程网关、SQLite、本机 Redis;预算/限流值是演示值;
  mock 上游的延迟分布不代表真实 LLM。
- askbi 的答案质量无意义(mock 按节点契约返回最小合法输出);被验证的是链路、闸门与计费口径。
- 402 后 askbi 的表现是它自身的 LLMError 降级语义(非网关行为);网关侧 402 只返回统一信封。
- askbi 未发上游流式调用(其客户端为非流式),SSE 形态由 S2 直连网关补证 —— 若未来 askbi
  切流式客户端,网关侧无新增配置(透传语义不变)。
