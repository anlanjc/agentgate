# 简历条目 · agentgate(2028 届校招,AI 应用/Agent 工程岗)

## 简历项目区(一行式,选一)

**主推版(信息密度优先):**
> **agentgate · 多租户 LLM/Agent 网关**(Python/FastAPI):自研 LiteLLM 核心链路——OpenAI 兼容代理与 SSE 流式;virtual key 缓存优先鉴权;Redis+Lua 分布式限流(并发 100 恰放行 50,零超卖);请求路径零 DB 写,Streams 任务队列 at-least-once+幂等(崩溃 XCLAIM 恢复/死信);故障转移状态机(429 冷却+Retry-After+EMA 路由),kill -9 演练 10ms 切换;精确响应缓存(租户隔离);Prometheus 可观测(TTFT);每请求附加开销 3.7ms,多进程 1→4 worker 4.04x 近线性;SQLite/PostgreSQL 双口径 47+47 测试全绿;真实 PolicyCompass 已接入,自家网关管自家 Agent。

**精简版(版面紧张时):**
> **agentgate · 多租户 LLM 网关**(Python):OpenAI 兼容代理+SSE;Redis+Lua 限流零超卖;零 DB 写记账;Streams at-least-once+幂等;故障转移 kill -9 演练 10ms 切换;附加开销 3.7ms;双数据库口径 94 测试全绿;真实 Agent 应用已接入。

## 面试三句话(标准回答)

1. **为什么做**:简历三层叙事缺平台层——检索(PolicyCompass)、多 Agent(AskBI)之后,Agent 工程岗真正的稀缺能力是服务化;而且这个品类真实存在(LiteLLM/OpenRouter/OneAPI),不是编造的业务。
2. **怎么做**:每个设计点有师承(LiteLLM 架构文档/Higress 插件/APISIX 短板),有测试钉死(并发恰放行/恰记账/RST 回归),有真实演练(kill -9 切换、kill Redis 降级、故障注入抓到 2 个真 bug)。
3. **为什么 Python**:IO 密集场景瓶颈在上游模型秒级延迟;LiteLLM 同架构 P95=8ms@1k RPS 且 Stripe/Netflix 生产采用;我知道下沉触发条件(热点路径→Rust/Go),到不了那个量级不预设优化。

## 与另两项目的组合叙事

> 我的项目是一套完整的 AI 应用工程栈:**PolicyCompass** 证明检索与 CRAG 工程能力,
> **AskBI** 证明多 Agent 编排与评测纪律,**agentgate** 证明把这两类应用真正"服务化"的
> 后端能力——鉴权、限流、计费、流式、容错、可观测。第三项目的上游就是前两个项目,
> 三个项目互相引用而非三个孤立 demo。
