# ADR-001 语言选型:Python(而非 Go/Java)

状态:已接受 · 2026-11-01 · 关联:[PROGRESS.md](../../PROGRESS.md)

## 背景

面试可预期的质疑:"Python 后端轻量/玩具,高并发要 Java,Python 后端是项目组临时 demo"。需要一条有证据的立场,而不是情绪反驳。

2026-11 调研证据(三路:开源架构 / 30 条 JD / 面经):

- **存在性证明**:LiteLLM proxy = FastAPI + uvicorn,官方数据 1k RPS 下 P95=8ms,生产用户列名 Stripe、Netflix;Dify 后端 = Flask + Celery,千万级 Docker pulls。同拓扑(FastAPI + PG + Redis)是 AI 平台事实标准。
- **JD 层面**:30 条样本 0 条要求 AI 应用后端"必须 Java";"高并发/高可用"同时出现在 Go 岗(月之暗面)、语言未指定岗(MiniMax)、Java 岗(B站/工行)——系统能力词,不绑语言。AI 岗高频词是 Agent/LLM 词族(47%)、可观测(20%)、CI(17%)。
- **诚实的反方证据**:FastAPI 单实例吞吐千级 RPS(V2EX 实测帖 ~2.5-8k RPS/worker 量级);Reddit 2025 年评论后端 Python→Go;LiteLLM 热点核心下沉 Rust。Python 单机吞吐天花板确实低于 Go/Java。

## 决策

用 Python 3.12 + FastAPI + asyncio 实现,理由三点:

1. **场景匹配**:LLM 网关是 IO 密集型——瓶颈在上游模型的百毫秒~秒级延迟(TTFT/生成),框架开销是毫秒级,语言吞吐不构成瓶颈;路由/鉴权/计费逻辑与 AI 生态(后续接 Agent 上游、评测脚本)同语言,全栈一致。
2. **目标岗位匹配**:AI 应用后端 JD 的默认栈即 Python;本项目的考察对象是系统能力(限流原子性、at-least-once、零 DB 写、可观测),这些与语言无关。
3. **承认边界**(写进 README,不回避):Python async 单实例千级 RPS 是公开事实;本项目以"压测数字 + 口径"回应,并给出下沉触发条件——①超热点路径(→ Rust/Go 核心)、②极低延迟尾部要求、③分发形态要求单二进制(One API 选 Go 的原因)。触发前不预设优化。

## 后果

- 正面:与 workspace 另两项目同栈,生态接入零成本;面试追问"GIL/多 worker/IO 密集"有真实落点(压测脚本 + 调优前后对比)。
- 负面:单机吞吐数字不会漂亮——**叙事改为正确性+可观测+口径严谨**,不与 Go 比吞吐峰值。
- 面试话术五步(承认取舍→场景匹配→亮边界→数字收尾→退路姿态)沉淀于 EVIDENCE.md §面试。
