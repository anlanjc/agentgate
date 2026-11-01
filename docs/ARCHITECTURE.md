# ARCHITECTURE · agentgate

> 每个设计点标注师承(来自 2026-11 开源调研),面试时可引出处。调研结论:LiteLLM(FastAPI+uvicorn)1k RPS 下 P95=8ms、生产用户含 Stripe/Netflix;同类平台拓扑一律为「API 服务 + worker + PG/MySQL + Redis + 专用存储」。

## 分层

```
                        ┌────────────────────────────────────────────┐
                        │                admin API                   │
                        │  租户/key 签发、配额、渠道管理、指标           │
                        └───────────────────┬────────────────────────┘
                                            │
 client ──► middleware(request_id/计时) ──► /v1/chat/completions
                                            │
                        ┌───────────────────▼────────────────────────┐
                        │  auth: virtual key(缓存优先 Redis→PG)      │  ← LiteLLM "Life of a Request"
                        ├────────────────────────────────────────────┤
                        │  ratelimit: Redis+Lua 令牌桶(token 级)      │  ← LiteLLM redis_requirements
                        │     文档记录:单机限流 N 副本放大 N 倍的坑      │     + Higress ai-token-ratelimit
                        ├────────────────────────────────────────────┤
                        │  quota: 配额预检 + 异步记账(请求路径零 DB 写) │  ← LiteLLM 架构文档原话
                        ├────────────────────────────────────────────┤
                        │  router: 渠道注册表 + 故障转移状态机          │  ← LiteLLM routing/reliability
                        │     重试→冷却(429 立即+Retry-After)→fallback │
                        ├────────────────────────────────────────────┤
                        │  upstream: httpx AsyncClient + SSE 透传     │  ← 流式 fallback 语义 caveat 主动留痕
                        └────────────────────────────────────────────┘
                                            │
             ┌──────────────────────────────┼──────────────────────────┐
             ▼                              ▼                          ▼
      Redis(Streams/限流/缓存)        PostgreSQL(租户/key/账单/任务)   Prometheus 指标
      异步任务: XADD→XREADGROUP→XACK   Alembic 迁移                    TTFT/token/分租户
      死信: XAUTOCLAIM→DLQ            幂等键表                        ← APISIX AI 可观测
      ← Dify Celery 骨架思路,自研可见性(ADR-003)
```

## 请求生命周期(写代码前先钉死)

1. **middleware**:生成 request_id(structlog 绑定),计时开始。
2. **auth**:`Authorization: Bearer ag-xxx` → SHA-256 哈希 → Redis 缓存查 key 元数据 → miss 则查 PG 并回填。**请求路径不写 DB**。
3. **ratelimit**:Lua 令牌桶原子扣减(键:`rl:{key_id}:{model}`),拒绝则 429 + Retry-After。
4. **quota**:余额预检(Redis 计数器,DB 是账本);不足则 402。
5. **router**:按 model 选渠道组 → 冷却检查 → 选延迟优先的健康渠道。
6. **upstream**:httpx 流式转发;SSE 逐 chunk 透传,**记录 TTFT**;流中断/上游错误按状态机决定重试或 fallback(已发 chunk 则不 fallback,如实转发错误——LiteLLM 同款 caveat)。
7. **记账(异步)**:后台任务写 UsageRecord(PG)+ 更新 Redis 计数;请求路径返回后执行,失败进重试队列。

## 数据模型(PG,通用类型保证 SQLite 可跑测试)

- `tenants(id, name, created_at)` — 租户
- `virtual_keys(id, tenant_id, key_hash, name, model_whitelist JSON, rpm_limit, tpm_limit, budget_tokens, is_active, created_at)` — virtual key
- `usage_records(id, key_id, tenant_id, model, prompt_tokens, completion_tokens, cost, latency_ms, ttft_ms, status, request_id, created_at)` — 账本(只追加)
- `channels(id, name, base_url, api_key_env, models JSON, weight, is_active)` — 上游渠道
- `task_records(idempotency_key UNIQUE, task_id, type, payload JSON, status, result JSON, attempts, created_at, updated_at)` — 异步任务幂等账本

## 关键取舍(详见 ADR)

| 决策 | ADR | 一句话 |
|---|---|---|
| Python 而非 Go/Java | ADR-001 | IO 密集场景,LiteLLM 存在性证明;承认单机吞吐边界并给出下沉触发条件 |
| SQLite 测试 + PG 生产,同 ORM | ADR-002 | 本机无 Docker 的诚实解;方言中立 + compose 交付 |
| Redis Streams 而非 Celery | ADR-003 | at-least-once 要自己实现才讲得透;黑盒框架不给面试留证据 |

## 与 workspace 生态的关系(M4)

- AskBI `/api/ask`、PolicyCompass `/chat` 注册为渠道 → 自家网关管理自家 Agent:统一 key、限流、计费、监控。
- 不改两项目一行代码(评测基线冻结),只在其外层加渠道配置。
