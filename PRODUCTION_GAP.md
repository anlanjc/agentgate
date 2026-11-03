# PRODUCTION_GAP · agentgate(上线前差距清单)

> 纪律同 askbi/PC 版:每项写"现状证据 + 触发条件 + 工作量"。红线:【部署后才能说】【试点后才能说】的事实一个字不提。

## A. 必做(生产部署前置)

| # | 项 | 现状 | 触发条件 | 工作量 |
|---|---|---|---|---|
| A1 | Docker 就绪跑 compose | compose 交付,本机无 Docker(ADR-002) | 装 Docker Desktop | 0.5h(compose up + 冒烟) |
| A2 | admin_token 强制 | 默认空=本机开发不设防 | 公网部署前 | 已实现,配 env 即可 |
| A3 | HTTPS/反代 | uvicorn 裸 HTTP | 公网 | 1h(nginx/Caddy + 证书) |
| A4 | 日志落盘与轮换 | structlog 到 stderr | 常驻 | 1h(logrotate/loki) |
| A5 | Redis 持久化确认 | compose 已 appendonly yes;本机便携实例 --save "" | 生产 | 0(已配置) |

## B. 应做(真实用户前)

> 2026-11-03 AG-2 P1-P4 批次销账:B2/B3/B4 已完成(标 ✅);管理台 token 输入+审计日志落地(B1 部分),
> 密钥加密/审计/Idempotency-Key/租户预算/并发上限/单飞锁见 EVIDENCE §⑨。

| # | 项 | 现状 | 触发条件 | 工作量 |
|---|---|---|---|---|
| B1 | 管理台鉴权/SSO | ✅部分:页面 token 输入(sessionStorage)+审计日志+常量时间比较;SSO 未做 | 多人使用 | SSO 1-2d |
| B2 | 账单导出/对账文件 | ✅ /admin/tenants/{id}/billing(JSON 汇总+CSV 明细,时间窗) | — | 0 |
| B3 | 滑动窗口 TPM | ✅ 双窗口加权滑动近似(O(1) 两键轮转);精确滑动日志仍可再进一步 | — | 精确版 0.5d |
| B4 | 语义缓存 | ✅ 语义缓存双后端:Redis 暴力余弦(默认,≤200 条)+ pgvector KNN/HNSW(PG 口径实测) | — | RediSearch 版按需 |
| B5 | 语义缓存失效策略 | TTL 惰性(Redis)/查询侧 created_at 过滤(pgvector) | 上游答案可变性高 | 1d |
| B6 | PG 账单表按月分区 | 单表+created_at 索引 | 账本 > 千万行 | 0.5d(PG 专属迁移分支) |
| B7 | 渠道协议适配器 | shim 外挂(~60 行/应用) | 接入非 OpenAI 协议应用多 | 1d/协议(Anthropic 等) |

## C. 锦上添花

| # | 项 | 说明 |
|---|---|---|
| C1 | Grafana 面板 JSON | ✅ provisioning 自动加载(TTFT 分位/缓存命中/队列积压/渠道冷却/死信) |
| C2 | MCP server HTTP transport | 目前 stdio(单机);远程用 SSE transport |
| C3 | key 用量告警(预算 80% 预警) | ✅部分:SLO 告警规则 6 条已交付(deploy/alerts.yml,Prometheus rule_files);预算百分比预警未做 |
| C4 | AskBI 实接 | PC 已实接;AskBI 同 shim 模板就绪,**等主人开放跨项目授权** |
| C5 | K8s 实跑验证 | ✅清单已交付(deploy/k8s:Deployment/Service/HPA);未在真实集群实跑,装 k8s 后核对 |

## 结论

代码层无必做阻塞项——A1 装完 Docker、A2 配一个 env,即满足"生产可部署"。
其余按用户量触发,与 askbi/PC 的 PRODUCTION_GAP 同构。
