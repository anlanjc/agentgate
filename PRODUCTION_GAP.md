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

| # | 项 | 现状 | 触发条件 | 工作量 |
|---|---|---|---|---|
| B1 | 管理台鉴权/SSO | 页面内网用,API 有 admin_token | 多人使用 | 1-2d |
| B2 | 账单导出/对账文件 | admin usage API 有,导出无 | 财务需求 | 0.5d |
| B3 | 滑动窗口 TPM | 固定 60s 窗口(交界 ~2×,EVIDENCE §⑥) | 计费敏感客户 | 0.5d |
| B4 | 语义缓存 | 精确匹配缓存 | 高重复问答场景 | 1-2d(向量+阈值) |
| B5 | 语义缓存失效策略 | TTL 惰性 | 上游答案可变性高 | 1d |
| B6 | PG 账单表按月分区 | 单表+created_at 索引 | 账本 > 千万行 | 0.5d(PG 专属迁移分支) |
| B7 | 渠道协议适配器 | shim 外挂(~60 行/应用) | 接入非 OpenAI 协议应用多 | 1d/协议(Anthropic 等) |

## C. 锦上添花

| # | 项 | 说明 |
|---|---|---|
| C1 | Grafana 面板 JSON | 指标已暴露,面板手工拖一次导出即可 |
| C2 | MCP server HTTP transport | 目前 stdio(单机);远程用 SSE transport |
| C3 | key 用量告警(预算 80% 预警) | webhook 通道已具备,加规则即可 |
| C4 | AskBI 实接 | PC 已实接;AskBI 同 shim 模式,任务模型映射稍复杂(异步任务表) |

## 结论

代码层无必做阻塞项——A1 装完 Docker、A2 配一个 env,即满足"生产可部署"。
其余按用户量触发,与 askbi/PC 的 PRODUCTION_GAP 同构。
