# ROADMAP · agentgate（2026-11-03 规划，基于 ds-update hardening 轮之后）

> 规划人：ZCode（AG-2 会话）。本文档只规划、不执行；执行时按黑板纪律登记根 PROGRESS.md（[AG-2] 前缀）。
> 现状基线（2026-11-03 现场复核）：ds-update 轮 27 文件未提交（+967/-337）；**SQLite 62 passed + ruff 全绿（实跑复核）**；
> PG 口径未复跑；文档数字漂移（README 47 / PROJECT_BOOK 41 / PITCH 55 / EVIDENCE 57，实际 62）。
> 核心判断：**当前最大风险不是缺功能，而是"大未验证变更集 + 全面文档漂移"。先锁存，再加码。**

## 执行顺序总览

| 阶段 | 主题 | 谁 | 预估 | 前置 |
|---|---|---|---|---|
| P0-A | 变更集验证与分批提交 | ZCode | 半天 | 主人表态接受 ds-update 改动 |
| P0-B | 口径统一（文档/Grafana/EVIDENCE） | ZCode | 半天 | P0-A |
| P0-C | 两个证据缺口（outbox 注入测试 / Redis 挂行为） | ZCode | 半天 | P0-A |
| P1 | 数字重测（压测/演练复跑） | ZCode | 半天 | P0-A/B |
| P2 | hardening 三连（密钥加密/审计日志/幂等键） | ZCode | 1-2 天 | P0 全部 |
| P3 | 部署与生态 | 混合 | 视主人进度 | Docker/push |
| P4 | 按 JD 触发的锦上添花 | ZCode | 按需 | P2 后 |

---

## P0-A 变更集验证与分批提交（我做，最优先）

**为什么最先**：27 个文件的未提交 diff 是当前唯一重大资产风险——任何误操作（`git checkout .`、`git clean`）都会丢掉整轮工作。ds-update.md 自己也警告了这一点。

1. **PG 双口径复跑**（新迁移只在 SQLite 测过，`batch_alter_table` 的 key_id 可空/唯一约束/published_at 索引需真 PG 验证）：
   - `AGENTGATE_TEST_PG=1 .venv/Scripts/python -m pytest tests -q` → 修暴露的方言问题（EVIDENCE 曾记录过 PG 强制外键/loop 绑定类差异，预期还有 1-2 个）
   - `alembic upgrade head` 真 PG 执行（pgserver 内嵌库即可，`scripts/verify_pg_migration.py` 已有骨架）→ 验证 5 表 + 新列/新索引齐全 + 幂等可重跑
   - **在此完成前，任何文档不得写"62×2 双口径"**（只可写 62 SQLite + PG 复跑中）
2. **剩余 diff 精读**：已审 quota/service.py、cache/response.py、queue/streams.py、db 迁移、v1_chat.py（质量过关）；还需过 admin.py、mcp_server.py、failover.py、api/main.py、config.py、upstream/sse.py、web/index.html、Dockerfile/compose/ci.yml，重点找：安全改动是否引入新依赖行为、prod fail-closed 逻辑边界、HashEmbedder 告警位置
3. **分批提交**（每笔提交前全量测试绿；建议 5 笔，保持"一笔=一个可讲的故事"）：
   - ① 流式指标/账本时机修复 + 499/502 语义 + include_usage 注入（tests/test_m1_quota）
   - ② 缓存正确性：显式 temperature 门控 + user 入键 + 语义缓存 context_digest（tests/test_m5_*）
   - ③ 队列 hardening：outbox 水位 + 原子抢占 + XACK+XDEL + visibility timeout + 新迁移（tests/test_m2_streams）
   - ④ 准入原子 Lua 合并 + 渠道 model 过滤 + 超时持久化 + admin 安全（常量时间比较/409/时间窗）
   - ⑤ 部署卫生：Dockerfile 非 root/healthcheck、compose token 必填、CI 加 PG 迁移+Docker build、requirements 补齐、删 writer.py/pc_child.log
   - ds-update.md 随最后一笔入库（诚实的审阅记录，本身是工程素养的证据）
4. 黑板登记：根 PROGRESS.md 加 [AG-2] 行

## P0-B 口径统一（我做，"背稿感"的最大消除项）

5. **四份文档对齐到同一口径**（62×2、双窗口 TPM、Streams 记账、outbox、tenant 级幂等键、admin token 常量时间比较）：
   - README：硬点表补"账本 (tenant_id, request_id) UNIQUE"与"outbox 补投"两行；测试数；结构图 queue 描述
   - PROJECT_BOOK：删"单写者批量落库"“TPM 固定窗口在 backlog"等已过时表述，10 问速查表同步 ds-update §3.3 的新答案
   - PITCH/EVIDENCE：数字统一 62；EVIDENCE 新增"hardening 批次（2026-11-03）"一节，逐条记录 §2.1 的 15 项修复与验证命令——**这节就是"故障注入/审计驱动开发"的活证据**
6. **Grafana 面板重写**（deploy/grafana/dashboards/agentgate.json）：删 `agentgate_usage_dropped_total`，加 `agentgate_tasks_dead_total`、`agentgate_usage_queue_depth`、`agentgate_channel_cooling`；修乱码标题；只保留 metrics.py 里真实存在的指标

## P0-C 两个证据缺口（我做）

7. **outbox 故障注入测试**（代码写了不能只说写了）：
   - monkeypatch `xadd` 抛异常 → enqueue 成功但 published_at 为空 → Redis 恢复 → 断言 sweeper 在 grace 期后补投 → 消费成功 → 同 task_id 不重复执行
   - 加上"published_at 回写失败 → 重复投递 → 消费端幂等吞掉"的反向用例
8. **Redis 挂掉时请求路径行为定义**（当前 `_admit`/`_authenticate` 的 Redis 异常会变成未处理 500）：
   - 决策：**fail-closed**（503 + 明确错误码 `admission_unavailable` + Retry-After）——配额场景宁可拒不可漏
   - 统一错误信封走 `_gateway_error`；测试：Redis 停掉时 /v1/chat/completions 返回 503 信封而非 500
   - 这条做完，"Redis 挂了怎么办"的追问从"待补项"变成"有测试的决策"（ds-update §3.3 的答案同步改写）

## P1 数字重测（我做，旧数字不能当新代码证据）

9. 重跑 `scripts/load_test.py`（附加开销口径）+ `scripts/load_workers.py`（扩展曲线）→ EVIDENCE 更新 3.70ms/4.04x 为新值；若数字变差，如实写并归因（预期影响极小：改动集中在后台路径）
10. 复跑 `scripts/demo_ecosystem.py` 演练（kill -9 切换在新代码下回归）+ `scripts/smoke_real.py` DashScope 冒烟（需主人确认 key 配额，见【主人】#4）

## P2 hardening 三连 + 补齐（我做，拷打防守升级）

按"最容易被戳"排序：

11. **渠道密钥加密**：`channels.api_key` Fernet 加密入库（主密钥 env），admin API 响应与日志永不出明文——"密钥明文"是安全拷打第一戳
12. **admin 审计日志**：`audit_log` 表（actor/action/target/created_at），admin API 全动作落库 + 查询端点——"谁吊销了这把 key"可回答
13. **客户端 Idempotency-Key**：请求头透传 → 网关用它生成 request_id + 短 TTL 响应去重缓存——"客户端重试会不会重复计费"从"账本不重"升级为"不重调上游"
14. **tenant 级预算**：租户预算计数器（key 汇总约束），堵"多签几把 key 绕预算"
15. 小项打包：限流响应头（X-RateLimit-Remaining/Reset）· 每 key 并发上限 · 缓存单飞锁（SETNX 防同 prompt 并发击穿）
16. 每项都按项目纪律配测试 + EVIDENCE 登记 + PRODUCTION_GAP 对应行销账

## P3 部署与生态（大半依赖主人，我做好我能准备的）

17. 【我】compose 加 Jaeger（OTLP receiver）——OTel span 有地方看，"追踪建了没看过"不再是半成品；加 SLO 告警规则示例（错误率/P99/队列积压/死信）
18. 【我】AskBI 实接准备：shim 已有模板（scripts/askbi_shim.py），跨项目改动需主人点头（原红线：不碰其他项目目录）——待确认后执行
19. 【主人】装 Docker Desktop → 我准备一键验证脚本（compose up → 冒烟 → PG/Redis7 口径数字回填 EVIDENCE §④）
20. 【主人】GitHub push（建议顺序沿用记忆：agenteval → askbi → policy → agentgate）→ CI 首跑验证（含 PG service 容器 + Docker build job）→ 仓库链接进简历
21. 【主人】DashScope 配额确认（qwen-turbo 上次 429 账号配额耗尽）→ 真冒烟复验

## P4 按 JD 触发（我做，不强求全做）

22. 成本金额表（模型单价 × token → 金额账单）+ 账单导出——"怎么计费"追问的完整答案
23. 语义缓存升级 pgvector/RediSearch KNN（当前 ZSET 暴力余弦 200 条上限，边界已写死在代码注释）
24. W3C traceparent 完整 extract/inject（现在只挂 span 属性）
25. K8s/Helm 清单、HPA/PDB——投基础设施向 JD 才做
26. 管理台 SSO/日志轮换/Loki——校招项目边际价值低，默认不做

---

## 需要主人做的事（汇总，我暂不执行）

| # | 事项 | 卡住什么 | 建议 |
|---|---|---|---|
| 1 | **表态：接受还是部分回滚 ds-update 的 27 文件改动** | P0-A 无法开始 | 我已抽查核心 diff + 复跑 62 绿 + ruff 绿，建议整轮接受；要回滚请逐文件来，勿 `git clean -fd` |
| 2 | 装 Docker Desktop | compose 实跑 / PG16+Redis7 口径 / Docker build 验证 | 装好后我给一键验证脚本 |
| 3 | GitHub 建仓 + push（四仓序：agenteval→askbi→policy→agentgate） | CI 真跑 / Docker build job / 简历可链 | push 后把 CI 徽章贴 README |
| 4 | DashScope key 配额确认 | P1 真冒烟复验 | qwen-turbo 上次配额耗尽，确认或换账号 |
| 5 | 决定是否开放 AskBI 侧改动（跨项目红线） | P3 AskBI 实接 | shim 模板已就绪，点头即做 |
| 6 | 背稿：ds-update §3 三段话术 + 演示剧本 §3.4 | 面试表现 | 演示前必须先跑完 P0-A/B（否则现场数字对不上） |

## 一句话总结

> 这轮 hardening 把项目的"可拷打面积"压小了一圈，但现在全部价值悬在一个未提交未验证的变更集上。**先用半天把它验证、审完、分批锁进 git，再用半天把文档和面板对齐到 62×2 口径**——这两步做完，项目才真正回到"每条主张都有证据"的状态；之后 P1 重测数字、P2 补安全三连，都是在这个地基上加楼层。
