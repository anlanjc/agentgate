# Docker Compose 部署实跑手册（2026-11-11，主人侧执行清单）

> 目的：GW-7 修复（compose 强制密钥）后 compose 拓扑从未真跑过——按本手册跑通一次，
> "部署就绪"就从主张变成事实。预计 30-60 分钟（含 Docker Desktop 安装）。

## 0. 前置（一次性）

1. 安装 Docker Desktop（wsl2 后端默认即可）：https://www.docker.com/products/docker-desktop/
2. 装后验证：`docker --version` 与 `docker compose version` 都有输出

## 1. 准备两个必填密钥（compose 用 `:?` 强制，缺了会拒绝启动——GW-7 修复的预期行为）

```bash
cd D:/1/1/1Tasks/zcode/shixiproj/agentgate
# Fernet 主密钥（渠道密钥加密用）：
.venv/Scripts/python.exe -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# Admin Token（管理面鉴权）：自造一串强随机即可，例如
.venv/Scripts/python.exe -c "import secrets; print(secrets.token_urlsafe(32))"
```

把两个值放进 `deploy/.env`（compose 会读取同目录 .env）：

```
AGENTGATE_CHANNEL_ENC_KEY=<Fernet 输出>
AGENTGATE_ADMIN_TOKEN=<token 输出>
```

## 2. 启动与验证（按序，每步有预期输出）

```bash
cd deploy
docker compose up -d          # 拉起 gateway + PostgreSQL 16 + Redis 7(AOF) + Prometheus + Grafana
docker compose ps             # 五个服务 Up（gateway 8100 / postgres 5432 / redis 6379 / prometheus / grafana）
```

验证清单（逐条打勾）：
- [ ] `curl http://127.0.0.1:8100/ready` → `{"ready": true, "checks": {"redis": "ok", "db": "ok", ...}}`（PG/Redis 双检过）
- [ ] `curl http://127.0.0.1:8100/api/health` → 200
- [ ] 管理台：浏览器开 `http://127.0.0.1:8100/admin/web`，输入 Admin Token 登录
- [ ] Playground：`http://127.0.0.1:8100/playground`（需在管理台签发一把 virtual key）
- [ ] `docker compose logs gateway | grep -i alembic` → 迁移在 PG 上执行成功（或按 DEPLOY 手动执行迁移）
- [ ] Grafana `http://127.0.0.1:3000`（admin/admin）→ agentgate 面板有数据

## 3. 三个"应该失败"的负验证（证明安全闸在工作）

- [ ] 删掉 .env 里的 AGENTGATE_CHANNEL_ENC_KEY 再 `docker compose up` → **启动即拒绝**并提示生成密钥（GW-7 fail-fast）
- [ ] 无 key 直接调知识库写接口（policy-compass 语义，agentgate 对应：无 Admin Token 调管理端点）→ 401/403
- [ ] 签发 key 时把渠道 read_timeout 设得比幂等锁 TTL 大 → 400 read_timeout_exceeds_idem_ttl（GW-14 硬约束）

## 4. 常见坑

- 端口占用：8100/3000 被占时改 compose 端口映射，别改容器内部端口
- Windows 防火墙首跑弹窗放行 Docker
- Postgres 数据卷首次启动会自动建库建表；重跑想清库：`docker compose down -v`

## 5. 跑通后回到这里登记

跑通/失败都请把输出贴给会话——跑通则黑板登记"部署就绪 ✅（compose 实跑）"，
失败则当日排障（GW 系列修复的实弹验证机会）。
