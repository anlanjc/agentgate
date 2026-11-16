FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    # GATE-6(评审 4.0):--workers 2 共享监听 socket,进程内默认 registry 使
    # /metrics 每次抓取只落到其中一个 worker(轮流取半份+虚假 counter reset),
    # 告警系统性漏报。prometheus_client 多进程模式:各 worker 指标落 MMAP 文件,
    # /metrics 经 MultiProcessCollector 合并(app/api/main.py)。目录置 /tmp
    # (1777,非 root 可写);优雅停运由 lifespan mark_process_dead 清本进程文件,
    # SIGKILL 残留由 CMD 启动前 rm -rf 兜底。
    PROMETHEUS_MULTIPROC_DIR=/tmp/agentgate_metrics

WORKDIR /srv/agentgate

# 非 root 运行:网关是公网入口,不把进程权限留在 root。
RUN useradd --create-home --shell /usr/sbin/nologin appuser

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=appuser:appuser app ./app
COPY --chown=appuser:appuser alembic.ini .
COPY --chown=appuser:appuser migrations ./migrations

USER appuser

EXPOSE 8100

HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8100/health', timeout=2)"

# 生产:PostgreSQL + Redis 由 compose 注入(ADR-002);启动前跑迁移;
# multiproc 指标目录先清残留再建(GATE-6:死进程 MMAP 文件会被合并进抓取)
CMD ["sh", "-c", "rm -rf /tmp/agentgate_metrics && mkdir -p /tmp/agentgate_metrics && alembic upgrade head && uvicorn app.api.main:app --host 0.0.0.0 --port 8100 --workers 2"]
