FROM python:3.12-slim

WORKDIR /srv/agentgate

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY alembic.ini .
COPY migrations ./migrations

EXPOSE 8100

# 生产:PostgreSQL + Redis 由 compose 注入(ADR-002);启动前跑迁移
CMD ["sh", "-c", "alembic upgrade head && uvicorn app.api.main:app --host 0.0.0.0 --port 8100 --workers 2"]
