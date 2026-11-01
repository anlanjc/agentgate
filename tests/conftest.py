"""测试装配:mock 上游 + 网关 app,全程进程内 ASGI(超时/拒连/重置用例除外,走真 TCP)。

双模式:
- gateway:M0 纯代理语义(auth_enabled=False,适配边缘已鉴权的内网部署);
- auth_gateway:M1 完整链路(鉴权/限流/配额),预置租户与高限额 key。

Redis:优先真 Redis(127.0.0.1:6379/1,便携 5.0.14),不可用降级 fakeredis;
flavor 记录在 client._flavor,EVIDENCE 分列口径。

DB 方言(ADR-002):默认 SQLite(aiosqlite);设 AGENTGATE_TEST_PG=1 时用 pgserver 内嵌
PostgreSQL 16 全量复跑 —— 同一套测试,双口径分列(每种方言各自的表隔离策略见下)。
"""

import os

import httpx
import pytest

from app.api.main import create_app
from app.auth.keys import generate_key
from app.config import Settings
from app.db.models import Tenant, VirtualKey
from app.db.session import init_db
from app.upstream.client import UpstreamClient
from tests.mock_upstream import build_mock_upstream

TENANT_ID = "t-test"
KEY_ID = "k-test"

_PG_TABLES = ("usage_records", "task_records", "virtual_keys", "channels", "tenants")


@pytest.fixture(scope="session")
def pg_url() -> str | None:
    """AGENTGATE_TEST_PG=1 时启动内嵌 PostgreSQL 16(pgserver),返回 SQLAlchemy URL。"""
    if not os.environ.get("AGENTGATE_TEST_PG"):
        yield None
        return
    import pgserver

    db = pgserver.get_server(os.path.join(os.path.dirname(__file__), "..", ".pgdata"))
    uri = db.get_uri()  # postgresql://postgres:@host:port/postgres
    yield uri.replace("postgresql://", "postgresql+asyncpg://", 1)


@pytest.fixture(autouse=True)
async def _pg_clean(pg_url):
    """PG 口径:测试间清表(SQLite 靠 tmp_path 文件隔离,PG 共库需显式清理)。"""
    yield
    if pg_url is None:
        return
    from sqlalchemy import text

    from app.db.session import build_engine

    engine = build_engine(pg_url)
    try:
        async with engine.begin() as conn:
            for t in _PG_TABLES:
                await conn.execute(text(f"TRUNCATE TABLE {t} CASCADE"))
    finally:
        await engine.dispose()


def _base_settings(tmp_path, pg_url: str | None = None, **overrides) -> Settings:
    defaults: dict = {
        "upstream_name": "mock",
        "upstream_base_url": "http://mock/v1",
        "upstream_api_key": "sk-test",
        "upstream_read_timeout": 1.0,
        "models": ["mock-model"],
        "db_url": pg_url or f"sqlite+aiosqlite:///{tmp_path}/test.db",
        "redis_url": "redis://127.0.0.1:6379/1",
        "auth_enabled": False,
    }
    defaults.update(overrides)
    return Settings(**defaults)


@pytest.fixture
def settings(tmp_path, pg_url) -> Settings:
    return _base_settings(tmp_path, pg_url)


@pytest.fixture
def auth_settings(tmp_path, pg_url) -> Settings:
    return _base_settings(tmp_path, pg_url, auth_enabled=True)


@pytest.fixture
async def redis_client(settings):
    import redis.asyncio as aredis

    try:
        r = aredis.from_url(
            settings.redis_url, decode_responses=True, socket_connect_timeout=1.0, protocol=2
        )  # 便携 Redis 5.0 仅支持 RESP2(protocol=2 对 redis:7 同样兼容)
        await r.ping()
        r._flavor = "real"
    except Exception:
        from fakeredis import aioredis as fakeredis_aio

        r = fakeredis_aio.FakeRedis(decode_responses=True)
        r._flavor = "fakeredis"
    await r.flushdb()
    yield r
    try:
        await r.flushdb()
        await r.aclose()
    except Exception:  # noqa: S110 测试清理,失败无需处理(下个 fixture 会重建)
        pass


@pytest.fixture
async def real_redis(redis_client):
    """真 Redis 口径的集成测试;不可用时诚实 skip(fakeredis 兜底口径单独标注)。"""
    if getattr(redis_client, "_flavor", "") != "real":
        pytest.skip("real Redis unavailable (fakeredis fallback); 口径分列见 EVIDENCE")
    return redis_client


def _build_upstream(settings, mock_app) -> UpstreamClient:
    return UpstreamClient(
        settings.upstream_base_url,
        settings.upstream_api_key,
        read_timeout=settings.upstream_read_timeout,
        client=httpx.AsyncClient(
            transport=httpx.ASGITransport(app=mock_app),
            base_url=settings.upstream_base_url,
            timeout=httpx.Timeout(
                connect=5.0, read=settings.upstream_read_timeout, write=30.0, pool=5.0
            ),  # 注入客户端必须显式带超时,否则 slow 用例不触发读超时
        ),
    )


@pytest.fixture
async def gateway(settings, redis_client):
    """(client, app, mock_app):纯代理模式。"""
    mock_app = build_mock_upstream()
    upstream = _build_upstream(settings, mock_app)
    app = create_app(settings=settings, upstream=upstream, redis_client=redis_client)
    await init_db(app.state.engine)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gateway") as client:
        yield client, app, mock_app
    await upstream.aclose()
    await app.state.writer.stop()


@pytest.fixture
async def auth_gateway(auth_settings, redis_client):
    """(client, app, mock_app, raw_key):完整链路模式,预置租户+高限额 key。"""
    mock_app = build_mock_upstream()
    upstream = _build_upstream(auth_settings, mock_app)
    app = create_app(settings=auth_settings, upstream=upstream, redis_client=redis_client)
    await init_db(app.state.engine)

    raw, key_hash, prefix = generate_key()
    async with app.state.session_factory() as db:
        db.add(Tenant(id=TENANT_ID, name="it-tenant"))
        db.add(
            VirtualKey(
                id=KEY_ID, tenant_id=TENANT_ID, key_hash=key_hash, key_prefix=prefix,
                name="default", rpm_limit=10_000, tpm_limit=10_000_000, budget_tokens=None,
            )
        )
        await db.commit()

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gateway") as client:
        yield client, app, mock_app, raw
    await upstream.aclose()
    await app.state.writer.stop()


def auth_headers(raw_key: str) -> dict:
    return {"Authorization": f"Bearer {raw_key}"}
