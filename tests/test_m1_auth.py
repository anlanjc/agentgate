"""M1 验收:virtual key 鉴权(缓存优先/白名单/吊销)与 admin API。"""

import httpx
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.main import create_app
from app.auth.keys import generate_key
from app.db.models import VirtualKey
from app.db.session import init_db
from tests.conftest import KEY_ID, TENANT_ID, _build_upstream, auth_headers
from tests.mock_upstream import build_mock_upstream


def _body(**extra) -> dict:
    body = {"model": "mock-model", "messages": [{"role": "user", "content": "你好"}]}
    body.update(extra)
    return body


async def test_missing_key_401(auth_gateway):
    client, app, mock, raw = auth_gateway
    resp = await client.post("/v1/chat/completions", json=_body())
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "missing_api_key"


async def test_unknown_key_401(auth_gateway):
    client, app, mock, raw = auth_gateway
    resp = await client.post(
        "/v1/chat/completions", json=_body(), headers={"Authorization": "Bearer ag-wrong"}
    )
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "key_not_found"


async def test_valid_key_200_and_ledger(auth_gateway):
    client, app, mock, raw = auth_gateway
    resp = await client.post("/v1/chat/completions", json=_body(), headers=auth_headers(raw))
    assert resp.status_code == 200
    await app.state.task_queue.flush()
    usage = (await client.get(f"/admin/tenants/{TENANT_ID}/usage")).json()
    assert usage["requests"] == 1
    assert usage["total_tokens"] == 18
    assert usage["by_model"]["mock-model"]["requests"] == 1


async def test_model_whitelist_403(auth_gateway):
    client, app, mock, raw = auth_gateway
    created = (
        await client.post(
            f"/admin/tenants/{TENANT_ID}/keys",
            json={"name": "wl", "model_whitelist": ["other-model"]},
        )
    ).json()
    resp = await client.post(
        "/v1/chat/completions", json=_body(), headers=auth_headers(created["key"])
    )
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "model_not_allowed"


async def test_cache_first_serves_after_db_row_gone(auth_gateway):
    """缓存优先:首次请求回填缓存后,删掉 DB 行仍能鉴权成功(60s TTL 内)。"""
    client, app, mock, raw = auth_gateway
    r1 = await client.post("/v1/chat/completions", json=_body(), headers=auth_headers(raw))
    assert r1.status_code == 200

    async with app.state.session_factory() as db:
        # PG 强制外键:先清该 key 的账本行,再删 key(SQLite 口径两步均幂等)
        from app.db.models import UsageRecord

        await db.execute(delete(UsageRecord).where(UsageRecord.key_id == KEY_ID))
        await db.execute(delete(VirtualKey).where(VirtualKey.id == KEY_ID))
        await db.commit()

    r2 = await client.post("/v1/chat/completions", json=_body(), headers=auth_headers(raw))
    assert r2.status_code == 200  # 缓存命中,不再回源


async def test_deactivate_revokes_immediately(auth_gateway):
    """吊销立即生效:先打满缓存,再吊销(主动失效缓存),下一发即 401。"""
    client, app, mock, raw = auth_gateway
    r1 = await client.post("/v1/chat/completions", json=_body(), headers=auth_headers(raw))
    assert r1.status_code == 200

    resp = await client.post(f"/admin/keys/{KEY_ID}/deactivate")
    assert resp.status_code == 200

    r2 = await client.post("/v1/chat/completions", json=_body(), headers=auth_headers(raw))
    assert r2.status_code == 401
    assert r2.json()["error"]["code"] == "key_inactive"


async def test_admin_token_protects(auth_settings, redis_client):
    auth_settings.admin_token = "secret-token"
    app = create_app(settings=auth_settings, redis_client=redis_client)
    await init_db(app.state.engine)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://gateway"
    ) as client:
        r1 = await client.post("/admin/tenants", json={"name": "x"})
        assert r1.status_code == 401
        r2 = await client.post(
            "/admin/tenants", json={"name": "x"}, headers={"X-Admin-Token": "secret-token"}
        )
        assert r2.status_code == 200
    await app.state.task_queue.stop()


async def test_auth_db_unavailable_503_envelope(auth_settings, redis_client, tmp_path):
    """PG 故障域混沌(2026-11-11 P1)回归:鉴权存储不可用 = 503 auth_unavailable 信封。

    三分语义第三腿:缓存命中 200 / key 判定无效 401 / 无法判定(存储断连)503。
    修复前 DB 断连异常穿透全链 → 裸 500(混沌 e2e 在真 uvicorn 上实锤)。
    """
    broken = auth_settings.model_copy(update={
        "db_url": f"sqlite+aiosqlite:///{tmp_path}/no_such_dir/auth.db"})
    mock_app = build_mock_upstream()
    upstream = _build_upstream(broken, mock_app)
    app = create_app(settings=broken, upstream=upstream, redis_client=redis_client)
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://gw"
        ) as client:
            raw, _, _ = generate_key()
            resp = await client.post(
                "/v1/chat/completions",
                json={"model": "mock-model", "messages": [{"role": "user", "content": "你好"}]},
                headers=auth_headers(raw),
            )
        assert resp.status_code == 503
        assert resp.json()["error"]["code"] == "auth_unavailable"
        assert resp.headers.get("retry-after") == "5"
    finally:
        await upstream.aclose()
        await app.state.task_queue.stop()
        await app.state.engine.dispose()


async def test_auth_cache_hit_survives_db_outage(auth_gateway, tmp_path):
    """PG 故障域混沌(2026-11-11 P2)回归:vk 缓存命中时 DB 断连不穿透鉴权。

    test_cache_first_serves_after_db_row_gone 覆盖"行被删"级;本用例把整个
    存储层换成连不上的引擎(故障级断连),验证缓存优先语义:命中即零 DB。
    """
    client, app, mock, raw = auth_gateway
    r1 = await client.post("/v1/chat/completions", json=_body(), headers=auth_headers(raw))
    assert r1.status_code == 200  # 回填缓存

    broken = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/no_such_dir/auth.db")
    old_engine, old_factory = app.state.engine, app.state.session_factory
    app.state.engine = broken
    app.state.session_factory = async_sessionmaker(broken, expire_on_commit=False)
    try:
        r2 = await client.post("/v1/chat/completions", json=_body(), headers=auth_headers(raw))
        assert r2.status_code == 200  # 缓存命中,零 DB
    finally:
        app.state.engine, app.state.session_factory = old_engine, old_factory
        await broken.dispose()
