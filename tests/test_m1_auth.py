"""M1 验收:virtual key 鉴权(缓存优先/白名单/吊销)与 admin API。"""

import httpx
from sqlalchemy import delete

from app.api.main import create_app
from app.db.models import VirtualKey
from app.db.session import init_db
from tests.conftest import KEY_ID, TENANT_ID, auth_headers


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
    await app.state.writer.flush()
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
    await app.state.writer.stop()
