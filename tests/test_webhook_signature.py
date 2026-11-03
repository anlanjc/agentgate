"""P4 安全补强:webhook HMAC 签名——签发/验签协议/无密钥不签名。"""

import asyncio
import hashlib
import hmac
import socket

import httpx
import uvicorn

from tests.conftest import auth_headers
from tests.mock_upstream import build_mock_upstream


def _body(**extra) -> dict:
    body = {"model": "mock-model", "messages": [{"role": "user", "content": "hi"}]}
    body.update(extra)
    return body


async def test_webhook_signature_e2e(auth_settings, redis_client, tmp_path):
    """带 secret 的租户:接收端收到的签名可用同一密钥验过;篡改 body 验不过。"""
    received: list[dict] = []

    from fastapi import FastAPI, Request

    receiver = FastAPI()

    @receiver.post("/receive")
    async def receive(request: Request):
        raw = await request.body()
        received.append({
            "raw": raw,
            "ts": request.headers.get("X-Agentgate-Timestamp", ""),
            "sig": request.headers.get("X-Agentgate-Signature", ""),
        })
        return {"ok": True}

    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    server = uvicorn.Server(uvicorn.Config(receiver, host="127.0.0.1", port=port, log_level="error"))
    server_task = asyncio.get_running_loop().create_task(server.serve())
    for _ in range(50):
        if server.started:
            break
        await asyncio.sleep(0.05)

    try:
        from app.api.main import create_app
        from app.auth.keys import generate_key
        from app.db.models import Tenant, VirtualKey
        from app.db.session import init_db
        from app.upstream.client import UpstreamClient

        mock_app = build_mock_upstream()
        upstream = UpstreamClient(
            auth_settings.upstream_base_url,
            auth_settings.upstream_api_key,
            client=httpx.AsyncClient(
                transport=httpx.ASGITransport(app=mock_app),
                base_url=auth_settings.upstream_base_url,
            ),
        )
        app = create_app(settings=auth_settings, upstream=upstream, redis_client=redis_client)
        await init_db(app.state.engine)
        raw_key, key_hash, prefix = generate_key()
        async with app.state.session_factory() as db:
            db.add(Tenant(id="t-sig", name="sig", webhook_url=f"http://127.0.0.1:{port}/receive",
                          webhook_secret="whsec-test-secret-0123456789"))
            db.add(VirtualKey(id="k-sig", tenant_id="t-sig", key_hash=key_hash, key_prefix=prefix,
                              rpm_limit=10_000, tpm_limit=10_000_000))
            await db.commit()

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://gw"
        ) as client:
            r = await client.post(
                "/v1/chat/completions", json=_body(), headers={"Authorization": f"Bearer {raw_key}"}
            )
            assert r.status_code == 200
        await app.state.task_queue.flush()
        assert received, "webhook 未投递"

        # 验签:HMAC(secret, "{ts}.{raw}") == sig
        item = received[0]
        expected = hmac.new(
            b"whsec-test-secret-0123456789",
            f"{item['ts']}.".encode() + item["raw"], hashlib.sha256
        ).hexdigest()
        assert item["sig"].startswith("sha256=")
        assert hmac.compare_digest(item["sig"][len("sha256="):], expected)

        # 篡改 body(或密钥不符)→ 验不过
        tampered = hmac.new(b"wrong-secret-xxxxxxxxxxxxxxxx", f"{item['ts']}.".encode() + item["raw"], hashlib.sha256).hexdigest()
        assert not hmac.compare_digest(item["sig"][len("sha256="):], tampered)

        await upstream.aclose()
        await app.state.engine.dispose()
    finally:
        server.should_exit = True
        await server_task


async def test_unsigned_tenant_sends_no_signature_headers(auth_gateway):
    """未配 secret:投递不带签名头(协议向后兼容,接收端按无签名处理)。"""
    client, app, mock, raw = auth_gateway
    import uvicorn as _uv
    from fastapi import FastAPI, Request

    got: dict = {}
    receiver = FastAPI()

    @receiver.post("/receive")
    async def receive(request: Request):
        got["sig"] = request.headers.get("X-Agentgate-Signature")
        got["ts"] = request.headers.get("X-Agentgate-Timestamp")
        return {"ok": True}

    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    server = _uv.Server(_uv.Config(receiver, host="127.0.0.1", port=port, log_level="error"))
    server_task = asyncio.get_running_loop().create_task(server.serve())
    for _ in range(50):
        if server.started:
            break
        await asyncio.sleep(0.05)
    try:
        from sqlalchemy import update

        from app.db.models import Tenant

        async with app.state.session_factory() as db:
            await db.execute(
                update(Tenant).where(Tenant.id == "t-test").values(webhook_url=f"http://127.0.0.1:{port}/receive")
            )
            await db.commit()
        r = await client.post("/v1/chat/completions", json=_body(), headers=auth_headers(raw))
        assert r.status_code == 200
        await app.state.task_queue.flush()
        assert got["sig"] is None and got["ts"] is None
        await app.state.engine.dispose()
    finally:
        server.should_exit = True
        await server_task
