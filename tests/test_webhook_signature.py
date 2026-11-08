"""P4 安全补强:webhook HMAC 签名——签发/验签协议/无密钥不签名。

GW-8(评审 2.0)回归:webhook_secret 加密落库 + 队列 payload 净化 + 投递时解密。
"""

import asyncio
import hashlib
import hmac
import json
import socket

import httpx
import uvicorn
from sqlalchemy import select, update

from tests.conftest import TENANT_ID, auth_headers
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


# ---------- GW-8(评审 2.0)回归:webhook_secret 加密落库 + 队列 payload 净化 ----------


async def test_webhook_secret_encrypted_at_rest_and_payload_clean(auth_gateway):
    """GW-8 回归:管理端写入的 webhook_secret 落库即密文(enc:v1:);
    webhook_callback 的任务 payload 只带 tenant_id —— 明文/密文都不进
    task_records.payload 与 Redis 流(修复前明文随 payload 落 Redis+AOF+账本)。"""
    from sqlalchemy import select

    from app.db.models import TaskRecord, Tenant
    from app.security import crypto

    client, app, mock, raw = auth_gateway
    secret = "whsec-gw8-plain-0123456789"

    # 模拟生产:主密钥就绪(在 create_app 之后注入,避免工厂重置)
    from app.security.crypto import init_cipher

    init_cipher(crypto.generate_key())
    try:
        # ① 创建租户即密文
        r = await client.post(
            "/admin/tenants",
            json={"name": "gw8-tenant", "webhook_url": "http://127.0.0.1:9/none",
                  "webhook_secret": secret},
        )
        assert r.status_code == 200, r.text
        tid = r.json()["id"]

        async with app.state.session_factory() as db:
            row = (await db.execute(select(Tenant).where(Tenant.id == tid))).scalar_one()
        assert row.webhook_secret != secret
        assert crypto.is_encrypted(row.webhook_secret)  # enc:v1: 前缀
        assert crypto.decrypt_secret(row.webhook_secret) == secret  # 可解回

        # ② 轮换也密文:给请求 key 所属租户(t-test)配上 webhook(rotate 端点);
        #    url 经直写 DB(_account 只对有 url 的租户入队)
        r_rot = await client.post(
            f"/admin/tenants/{TENANT_ID}/webhook-secret",
            json={"webhook_secret": secret},
        )
        assert r_rot.status_code == 200
        async with app.state.session_factory() as db:
            await db.execute(
                update(Tenant).where(Tenant.id == TENANT_ID).values(
                    webhook_url="http://127.0.0.1:9/none"
                )
            )
            await db.commit()
            trow = (await db.execute(select(Tenant).where(Tenant.id == TENANT_ID))).scalar_one()
        assert crypto.is_encrypted(trow.webhook_secret)

        # ③ 正常请求 → 200 → webhook 入队:payload 必须干净
        r2 = await client.post(
            "/v1/chat/completions",
            json={"model": "mock-model", "messages": [{"role": "user", "content": "hi"}]},
            headers=auth_headers(raw),
        )
        assert r2.status_code == 200
        await app.state.task_queue.flush()

        async with app.state.session_factory() as db:
            tasks = (
                await db.execute(select(TaskRecord).where(TaskRecord.type == "webhook_callback"))
            ).scalars().all()
        assert tasks, "webhook 任务未入队"
        for t in tasks:
            assert "webhook_secret" not in (t.payload or {}), "payload 仍携带 secret 键"
            assert secret not in json.dumps(t.payload or {}), "payload 泄漏明文 secret"
            assert t.payload.get("tenant_id") == TENANT_ID
    finally:
        init_cipher(None)


async def test_webhook_delivery_decrypts_stored_secret(auth_gateway):
    """GW-8 回归(投递端到端):库里存密文,投递时查库解密 → HMAC 验签仍通过;
    未配 url 的租户任务按 skipped 完成(不留死信不重试)。"""
    import hashlib
    import hmac as hmac_mod

    from sqlalchemy import select, update

    from app.db.models import TaskRecord, Tenant
    from app.security import crypto
    from app.security.crypto import init_cipher, encrypt_secret

    client, app, mock, raw = auth_gateway
    secret = "whsec-gw8-e2e-0123456789"
    got: dict = {}

    from fastapi import FastAPI, Request

    receiver = FastAPI()

    @receiver.post("/receive")
    async def receive(request: Request):
        got["raw"] = await request.body()
        got["sig"] = request.headers.get("X-Agentgate-Signature", "")
        got["ts"] = request.headers.get("X-Agentgate-Timestamp", "")
        return {"ok": True}

    import socket

    import uvicorn as _uv

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

    init_cipher(crypto.generate_key())
    try:
        # 密文直落库(模拟升级路径:管理端写入即密文)
        async with app.state.session_factory() as db:
            await db.execute(
                update(Tenant).where(Tenant.id == TENANT_ID).values(
                    webhook_url=f"http://127.0.0.1:{port}/receive",
                    webhook_secret=encrypt_secret(secret),
                )
            )
            await db.commit()
        r = await client.post(
            "/v1/chat/completions",
            json={"model": "mock-model", "messages": [{"role": "user", "content": "hi"}]},
            headers=auth_headers(raw),
        )
        assert r.status_code == 200
        await app.state.task_queue.flush()
        assert got.get("raw"), "webhook 未投递"
        expected = hmac_mod.new(
            secret.encode(), f"{got['ts']}.".encode() + got["raw"], hashlib.sha256
        ).hexdigest()
        assert got["sig"].startswith("sha256=")
        assert hmac_mod.compare_digest(got["sig"][len("sha256="):], expected)

        # 任务账本:payload 干净 + skipped/delivered 结果不进死信
        async with app.state.session_factory() as db:
            tasks = (
                await db.execute(select(TaskRecord).where(TaskRecord.type == "webhook_callback"))
            ).scalars().all()
        assert all(t.status == "done" for t in tasks)
        assert all("webhook_secret" not in (t.payload or {}) for t in tasks)
    finally:
        init_cipher(None)
        server.should_exit = True
        await server_task
