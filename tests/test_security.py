"""P2 安全 hardening:渠道密钥静态加密(Fernet)——加解密往返/存量明文兼容/持久化密文/重启解密。"""

import pytest

from app.security import crypto
from app.security.crypto import InvalidToken, decrypt_secret, encrypt_secret, init_cipher


@pytest.fixture(autouse=True)
def _reset_cipher():
    """每用例后复位为"无主密钥"口径,避免全局密钥泄漏到其他测试。"""
    yield
    init_cipher(None)


def test_roundtrip_with_master_key():
    init_cipher(crypto.generate_key())
    stored = encrypt_secret("sk-super-secret")
    assert stored != "sk-super-secret"
    assert crypto.is_encrypted(stored) and stored.startswith("enc:v1:")
    assert decrypt_secret(stored) == "sk-super-secret"


def test_no_master_key_is_plaintext_dev_mode():
    """dev 口径诚实明文(启动有告警,prod 被校验器拒绝):不假装安全。"""
    init_cipher(None)
    assert encrypt_secret("sk-plain") == "sk-plain"
    assert decrypt_secret("sk-plain") == "sk-plain"
    assert not crypto.is_encrypted("sk-plain")


def test_legacy_plaintext_rows_still_readable():
    """存量明文行(加密上线前)读取原样返回 —— 向后兼容,不做破坏性迁移。"""
    init_cipher(crypto.generate_key())
    assert decrypt_secret("sk-legacy-plaintext-row") == "sk-legacy-plaintext-row"


def test_wrong_master_key_raises_not_garbage():
    """主密钥轮换后解密:抛 InvalidToken,宁可渠道路由失败也不用错密钥打上游。"""
    init_cipher(crypto.generate_key())
    stored = encrypt_secret("sk-x")
    init_cipher(crypto.generate_key())  # 轮换
    with pytest.raises(InvalidToken):
        decrypt_secret(stored)


def test_empty_secret_passthrough():
    """空密钥(内网 Agent 服务无 key 的真实场景)不包装。"""
    init_cipher(crypto.generate_key())
    assert encrypt_secret("") == "" and decrypt_secret("") == ""


def test_prod_requires_channel_enc_key():
    """prod fail-closed:渠道密钥明文口径不允许上生产(与 admin_token 同款)。"""
    from pydantic import ValidationError

    from app.config import Settings

    base = {
        "environment": "prod", "admin_token": "strong-token", "auth_enabled": True,
        "upstream_name": "m", "upstream_base_url": "http://m/v1", "models": ["m"],
    }
    with pytest.raises(ValidationError, match="CHANNEL_ENC_KEY"):
        Settings(**base)
    s = Settings(**base, channel_enc_key=crypto.generate_key())
    assert s.channel_enc_key


async def test_channel_key_encrypted_at_rest_and_usable(auth_settings, redis_client, tmp_path):
    """端到端:注册渠道 → DB 落密文(非明文)→ 重启恢复路径解密后真 HTTP 可用。

    渠道走真 uvicorn 端口(恢复路径的 UpstreamClient 是真 httpx 客户端,
    ASGI 别名主机解析不了)—— 解密后的密钥必须能在真实 TCP 上通过上游鉴权语义。
    """
    import socket
    import threading

    import httpx
    import uvicorn
    from sqlalchemy import select

    from app.api.main import create_app, load_channels_from_db
    from app.db.models import Channel
    from app.db.session import init_db
    from tests.mock_upstream import build_mock_upstream

    def _free_port() -> int:
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        s.close()
        return port

    up_port = _free_port()
    server = uvicorn.Server(
        uvicorn.Config(build_mock_upstream(), host="127.0.0.1", port=up_port, log_level="error")
    )
    threading.Thread(target=server.run, daemon=True).start()
    for _ in range(100):
        if server.started:
            break
        import asyncio

        await asyncio.sleep(0.05)
    base_url = f"http://127.0.0.1:{up_port}/v1"

    try:
        auth_settings = auth_settings.model_copy(
            update={"channel_enc_key": crypto.generate_key()}
        )
        app = create_app(settings=auth_settings, redis_client=redis_client)
        await init_db(app.state.engine)

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://gw"
        ) as client:
            r = await client.post(
                "/admin/channels",
                json={"name": "enc-ch", "base_url": base_url, "api_key": "sk-visible-once"},
            )
            assert r.status_code == 200, r.text

        async with app.state.session_factory() as db:
            ch = (
                await db.execute(select(Channel).where(Channel.name == "enc-ch"))
            ).scalar_one()
            assert ch.api_key != "sk-visible-once"
            assert crypto.is_encrypted(ch.api_key)

        # 重启语义:新实例从同一 DB 恢复(解密)并服务真 HTTP 请求
        app2 = create_app(settings=auth_settings, redis_client=redis_client)
        await init_db(app2.state.engine)
        restored = await load_channels_from_db(app2)
        assert restored >= 1
        client2 = app2.state.channel_router.client("enc-ch")
        resp = await client2.chat_completion(
            {"model": "mock-model", "messages": [{"role": "user", "content": "hi"}]}
        )
        assert resp.status_code == 200
        await app.state.task_queue.stop()
        await app2.state.task_queue.stop()
    finally:
        server.should_exit = True
