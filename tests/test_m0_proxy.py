"""M0 验收测试:OpenAI 兼容代理语义(SSE 字节透传 / 错误映射 / request_id)。"""

import asyncio
import json

import httpx

from app.api.main import create_app
from app.upstream.client import UpstreamClient


def _chat_body(stream: bool = False, **extra) -> dict:
    body = {
        "model": "mock-model",
        "messages": [{"role": "user", "content": "你好"}],
        "stream": stream,
    }
    body.update(extra)
    return body


async def test_health(gateway):
    client, app, _ = gateway
    resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


async def test_non_stream_passthrough(gateway):
    client, app, mock = gateway
    resp = await client.post("/v1/chat/completions", json=_chat_body())
    assert resp.status_code == 200
    data = resp.json()
    assert data["object"] == "chat.completion"
    assert data["choices"][0]["message"]["content"] == "mock reply"
    assert data["usage"]["total_tokens"] == 18
    assert resp.headers["x-request-id"]  # 网关注入


async def test_non_stream_unknown_extra_fields_passthrough(gateway):
    """未知字段(上游新增参数)必须原样透传,网关不因协议演进而升级。"""
    client, app, mock_app = gateway
    resp = await client.post(
        "/v1/chat/completions", json=_chat_body(temperature=0.7, some_future_param={"a": 1})
    )
    assert resp.status_code == 200


async def test_stream_passthrough_sse(gateway):
    client, app, _ = gateway
    resp = await client.post("/v1/chat/completions", json=_chat_body(stream=True))
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/event-stream")

    lines = [ln for ln in resp.text.splitlines() if ln.startswith("data: ")]
    assert lines[-1] == "data: [DONE]"
    deltas = []
    for ln in lines[:-1]:
        payload = json.loads(ln[len("data: ") :])
        if payload.get("choices") and payload["choices"][0].get("delta", {}).get("content"):
            deltas.append(payload["choices"][0]["delta"]["content"])
    assert "".join(deltas) == "你好!"


async def test_stream_ttft_sampled(gateway):
    """TTFT 必须被采样(>0,量级 < 1s —— mock 立即响应)。"""
    client, app, _ = gateway
    resp = await client.post("/v1/chat/completions", json=_chat_body(stream=True))
    assert resp.status_code == 200
    samples = list(app.state.ttft_samples)
    assert len(samples) == 1
    assert 0 < samples[0] < 1000


async def test_request_id_echo(gateway):
    client, app, _ = gateway
    resp = await client.post(
        "/v1/chat/completions", json=_chat_body(), headers={"X-Request-Id": "req-abc-123"}
    )
    assert resp.headers["x-request-id"] == "req-abc-123"


async def test_upstream_500_passthrough(gateway):
    """M0 代理语义:上游应用层错误状态码原样透传(不吞、不翻译)。"""
    client, app, _ = gateway
    resp = await client.post(
        "/v1/chat/completions", json=_chat_body(), headers={"X-Mock-Case": "err_500"}
    )
    assert resp.status_code == 500
    assert resp.json()["error"]["message"] == "boom"


async def test_upstream_unreachable_maps_502(settings, redis_client):
    """连接类异常映射 502 + 网关错误信封(含 request_id)。

    用真实 TCP 连接(discard 端口必被拒),ASGITransport 永远连不上、测不了连接失败。
    """
    upstream = UpstreamClient("http://127.0.0.1:9/v1", "sk-x", connect_timeout=0.5)
    try:
        app = create_app(settings=settings, upstream=upstream, redis_client=redis_client)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://gateway"
        ) as client:
            resp = await client.post("/v1/chat/completions", json=_chat_body())
        assert resp.status_code == 502
        err = resp.json()["error"]
        assert err["code"] == "upstream_connect_failed"
        assert err["request_id"] == resp.headers["x-request-id"]
    finally:
        await upstream.aclose()


async def test_upstream_read_timeout_maps_504(settings, redis_client):
    """读超时映射 504。

    注意:ASGITransport 不经过 socket,httpx 超时永不触发 —— 必须用真实 TCP 服务器
    (接受连接后不响应),这才是生产语义的超时测试。
    """
    async def stall_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            await reader.read(65536)
            # 装死;客户端超时中止断连后立即退出,不留 10s 挂尾
            for _ in range(100):
                await asyncio.sleep(0.1)
                if writer.transport.is_closing():
                    return
        except (ConnectionError, asyncio.CancelledError):
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(stall_handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        upstream = UpstreamClient(
            f"http://127.0.0.1:{port}/v1", "sk-x", read_timeout=0.3, connect_timeout=1.0
        )
        try:
            app = create_app(settings=settings, upstream=upstream, redis_client=redis_client)
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://gateway"
            ) as client:
                resp = await client.post("/v1/chat/completions", json=_chat_body())
            assert resp.status_code == 504
            assert resp.json()["error"]["code"] == "upstream_timeout"
        finally:
            await upstream.aclose()
    finally:
        server.close()
        await server.wait_closed()


async def test_upstream_connection_reset_maps_502(settings, redis_client):
    """故障演练实证回归:上游进程被 kill(连接重置)→ 502 而非未处理 500。

    真实 TCP 服务器:接受请求后立即粗暴关闭(RST 语义),httpx 抛 ReadError。
    """
    async def reset_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            await reader.read(65536)
            writer.transport.abort()  # RST:模拟进程被 kill -9
        except (ConnectionError, asyncio.CancelledError):
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(reset_handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        upstream = UpstreamClient(
            f"http://127.0.0.1:{port}/v1", "sk-x", connect_timeout=1.0, read_timeout=5.0
        )
        try:
            app = create_app(settings=settings, upstream=upstream, redis_client=redis_client)
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://gateway"
            ) as client:
                resp = await client.post("/v1/chat/completions", json=_chat_body())
            assert resp.status_code == 502
            assert resp.json()["error"]["code"] == "upstream_connect_failed"
        finally:
            await upstream.aclose()
    finally:
        server.close()
        await server.wait_closed()


async def test_validation_error_422(gateway):
    client, app, _ = gateway
    resp = await client.post("/v1/chat/completions", json={"model": "mock-model", "messages": []})
    assert resp.status_code == 422


async def test_models_list(gateway):
    client, app, _ = gateway
    resp = await client.get("/v1/models")
    assert resp.status_code == 200
    assert resp.json()["data"][0]["id"] == "mock-model"
