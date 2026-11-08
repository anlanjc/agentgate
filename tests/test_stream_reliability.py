"""GW-5/GW-9(评审 2.0)回归:流式断开记账 + SSE 异常/截断契约。

用裸 ASGI 驱动 + 真 TCP 上游(starlette 的断开取消与 background 行为依赖
spec_version 分支,httpx.ASGITransport 无法模拟客户端中途断开 —— 且它会缓冲
上游整条流,aiter_raw 永不真挂起,取消无检查点可投递):
- spec 2.3(uvicorn 0.54 生产口径):断开经 anyio cancel scope → relay 收 CancelledError;
- spec 2.4:服务器不再监听 disconnect,断开 = 下游 send 抛 OSError → 生成器被
  GeneratorExit 收尾 —— 两条路径都必须把已产生的 usage 记进账本。
"""

import asyncio
import json
import socket

import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import select

from app.auth.keys import generate_key
from app.db.models import Tenant, UsageRecord, VirtualKey
from app.db.session import init_db
from app.upstream.client import UpstreamClient
from tests.conftest import auth_headers


def build_slow_tail_upstream(tail_delay: float = 0.5) -> FastAPI:
    """usage 块之后、[DONE] 之前插入延迟:给"客户端中途断开"留出确定性窗口。"""

    app = FastAPI()

    @app.post("/v1/chat/completions")
    async def chat(request: Request):
        body = await request.json()

        def chunk(delta: dict, finish: str | None = None, usage: dict | None = None) -> bytes:
            data = {
                "id": "chatcmpl-gw5", "object": "chat.completion.chunk", "created": 0,
                "model": body.get("model", "mock-model"),
                "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
            }
            if usage is not None:
                data["usage"] = usage
            return f"data: {json.dumps(data, ensure_ascii=False)}\n\n".encode()

        async def gen():
            yield chunk({"role": "assistant"})
            yield chunk({"content": "你"})
            yield chunk({}, finish="stop", usage={"prompt_tokens": 11, "completion_tokens": 7, "total_tokens": 18})
            await asyncio.sleep(tail_delay)  # 断开窗口:取消应落在这里
            yield b"data: [DONE]\n\n"

        return StreamingResponse(gen(), media_type="text/event-stream")

    return app


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


async def _serve_upstream() -> tuple[uvicorn.Server, asyncio.Task, int]:
    port = _free_port()
    server = uvicorn.Server(
        uvicorn.Config(build_slow_tail_upstream(), host="127.0.0.1", port=port, log_level="error")
    )
    task = asyncio.get_running_loop().create_task(server.serve())
    for _ in range(50):
        if server.started:
            break
        await asyncio.sleep(0.05)
    return server, task, port


async def _make_gateway_app(auth_settings, redis_client, port: int):
    from app.api.main import create_app

    upstream = UpstreamClient(
        auth_settings.upstream_base_url, auth_settings.upstream_api_key,
        client=httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{port}/v1",
            headers={"Authorization": f"Bearer {auth_settings.upstream_api_key}"},
            trust_env=False,
            timeout=httpx.Timeout(connect=5.0, read=300.0, write=30.0, pool=5.0),
        ),
    )
    app = create_app(settings=auth_settings, upstream=upstream, redis_client=redis_client)
    await init_db(app.state.engine)
    raw, key_hash, prefix = generate_key()
    async with app.state.session_factory() as db:
        db.add(Tenant(id="t-gw5", name="gw5"))
        db.add(VirtualKey(id="k-gw5", tenant_id="t-gw5", key_hash=key_hash, key_prefix=prefix,
                          rpm_limit=10_000, tpm_limit=10_000_000))
        await db.commit()
    return app, raw, upstream


async def _finalize_abandoned_generators() -> None:
    """GW-5 spec2.4 场景的确定性收尾:被弃的 relay 生成器悬在 yield 点,
    GeneratorExit 收尾(含显式记账)依赖 asyncgen finalizer —— 不赌 GC 时序,
    直接 shutdown_asyncgens 同步驱动(loop 内其余生成器均已自然耗尽,无副作用)。"""
    await asyncio.get_running_loop().shutdown_asyncgens()
    import gc

    gc.collect()  # 兜底:清掉残余引用,防 "Task exception was never retrieved" 噪声
    await asyncio.sleep(0.01)


async def _poll_ledger_rows(app) -> list:
    rows: list = []
    for _ in range(300):
        async with app.state.session_factory() as db:
            rows = (await db.execute(select(UsageRecord))).scalars().all()
        if rows:
            break
        await _finalize_abandoned_generators()
        await asyncio.sleep(0.02)
    return rows


async def _drive_stream_disconnect(
    auth_settings, redis_client, *, spec_version: str, send_raises_oserror: bool
):
    """真 TCP 上游 + 裸 ASGI 客户端:usage 块透传后立即断开,断言账本仍落 499+18token。"""
    server, server_task, port = await _serve_upstream()
    app, raw, upstream = await _make_gateway_app(auth_settings, redis_client, port)

    body = json.dumps({
        "model": "mock-model", "messages": [{"role": "user", "content": "hi"}], "stream": True,
    }).encode()
    scope = {
        "type": "http", "asgi": {"version": "3.0", "spec_version": spec_version},
        "http_version": "1.1", "method": "POST", "scheme": "http",
        "path": "/v1/chat/completions", "raw_path": b"/v1/chat/completions",
        "query_string": b"", "root_path": "",
        "headers": [
            (b"host", b"gateway"),
            (b"content-type", b"application/json"),
            (b"authorization", f"Bearer {raw}".encode()),
        ],
        "client": ("testclient", 50000), "server": ("gateway", 80),
    }
    usage_sent = asyncio.Event()
    disconnect_now = asyncio.Event()

    async def receive():
        if not getattr(receive, "_body_sent", False):
            receive._body_sent = True
            return {"type": "http.request", "body": body, "more_body": False}
        await disconnect_now.wait()
        return {"type": "http.disconnect"}

    async def send(message):
        if (
            message["type"] == "http.response.body"
            and b'"usage"' in message.get("body", b"")
        ):
            usage_sent.set()
            if send_raises_oserror:
                raise OSError("transport closed by peer")  # spec 2.4 断开形态

    try:
        app_task = asyncio.get_running_loop().create_task(app(scope, receive, send))
        await asyncio.wait_for(usage_sent.wait(), timeout=5)
        disconnect_now.set()

        # 在隔离协程里消费结果/异常:spec 2.4 的 ClientDisconnect traceback 引用
        # StreamingResponse 帧链(body_iterator=relay 生成器)——引用不放掉,
        # 生成器不被 GC,GeneratorExit 收尾(记账)就不会发生。
        outcome: dict = {}

        async def reap() -> None:
            try:
                await app_task
                outcome["end"] = "normal"
            except Exception as exc:
                outcome["end"] = type(exc).__name__

        await reap()
        app_task = None  # Task 对象持有 exception/traceback,一并放掉

        if send_raises_oserror:
            assert outcome["end"] == "ClientDisconnect"
        else:
            assert outcome["end"] == "normal"  # spec 2.3:取消在任务组内消化

        await _finalize_abandoned_generators()  # spec 2.4:确定性驱动 GeneratorExit 收尾
        rows = await _poll_ledger_rows(app)
        assert len(rows) == 1, f"账本应恰好一行,实际 {len(rows)}"
        row = rows[0]
        assert row.status == 499  # 断开如实记账,不伪装 2xx
        assert row.total_tokens == 18  # 已产生的 token 不漏计(修复前恒 0/行不存在)
        assert row.ttft_ms is not None  # TTFT 指标随收尾赋值恢复
    finally:
        from app.queue.streams import TaskQueue

        tq: TaskQueue = app.state.task_queue
        await tq.stop()
        await app.state.channel_router.aclose()
        await upstream.aclose()
        await app.state.engine.dispose()
        server.should_exit = True
        await server_task


async def test_stream_disconnect_spec23_still_bills(auth_settings, redis_client):
    """GW-5 回归(spec 2.3 / uvicorn 0.54 生产口径):客户端在 usage 块到达后断开,
    cancel scope 级联取消 —— 账本仍记 499 + 18 token + TTFT。
    修复前 finally 内首个 await(aclose)再抛 CancelledError,usage/指标永不写入。"""
    await _drive_stream_disconnect(
        auth_settings, redis_client, spec_version="2.3", send_raises_oserror=False
    )


async def test_stream_disconnect_spec24_still_bills(auth_settings, redis_client):
    """GW-5 回归(spec 2.4):断开 = 下游 send 抛 OSError,background 整个不跑 ——
    relay 收尾显式记账兜底,账本仍有 499 + 18 token 行(修复前连账本行都没有)。"""
    await _drive_stream_disconnect(
        auth_settings, redis_client, spec_version="2.4", send_raises_oserror=True
    )


async def test_stream_normal_completion_no_double_accounting(auth_settings, redis_client):
    """GW-5 反向回归:正常跑完的流式请求仍走 background 记账,恰好一行不双计。"""
    from app.api.main import create_app

    mock_app = build_slow_tail_upstream(tail_delay=0.0)
    upstream = UpstreamClient(
        auth_settings.upstream_base_url, auth_settings.upstream_api_key,
        client=httpx.AsyncClient(
            transport=httpx.ASGITransport(app=mock_app),
            base_url=auth_settings.upstream_base_url,
        ),
    )
    app = create_app(settings=auth_settings, upstream=upstream, redis_client=redis_client)
    await init_db(app.state.engine)
    raw, key_hash, prefix = generate_key()
    async with app.state.session_factory() as db:
        db.add(Tenant(id="t-gw5", name="gw5"))
        db.add(VirtualKey(id="k-gw5", tenant_id="t-gw5", key_hash=key_hash, key_prefix=prefix,
                          rpm_limit=10_000, tpm_limit=10_000_000))
        await db.commit()

    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://gw"
        ) as client:
            r = await client.post(
                "/v1/chat/completions",
                json={"model": "mock-model", "messages": [{"role": "user", "content": "hi"}],
                      "stream": True},
                headers={"Authorization": f"Bearer {raw}"},
            )
            assert r.status_code == 200
            assert b"data: [DONE]" in r.content
        await app.state.task_queue.flush()
        async with app.state.session_factory() as db:
            rows = (await db.execute(select(UsageRecord))).scalars().all()
        assert len(rows) == 1
        assert rows[0].status == 200 and rows[0].total_tokens == 18
    finally:
        await app.state.task_queue.stop()
        await upstream.aclose()
        await app.state.engine.dispose()


# ---------- GW-9(评审 2.0)回归:SSE 异常帧统一信封 + 干净截断探测 ----------


async def test_stream_break_yields_unified_error_frame_and_502_ledger(auth_gateway):
    """GW-9 回归(干净截断):上游 stream_break(发一半关流,无 [DONE])——
    修复前 relay 正常走完、无任何异常标记、账本静默记 200。
    修复后:补发 event: error + 统一错误信封(code=stream_truncated_no_done,
    含 request_id),账本记 502,响应中无 [DONE]。"""
    import json as json_mod

    from app.db.models import UsageRecord

    client, app, mock, raw = auth_gateway
    r = await client.post(
        "/v1/chat/completions",
        json={"model": "mock-model", "messages": [{"role": "user", "content": "hi"}],
              "stream": True, "stream_options": {"include_usage": True}},
        headers={**auth_headers(raw), "X-Mock-Case": "stream_break"},
    )
    assert r.status_code == 200  # 响应头已提交(SSE),终态在帧与账本里如实标注
    body = r.content
    assert b"data: [DONE]" not in body  # 截断流不发 [DONE](判据契约)
    assert b"event: error" in body
    # 最后一帧 data 是统一信封(与 HTTP 错误同构,含 request_id)
    last_data = [ln for ln in body.split(b"\n") if ln.startswith(b"data: ")][-1]
    envelope = json_mod.loads(last_data[len(b"data: "):])
    assert envelope["error"]["type"] == "bad_gateway"
    assert envelope["error"]["code"] == "stream_truncated_no_done"
    assert envelope["error"]["request_id"]

    await app.state.task_queue.flush()
    async with app.state.session_factory() as db:
        from sqlalchemy import select

        rows = (await db.execute(select(UsageRecord))).scalars().all()
    assert len(rows) == 1 and rows[0].status == 502  # 账本不静默记 200


async def test_sse_error_frame_shape_is_gateway_envelope():
    """GW-9 单元回归:异常帧构造器输出网关统一信封(SSE 承载),非自定义 schema。"""
    from app.api.v1_chat import _sse_error_frame

    frame = _sse_error_frame("boom mid-stream", "upstream_read_error_midstream")
    assert frame.startswith(b"event: error\ndata: ")
    assert frame.endswith(b"\n\n")
    import json as json_mod

    payload = json_mod.loads(frame[len(b"event: error\ndata: "):-2].decode())
    assert payload["error"]["message"] == "boom mid-stream"
    assert payload["error"]["type"] == "bad_gateway"
    assert payload["error"]["code"] == "upstream_read_error_midstream"
    assert "request_id" in payload["error"]


# ---------- GW-13(评审 3.0)回归:[DONE] 判定兼容 SSE 规范允许的间距变体 ----------


def test_sse_parser_done_marker_variants():
    """GW-13 单元回归:[DONE] 判定在 body 归一化之后做 —— `data:[DONE]`(无空格)
    等合法 SSE 变体必须置位 saw_done(修复版整行全等比较会把它们漏成 json 静默失败,
    完整流被误判干净截断,账本记 502)。"""
    from app.upstream.sse import SSELineParser

    for raw in (
        b"data: [DONE]\n\n",   # 规范间距
        b"data:[DONE]\n\n",    # 无空格(规范允许)
        b"data:  [DONE]\n\n",  # 多空格(strip 归一)
        b"data:[DONE]\r\n",    # CRLF 行尾(SSE 规范允许)
        b"data: [DONE]\r\r\n", # 混合尾随空白
    ):
        p = SSELineParser()
        assert p.feed(raw) == []
        assert p.saw_done is True, f"变体 {raw!r} 应识别为 [DONE]"

    # 负例:JSON 里含 "[DONE]" 字样 / 带后缀的伪终止 / 非 data 行,不置位
    p = SSELineParser()
    p.feed(b'data: {"text": "[DONE]"}\n\n')
    assert p.saw_done is False
    p2 = SSELineParser()
    p2.feed(b"data: [DONE]extra\n\n")
    assert p2.saw_done is False
    p3 = SSELineParser()
    p3.feed(b"event: [DONE]\n\n")
    assert p3.saw_done is False


def test_sse_parser_done_across_chunk_boundary():
    """[DONE] 跨 TCP 分段:解析器缓冲语义不因 GW-13 改动回退。"""
    from app.upstream.sse import SSELineParser

    p = SSELineParser()
    assert p.feed(b'data: {"a": 1}\ndata:[DO') == [{"a": 1}]
    assert p.saw_done is False
    assert p.feed(b"NE]\n\n") == []
    assert p.saw_done is True


async def test_stream_nonstandard_done_marker_not_truncated(auth_settings, redis_client):
    """GW-13 集成回归:上游以 `data:[DONE]`(无空格)收尾完整流 —— 修复前
    saw_done 不置位,relay 补发 error 帧且账本误记 502;修复后如实 200,
    转发字节零改动(客户端收到的就是上游原样)。"""
    from app.api.main import create_app
    from app.db.models import UsageRecord
    from app.upstream.client import UpstreamClient

    async def gen():
        yield b'data: {"choices": [{"delta": {"content": "ok"}}]}\n\n'
        yield b"data:[DONE]\n\n"  # 非规范间距(规范合法)

    upstream_app = FastAPI()

    @upstream_app.post("/v1/chat/completions")
    async def chat(request: Request):
        return StreamingResponse(gen(), media_type="text/event-stream")

    upstream = UpstreamClient(
        auth_settings.upstream_base_url, auth_settings.upstream_api_key,
        client=httpx.AsyncClient(
            transport=httpx.ASGITransport(app=upstream_app),
            base_url=auth_settings.upstream_base_url,
        ),
    )
    app = create_app(settings=auth_settings, upstream=upstream, redis_client=redis_client)
    await init_db(app.state.engine)
    raw, key_hash, prefix = generate_key()
    async with app.state.session_factory() as db:
        db.add(Tenant(id="t-gw13", name="gw13"))
        db.add(VirtualKey(id="k-gw13", tenant_id="t-gw13", key_hash=key_hash, key_prefix=prefix,
                          rpm_limit=10_000, tpm_limit=10_000_000))
        await db.commit()

    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://gw"
        ) as client:
            r = await client.post(
                "/v1/chat/completions",
                json={"model": "mock-model", "messages": [{"role": "user", "content": "hi"}],
                      "stream": True},
                headers={"Authorization": f"Bearer {raw}"},
            )
        assert r.status_code == 200
        assert b"data:[DONE]" in r.content  # 透传零改动
        assert b"event: error" not in r.content  # 完整流不补发 error 帧
        await app.state.task_queue.flush()
        async with app.state.session_factory() as db:
            from sqlalchemy import select

            rows = (await db.execute(select(UsageRecord))).scalars().all()
        assert len(rows) == 1 and rows[0].status == 200  # 不误记 502
    finally:
        await app.state.task_queue.stop()
        await upstream.aclose()
        await app.state.engine.dispose()
