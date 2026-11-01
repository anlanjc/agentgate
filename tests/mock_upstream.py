"""进程内 mock 上游(FastAPI ASGI app):免费、确定、可注入故障。

故障注入面(经 X-Mock-Case 头控制,M2 状态机测试依赖):
- ok(默认) | err_429(限流,带 Retry-After) | err_500 | slow(睡眠 2s 触发读超时)
- stream_break:流式发一半即断流(测 mid-stream 处理)
"""

import asyncio
import json
import time

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse


def build_mock_upstream(default_case: str = "ok") -> FastAPI:
    app = FastAPI()
    created = int(time.time())

    def completion_body(model: str, content: str) -> dict:
        return {
            "id": "chatcmpl-mock-001",
            "object": "chat.completion",
            "created": created,
            "model": model,
            "choices": [
                {"index": 0, "message": {"role": "assistant", "content": content}, "finish_reason": "stop"}
            ],
            "usage": {"prompt_tokens": 11, "completion_tokens": 7, "total_tokens": 18},
        }

    async def sse_chunks(model: str, tokens: list[str], include_usage: bool):
        def chunk(delta: dict, finish: str | None = None, usage: dict | None = None) -> bytes:
            data = {
                "id": "chatcmpl-mock-001",
                "object": "chat.completion.chunk",
                "created": created,
                "model": model,
                "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
            }
            if usage is not None:
                data["usage"] = usage
            return f"data: {json.dumps(data, ensure_ascii=False)}\n\n".encode()

        yield chunk({"role": "assistant"})
        for tk in tokens:
            yield chunk({"content": tk})
        yield chunk({}, finish="stop", usage={"prompt_tokens": 11, "completion_tokens": 7, "total_tokens": 18} if include_usage else None)
        yield b"data: [DONE]\n\n"

    @app.post("/v1/chat/completions")
    async def chat_completions(request: Request):
        case = request.headers.get("x-mock-case", default_case)
        body = await request.json()
        model = body.get("model", "mock-model")
        stream = bool(body.get("stream"))

        if case == "err_429":
            return JSONResponse(
                status_code=429,
                content={"error": {"message": "rate limited", "type": "rate_limit"}},
                headers={"Retry-After": "1"},
            )
        if case == "err_500":
            return JSONResponse(status_code=500, content={"error": {"message": "boom"}})

        if case == "slow":
            await asyncio.sleep(2.0)

        if not stream:
            return JSONResponse(status_code=200, content=completion_body(model, "mock reply"))

        include_usage = bool((body.get("stream_options") or {}).get("include_usage"))
        tokens = ["你", "好", "!"]
        if case == "stream_break":

            async def broken():
                gen = sse_chunks(model, tokens, include_usage=False)
                async for c in gen:
                    yield c
                    break  # 只发第一个 chunk 就断流
                yield b""  # 触发连接关闭

            return StreamingResponse(broken(), media_type="text/event-stream")
        return StreamingResponse(sse_chunks(model, tokens, include_usage), media_type="text/event-stream")

    @app.get("/v1/models")
    async def models():
        return {"object": "list", "data": [{"id": "mock-model", "object": "model"}]}

    @app.get("/health")
    async def health():
        return {"status": "ok", "mock": True}

    return app
