"""通用 Agent 服务 OpenAI 适配器(shim)—— 把自有应用的 JSON API 包成 /v1/chat/completions。

背景:AskBI(/api/ask)与 PolicyCompass(/chat)是自有 JSON 协议,不是 OpenAI 兼容服务。
网关渠道层说 OpenAI 协议;接入方式二选一:
  a) 本文件模式:给应用配一个 ~60 行 shim(部署形态多一个进程,应用零改动);
  b) 渠道层协议适配器(LiteLLM 对 Anthropic 的做法)—— backlog,见 EVIDENCE §⑦。
以 PolicyCompass 为例:SHIM_TARGET_URL 指向其 /chat,shim 把 OpenAI 请求映射过去,
回复包成 chat.completion 格式。AskBI 同理(SHIM_TARGET_URL 指向 /api/ask,映射其任务模型)。
"""

import os
import time

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse


def build_agent_shim(target_url: str, agent_name: str = "agent") -> FastAPI:
    """SHIM_TARGET_URL = 自有应用的对话端点(POST,JSON in/out)。

    映射约定(以 PolicyCompass /chat 为例,字段可按目标应用调整):
      OpenAI messages[-1].content  ->  {"question": ...}
      返回 {"answer": ...}          ->  choices[0].message.content
    """
    app = FastAPI()
    client = httpx.AsyncClient(trust_env=False, timeout=120.0)  # 该机代理坑:直连

    @app.get("/v1/models")
    async def models():
        return {"object": "list", "data": [{"id": agent_name, "object": "model"}]}

    @app.post("/v1/chat/completions")
    async def chat_completions(request: Request):
        payload = await request.json()
        messages = payload.get("messages", [])
        question = next(
            (m.get("content") for m in reversed(messages) if m.get("role") == "user"), ""
        )
        try:
            resp = await client.post(target_url, json={"question": question})
        except httpx.ConnectError:
            return JSONResponse(
                status_code=502,
                content={"error": {"message": f"agent {agent_name} unreachable", "type": "bad_gateway"}},
            )
        if resp.status_code >= 500 or resp.status_code == 429:
            return JSONResponse(status_code=resp.status_code, content={"error": {"message": "agent error"}})
        answer = resp.json().get("answer", "")

        created = int(time.time())
        if payload.get("stream"):
            async def sse():
                def chunk(delta: dict, finish: str | None = None) -> bytes:
                    import json

                    data = {
                        "id": f"chatcmpl-{agent_name}",
                        "object": "chat.completion.chunk",
                        "created": created,
                        "model": agent_name,
                        "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
                    }
                    return f"data: {json.dumps(data, ensure_ascii=False)}\n\n".encode()

                # 自有应用若本身支持流式,逐段转发;此处演示整段拆 token
                import asyncio

                for piece in _split_tokens(answer):
                    yield chunk({"content": piece})
                    await asyncio.sleep(0)
                text_usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
                yield chunk({}, finish="stop")
                yield (
                    b'data: {"id":"x","object":"chat.completion.chunk","created":%d,"model":"%s",'
                    b'"choices":[{"index":0,"delta":{},"finish_reason":null}],"usage":%s}\n\n'
                    % (created, agent_name.encode(), _json_bytes(text_usage))
                )
                yield b"data: [DONE]\n\n"

            return StreamingResponse(sse(), media_type="text/event-stream")

        return JSONResponse(
            status_code=200,
            content={
                "id": f"chatcmpl-{agent_name}",
                "object": "chat.completion",
                "created": created,
                "model": agent_name,
                "choices": [
                    {"index": 0, "message": {"role": "assistant", "content": answer}, "finish_reason": "stop"}
                ],
                "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
            },
        )

    def _json_bytes(obj) -> bytes:
        import json

        return json.dumps(obj).encode()

    def _split_tokens(text: str, width: int = 6) -> list[str]:
        return [text[i : i + width] for i in range(0, len(text), width)] or [""]

    return app


if __name__ == "__main__":
    import uvicorn

    target = os.environ.get("SHIM_TARGET_URL", "http://127.0.0.1:8000/chat")
    port = int(os.environ.get("SHIM_PORT", "9401"))
    uvicorn.run(build_agent_shim(target), host="127.0.0.1", port=port)
