"""AskBI 专用 shim:OpenAI 协议 ↔ AskBI /api/ask(SSE 流式 + 结构化答案)。

与 PC shim(agent_shim_example.py)的差异:AskBI 是 SSE 流式 + 结构化答案 dict
(kind: answer/refusal/clarify),需要消费 SSE、等 final、把 table/summary 渲染成文本。
"""

import json
import time

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


def _render_answer(answer: dict) -> str:
    kind = answer.get("kind")
    if kind == "answer":
        parts = []
        if answer.get("summary"):
            parts.append(str(answer["summary"]))
        if answer.get("narrative"):
            parts.append(str(answer["narrative"]))
        t = answer.get("table") or {}
        if t.get("rows"):
            cols = t.get("columns", [])
            if cols:
                parts.append(" | ".join(map(str, cols)))
            for row in t["rows"][:10]:
                parts.append(" | ".join(map(str, row)))
        if answer.get("caliber"):
            parts.append(f"口径:{answer['caliber']}")
        return "\n".join(parts) or "(空结果)"
    if kind == "refusal":
        return f"[拒答] {answer.get('message', '')}"
    if kind == "clarify":
        opts = " / ".join(map(str, answer.get("options", [])))
        return f"[需澄清] {answer.get('message', '')}({opts})"
    return f"[{kind}] {answer.get('message', '')}"


def build_askbi_shim(target_url: str, agent_name: str = "askbi") -> FastAPI:
    app = FastAPI()
    client = httpx.AsyncClient(trust_env=False, timeout=180.0)

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
        created = int(time.time())

        try:
            async with client.stream(
                "POST", target_url, json={"question": question, "role": "boss"}
            ) as resp:
                if resp.status_code >= 500:
                    return JSONResponse(
                        status_code=resp.status_code,
                        content={"error": {"message": f"agent {agent_name} error"}},
                    )
                answer_dict = None
                usage = None
                async for line in resp.aiter_lines():
                    if not line.startswith("data:"):
                        continue
                    try:
                        evt = json.loads(line[len("data:"):].strip())
                    except ValueError:
                        continue
                    if evt.get("type") == "final":
                        answer_dict = evt.get("answer")
                        usage = evt.get("usage")
                        break
        except httpx.ConnectError:
            return JSONResponse(
                status_code=502,
                content={"error": {"message": f"agent {agent_name} unreachable", "type": "bad_gateway"}},
            )

        if answer_dict is None:
            return JSONResponse(
                status_code=502,
                content={"error": {"message": "no final event from agent", "type": "bad_gateway"}},
            )

        text = _render_answer(answer_dict)
        u = usage or {}
        usage_out = {
            "prompt_tokens": int(u.get("prompt_tokens", 0)),
            "completion_tokens": int(u.get("completion_tokens", 0)),
            "total_tokens": int(u.get("total_tokens", 0)),
        }
        return JSONResponse(
            status_code=200,
            content={
                "id": f"chatcmpl-{agent_name}",
                "object": "chat.completion",
                "created": created,
                "model": agent_name,
                "choices": [
                    {"index": 0, "message": {"role": "assistant", "content": text}, "finish_reason": "stop"}
                ],
                "usage": usage_out,
            },
        )

    return app


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        build_askbi_shim(
            __import__("os").environ.get("SHIM_TARGET_URL", "http://127.0.0.1:8000/api/ask")
        ),
        host="127.0.0.1",
        port=int(__import__("os").environ.get("SHIM_PORT", "9402")),
    )
