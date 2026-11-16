"""生态闭环演示 · mock OpenAI 兼容上游(独立进程,JSON + SSE 双形态)。

定位:替换 askbi 的真实 LLM(DeepSeek 等)做链路与计费演示 —— mock only 零现金,
回答质量无意义,只验证 askbi→agentgate→mock 的调用链、网关记账与降级语义。

节点识别:askbi 多 Agent 链路每个节点用不同 system 提示词调 LLM(app/agents/nodes.py),
system prompt 中含高区分度标记词;mock 按标记词返回**该节点契约允许的最小合法输出**,
让链路走通到最终结构化答案(而不是全程降级拒答)。识别词与 askbi 节点的对应:
- "意图澄清器" → clarifier:返回 verdict=clear(放行主链路);
- "NL2DSL" → sql_agent:返回电商数据集的合法 DSL(metric=revenue 为白名单 id,
  维度 channel/month 也是白名单 id —— 语义层校验在 askbi 侧,dsl.py);
- "数据解读器" → insight:返回**不含任何数字**的叙述(质检对数字一致性有硬校验,
  mock 不知道真实查询结果,不编数字 —— 与 askbi mock 同款纪律);
- "质检员" → qa:返回 passed=true(叙述无数字,确定性校验必过);
- 其余(直连网关的普通请求) → 通用聊天回复。

运行:python scripts/ecosystem_mock_upstream.py --port 8101
"""

from __future__ import annotations

import argparse
import json
import time

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

_CREATED = int(time.time())
_SEQ = {"n": 0}

# 电商数据集(askbi 默认)的合法 DSL 片段 —— 白名单 id 实读自 askbi app/data/
# metrics.py(revenue)与 schema.py(channel/month 维度),不猜标识符。
_DSL_BREAKDOWN_CHANNEL = {
    "intent": "breakdown", "metric": "revenue", "dimensions": ["channel"],
    "filters": [], "time": {"mode": "last_n_months", "n": 1},
    "order_by": {"metric": "revenue", "direction": "desc"},
}
_DSL_TREND_MONTH = {
    "intent": "trend", "metric": "revenue", "dimensions": ["month"],
    "filters": [], "time": {"mode": "last_n_months", "n": 3},
    "order_by": {"metric": "revenue", "direction": "desc"},
}
_INSIGHT_NODIGITS = (
    "查询已成功执行,数据表与图表展示了各分组的销售额构成,排名依据销售额降序排列。"
    "具体数值请以表格数据为准,如需进一步拆分到其他维度可以继续追问。"
)


def _usage(content: str, system: str, user: str) -> dict:
    """确定性 usage(与 askbi OpenAICompatibleClient 的 TokenUsage 消费端对齐)。"""
    prompt = 48 + (len(system) + len(user)) // 8
    completion = 64 + len(content) // 4
    return {
        "prompt_tokens": prompt,
        "completion_tokens": completion,
        "total_tokens": prompt + completion,
    }


def _reply_content(body: dict) -> str:
    """按 system 标记词分发到节点契约的最小合法输出(识别逻辑见模块 docstring)。"""
    messages = body.get("messages") or []
    system = messages[0].get("content", "") if messages else ""
    user = messages[-1].get("content", "") if len(messages) > 1 else ""
    if "意图澄清器" in system:
        return json.dumps(
            {"verdict": "clear", "reason": "指标与时间范围明确,可直接查询",
             "clarify_question": "", "options": []}, ensure_ascii=False)
    if "NL2DSL" in system:
        # 用户 payload 是 askbi 侧 JSON 序列化的问题上下文,按问题词形选 DSL
        question = ""
        try:
            question = str(json.loads(user).get("question", ""))
        except Exception:  # noqa: S110 payload 形态不可枚举,退默认 DSL
            pass
        if "趋势" in question or "最近" in question:
            return json.dumps(_DSL_TREND_MONTH, ensure_ascii=False)
        return json.dumps(_DSL_BREAKDOWN_CHANNEL, ensure_ascii=False)
    if "数据解读器" in system:
        return _INSIGHT_NODIGITS  # 铁律:不编数字(质检有数字一致性硬校验)
    if "质检员" in system:
        return json.dumps({"passed": True, "issues": []}, ensure_ascii=False)
    return "mock reply: 链路贯通(askbi→agentgate→mock,零现金演示口径)。"


def build_app() -> FastAPI:
    app = FastAPI(title="eco-loop mock upstream")

    @app.post("/v1/chat/completions")
    async def chat_completions(request: Request):
        body = await request.json()
        model = body.get("model", "demo-chat")
        _SEQ["n"] += 1
        cid = f"chatcmpl-eco-{_SEQ['n']:03d}"
        content = _reply_content(body)
        usage = _usage(
            content,
            str((body.get("messages") or [{}])[0].get("content") or ""),
            str((body.get("messages") or [{}, {}])[-1].get("content") or ""),
        )

        if not body.get("stream"):
            return JSONResponse(
                status_code=200,
                content={
                    "id": cid, "object": "chat.completion", "created": _CREATED,
                    "model": model,
                    "choices": [{"index": 0, "message": {"role": "assistant", "content": content},
                                 "finish_reason": "stop"}],
                    "usage": usage,
                },
            )

        include_usage = bool((body.get("stream_options") or {}).get("include_usage"))
        pieces = [content[i:i + 8] for i in range(0, len(content), 8)] or [""]

        async def sse():
            def chunk(delta: dict, finish: str | None = None, u: dict | None = None) -> bytes:
                data: dict = {
                    "id": cid, "object": "chat.completion.chunk", "created": _CREATED,
                    "model": model,
                    "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
                }
                if u is not None:
                    data["usage"] = u
                return f"data: {json.dumps(data, ensure_ascii=False)}\n\n".encode()

            yield chunk({"role": "assistant"})
            for piece in pieces:
                yield chunk({"content": piece})
            yield chunk({}, finish="stop", u=usage if include_usage else None)
            yield b"data: [DONE]\n\n"

        return StreamingResponse(sse(), media_type="text/event-stream")

    @app.get("/v1/models")
    async def models():
        return {"object": "list", "data": [{"id": "demo-chat", "object": "model"}]}

    @app.get("/health")
    async def health():
        return {"status": "ok", "mock": "eco-loop"}

    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8101)
    args = parser.parse_args()
    uvicorn.run(build_app(), host=args.host, port=args.port, log_level="warning")
