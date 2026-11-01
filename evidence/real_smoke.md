# DashScope 真实冒烟(经 agentgate 全链路)

- 时间:2026-11-02 00:31:05 · 上游:https://dashscope.aliyuncs.com/compatible-mode/v1 · 模型:qwen-flash
- 链路:真 HTTP(aiohttp)→ uvicorn 网关 → 鉴权/限流/配额(Lua)→ httpx → DashScope

## 非流式
- 200 OK · 延迟 780ms · usage={'completion_tokens': 19, 'prompt_tokens': 12, 'prompt_tokens_details': {'cached_tokens': 0}, 'total_tokens': 31} · channel=dashscope
- 回复摘要:我是一个能够回答问题、创作文字、编程，还能提供各种帮助的AI助手。…

## 流式
- 200 OK · TTFT 133ms · 总延迟 303ms · chunks=11 · usage={'total_tokens': 35, 'completion_tokens': 23, 'prompt_tokens': 12, 'prompt_tokens_details': {'cached_tokens': 0}}

## 判定
- ✅ 真实上游全链路可用:鉴权(Lua 准入)→ SSE 透传 → 记账(账本由后台落库);总花费 ≈ 0.000053 元量级(qwen-turbo)
- 账本对账:2 行,总 token 66(= 网关异步记账落库成功)