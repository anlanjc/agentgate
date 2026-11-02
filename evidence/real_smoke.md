# DashScope 真实冒烟(经 agentgate 全链路)

- 时间:2026-11-03 03:25:43 · 上游:https://dashscope.aliyuncs.com/compatible-mode/v1 · 模型:qwen-flash
- 链路:真 HTTP(aiohttp)→ uvicorn 网关 → 鉴权/限流/配额(Lua)→ httpx → DashScope

## 非流式
- 200 OK · 延迟 569ms · usage={'completion_tokens': 25, 'prompt_tokens': 12, 'prompt_tokens_details': {'cached_tokens': 0}, 'total_tokens': 37} · channel=dashscope
- 回复摘要:我是一个能够理解并生成自然语言的AI助手，旨在帮助你解答问题、提供信息和完成各种…

## 流式
- 200 OK · TTFT 130ms · 总延迟 302ms · chunks=11 · usage={'total_tokens': 38, 'completion_tokens': 26, 'prompt_tokens': 12, 'prompt_tokens_details': {'cached_tokens': 0}}

## 判定
- ✅ 真实上游全链路可用:鉴权(Lua 准入)→ SSE 透传 → 记账(账本由后台落库);总花费 ≈ 0.000060 元量级(qwen-turbo)
- 账本对账:2 行,总 token 75(= 网关异步记账落库成功)
- 边界注记:记账 at-least-once 从 enqueue 起算;进程硬停可丢在途 BackgroundTask 账本行(升级路径=接入层 write-ahead),本脚本以优雅停机+轮询规避