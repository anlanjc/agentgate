"""请求上下文中间件:纯 ASGI 实现。

为什么不用 BaseHTTPMiddleware:LiteLLM 官方基准显示其框架级开销显著——
单 worker no-op 端点从 ~3.6k RPS(BaseHTTPMiddleware)提升到 ~6.5k RPS(纯 ASGI 中间件),
P50 21ms→13ms(https://docs.litellm.ai/blog/fastapi-middleware-performance)。
网关是逐字节转发路径,中间件必须零缓冲。
"""

import re
import time
import uuid

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.log import bind_request_id, current_request_id, get_logger

logger = get_logger("http")

# GW-1 修复(评审 2.0):客户端 X-Request-Id 曾无校验直入账本——超长/控制字符 ID
# 使 PG 账本行写入失败被静默吞掉。校验=可见 ASCII ≤128(对齐 W3C traceparent parent-id
# 与常见反向代理约束),非法即弃用换服务端生成 ID(响应头回真实 ID,客户端不受影响)。
_REQUEST_ID_RE = re.compile(r"^[\x21-\x7e]{1,128}$")


def _valid_request_id(value: str) -> bool:
    return bool(_REQUEST_ID_RE.fullmatch(value))


class RequestContextMiddleware:
    """request_id 注入(继承或生成)→ contextvars 绑定 → 响应头回写 → 访问日志(含耗时)。"""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        req_id = ""
        for key, value in scope.get("headers", []):
            if key == b"x-request-id":
                req_id = value.decode("latin-1", errors="replace")
                break
        if not _valid_request_id(req_id):
            req_id = uuid.uuid4().hex
        bind_request_id(req_id)

        t0 = time.perf_counter()
        status_holder = {"status": 0}

        async def send_wrapper(message: Message) -> None:
            if message["type"] == "http.response.start":
                status_holder["status"] = message["status"]
                MutableHeaders(scope=message).append("x-request-id", req_id)
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            logger.info(
                "http_request",
                method=scope.get("method", ""),
                path=scope.get("path", ""),
                status=status_holder["status"],
                duration_ms=round((time.perf_counter() - t0) * 1000, 2),
                request_id=current_request_id(),
            )
