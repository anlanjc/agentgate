"""网关业务错误:统一错误信封(区别于上游错误的透传语义)。"""

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse


class GatewayError(Exception):
    def __init__(self, status: int, message: str, err_type: str, code: str, retry_after_s: int | None = None):
        self.status = status
        self.message = message
        self.err_type = err_type
        self.code = code
        self.retry_after_s = retry_after_s
        super().__init__(message)


async def gateway_error_handler(request: Request, exc: GatewayError) -> JSONResponse:
    from app.log import current_request_id

    headers = {"Retry-After": str(exc.retry_after_s)} if exc.retry_after_s else None
    return JSONResponse(
        status_code=exc.status,
        content={
            "error": {
                "message": exc.message,
                "type": exc.err_type,
                "code": exc.code,
                "request_id": current_request_id(),
            }
        },
        headers=headers,
    )


async def validation_error_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    """GW-11:请求校验错误走统一错误信封 —— 修复前 FastAPI 默认 422 + detail 数组,
    与同网关 401/403/429/402/503 的 error 信封并存两套契约,客户端要写两种解析。
    状态码选 400 对齐 OpenAI(无效请求参数即 400 invalid_request_error)。
    """
    from app.log import current_request_id

    def _fmt(err: dict) -> str:
        loc = ".".join(str(p) for p in err.get("loc", ()) if p != "body")
        return f"{loc or 'request'}: {err.get('msg', 'invalid')}"

    detail = "; ".join(_fmt(err) for err in exc.errors()[:5])
    return JSONResponse(
        status_code=400,
        content={
            "error": {
                "message": f"invalid request body: {detail}",
                "type": "invalid_request_error",
                "code": "invalid_request_body",
                "request_id": current_request_id(),
            }
        },
    )
