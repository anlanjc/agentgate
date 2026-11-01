"""网关业务错误:统一错误信封(区别于上游错误的透传语义)。"""

from fastapi import Request
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
