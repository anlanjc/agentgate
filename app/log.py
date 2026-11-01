"""结构化日志:structlog JSON 输出 + request_id 贯穿(contextvars)。"""

import logging
import sys
from contextvars import ContextVar

import structlog

request_id_var: ContextVar[str] = ContextVar("request_id", default="-")


def configure_logging(level: str = "INFO") -> None:
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(ensure_ascii=False),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        cache_logger_on_first_use=True,
    )
    logging.basicConfig(stream=sys.stderr, level=getattr(logging, level.upper(), logging.INFO))


def get_logger(name: str = "agentgate"):
    return structlog.get_logger(name)


def bind_request_id(request_id: str):
    """绑定 request_id 到当前上下文(structlog contextvars + 独立 ContextVar)。"""
    structlog.contextvars.bind_contextvars(request_id=request_id)
    return request_id_var.set(request_id)


def current_request_id() -> str:
    return request_id_var.get()
