"""FastAPI 应用工厂。

M0: 协议代理 + 健康检查;M1 起在 lifespan 挂接 DB/Redis,鉴权/限流以依赖注入方式进入路由。
create_app 支持注入 settings 与 upstream(测试用 mock 上游,不打真实网络)。
"""

from collections import deque
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app import __version__
from app.api.admin import router as admin_router
from app.api.v1_chat import router as v1_router
from app.config import Settings, get_settings
from app.db.session import build_engine, build_session_factory
from app.errors import GatewayError, gateway_error_handler
from app.log import configure_logging, get_logger
from app.middleware import RequestContextMiddleware
from app.quota.writer import UsageWriter
from app.upstream.client import UpstreamClient

logger = get_logger("app")


def create_app(
    settings: Settings | None = None,
    upstream: UpstreamClient | None = None,
    redis_client=None,
) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level)

    # state 同步初始化(引擎/客户端均为惰性建连,不触碰网络):
    # ASGITransport 测试路径不跑 lifespan,state 必须在工厂内就绪;lifespan 只负责优雅关闭。
    @asynccontextmanager
    async def lifespan(app_: FastAPI):
        logger.info("app_started", version=__version__, upstream=settings.upstream_name)
        yield
        await app_.state.writer.stop()
        await app_.state.redis.aclose()
        await app_.state.engine.dispose()
        logger.info("app_stopped")

    import redis.asyncio as aredis

    engine = build_engine(settings.db_url)
    app_ = FastAPI(title=settings.app_name, version=__version__, lifespan=lifespan)
    app_.state.settings = settings
    app_.state.engine = engine
    app_.state.session_factory = build_session_factory(engine)
    app_.state.redis = redis_client or aredis.from_url(
        settings.redis_url, decode_responses=True, protocol=2
    )
    app_.state.writer = UsageWriter(app_.state.session_factory)
    app_.state.upstream = upstream or UpstreamClient(
        settings.upstream_base_url,
        settings.upstream_api_key,
        connect_timeout=settings.upstream_connect_timeout,
        read_timeout=settings.upstream_read_timeout,
        trust_env=settings.llm_trust_env,
    )
    app_.state.ttft_samples = deque(maxlen=1000)  # M0 观测占位,M3 换 Prometheus 直方图

    # 语义缓存 embedder(默认关;有 EMBED key 用真 embedding,否则 HashEmbedder 词面口径)
    if settings.semantic_cache_enabled:
        if settings.embed_api_key:
            from app.cache.semantic import DashScopeEmbedder

            app_.state.embedder = DashScopeEmbedder(
                settings.embed_api_key,
                base_url=settings.embed_api_base or "https://dashscope.aliyuncs.com/compatible-mode/v1",
                trust_env=settings.llm_trust_env,
            )
        else:
            from app.cache.semantic import HashEmbedder

            app_.state.embedder = HashEmbedder()

    # M2:渠道注册表(默认渠道 = settings 指向的上游)+ 任务队列(webhook 回调)
    from app.queue.streams import TaskQueue
    from app.queue.webhooks import webhook_handler
    from app.router.failover import ChannelRouter

    channel_router = ChannelRouter(app_.state.redis)
    channel_router.register(settings.upstream_name, app_.state.upstream, settings.models)
    app_.state.channel_router = channel_router
    app_.state.task_queue = TaskQueue(app_.state.redis, app_.state.session_factory, webhook_handler)

    app_.add_middleware(RequestContextMiddleware)
    app_.add_exception_handler(GatewayError, gateway_error_handler)
    app_.include_router(v1_router)
    app_.include_router(admin_router)

    @app_.get("/health")
    async def health() -> dict:
        return {"status": "ok", "version": __version__}

    @app_.get("/metrics")
    async def metrics_endpoint():
        from prometheus_client import generate_latest
        from starlette.responses import Response

        return Response(
            content=generate_latest(), media_type="text/plain; version=0.0.4; charset=utf-8"
        )

    @app_.get("/admin/web")
    async def admin_web():
        from pathlib import Path

        from starlette.responses import FileResponse

        page = Path(__file__).resolve().parents[2] / "app" / "web" / "index.html"
        return FileResponse(page, media_type="text/html")

    @app_.get("/playground")
    async def playground():
        from pathlib import Path

        from starlette.responses import FileResponse

        page = Path(__file__).resolve().parents[2] / "app" / "web" / "playground.html"
        return FileResponse(page, media_type="text/html")

    return app_


app = create_app()
