"""引擎与会话工厂(ADR-002:URL 注入切换方言,代码零改动)。"""

from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from app.db.models import Base


def build_engine(db_url: str) -> AsyncEngine:
    kwargs: dict = {"echo": False}
    if db_url.startswith("sqlite"):
        # SQLite 测试口径:单写者 Streams 消费者 + 唯一键兜底,放宽锁等待。
        kwargs["connect_args"] = {"timeout": 10}
    return create_async_engine(db_url, **kwargs)


def build_session_factory(engine: AsyncEngine) -> async_sessionmaker:
    return async_sessionmaker(engine, expire_on_commit=False)


async def init_db(engine: AsyncEngine) -> None:
    """测试/演示环境的建表方式;生产走 Alembic 迁移。"""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
