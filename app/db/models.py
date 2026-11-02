"""数据模型(SQLAlchemy 2.0 typed,方言中立——ADR-002)。

红线:只用通用类型(String/Integer/BigInteger/Boolean/DateTime/JSON/Float),
禁止 PG 专属特性(JSONB/ARRAY/方言函数),保证 aiosqlite 与 asyncpg 双驱动可跑。
"""

from datetime import UTC, datetime

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utcnow() -> datetime:
    return datetime.now(UTC)


class Base(DeclarativeBase):
    pass


class Tenant(Base):
    __tablename__ = "tenants"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    name: Mapped[str] = mapped_column(String(128), unique=True)
    webhook_url: Mapped[str | None] = mapped_column(String(512), nullable=True)  # 任务完成回调
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class VirtualKey(Base):
    """virtual key:只存哈希不存原文(泄露库不泄露可用 key);prefix 供管理端辨认。"""

    __tablename__ = "virtual_keys"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(ForeignKey("tenants.id"), index=True)
    key_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    key_prefix: Mapped[str] = mapped_column(String(12))
    name: Mapped[str] = mapped_column(String(128), default="")
    model_whitelist: Mapped[list | None] = mapped_column(JSON, nullable=True)  # None=不限
    rpm_limit: Mapped[int] = mapped_column(Integer, default=60)
    tpm_limit: Mapped[int] = mapped_column(Integer, default=100_000)
    budget_tokens: Mapped[int | None] = mapped_column(BigInteger, nullable=True)  # None=不限
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class UsageRecord(Base):
    """用量账本(只追加,不更新)——计费对账的最终事实来源。

    幂等边界:``(tenant_id, request_id)`` 唯一。消费端重投/崩溃恢复可能重复执行
    handler,唯一约束保证账本最多落一行;request_id 由客户端可传,因此必须按租户
    隔离,不能做全局唯一(否则可被伪造 ID 阻塞其他租户记账)。
    """

    __tablename__ = "usage_records"
    __table_args__ = (
        UniqueConstraint("tenant_id", "request_id", name="uq_usage_tenant_request"),
        Index("ix_usage_records_tenant_created", "tenant_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    request_id: Mapped[str] = mapped_column(String(64), index=True)
    # auth_enabled=False 的纯代理模式没有 virtual key:账本仍保留,key_id 为空。
    key_id: Mapped[str | None] = mapped_column(
        ForeignKey("virtual_keys.id"), index=True, nullable=True
    )
    tenant_id: Mapped[str] = mapped_column(String(32), index=True)
    model: Mapped[str] = mapped_column(String(128))
    prompt_tokens: Mapped[int] = mapped_column(Integer, default=0)
    completion_tokens: Mapped[int] = mapped_column(Integer, default=0)
    total_tokens: Mapped[int] = mapped_column(Integer, default=0)
    ttft_ms: Mapped[float | None] = mapped_column(Float, nullable=True)
    latency_ms: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[int] = mapped_column(Integer, default=200)  # 上游状态码或网关映射码
    channel: Mapped[str] = mapped_column(String(64), default="")  # 命中的上游渠道(可观测)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)


class Channel(Base):
    """上游渠道注册表(M2 故障转移的实体)。"""

    __tablename__ = "channels"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    name: Mapped[str] = mapped_column(String(128), unique=True)
    base_url: Mapped[str] = mapped_column(String(512))
    api_key: Mapped[str] = mapped_column(String(256), default="")
    models: Mapped[list] = mapped_column(JSON, default=list)
    connect_timeout: Mapped[float] = mapped_column(Float, default=5.0)
    read_timeout: Mapped[float] = mapped_column(Float, default=300.0)
    weight: Mapped[int] = mapped_column(Integer, default=1)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)


class TaskRecord(Base):
    """异步任务账本:幂等键 UNIQUE 是"不重"的最终防线(at-least-once 投递 + 幂等消费)。

    published_at 是 **transactional outbox** 的发布水位:
    - 先提交 task_records(意图),再 XADD(投递);XADD 失败时 published_at 为空;
    - sweeper 定期扫描 published_at IS NULL AND status='pending' 重新投递;
    - 重复投递由消费端状态机 + 账本唯一键吞掉,因此"至少一次"不会变成"至少两次副作用"。
    """

    __tablename__ = "task_records"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    idempotency_key: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    type: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    status: Mapped[str] = mapped_column(String(16), default="pending")  # pending/processing/done/dead
    result: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)
