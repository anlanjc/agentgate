"""hardening: outbox watermark, tenant-scoped ledger idempotency, channel timeouts

Revision ID: 7c1f4a9b2d10
Revises: 3215fe6889f5
Create Date: 2026-11-03

生产审计修复:
1. task_records.published_at —— transactional outbox 发布水位;
2. usage_records(tenant_id, request_id) 唯一 —— 崩溃重投时账本最多一行;
3. usage_records.key_id 可空 —— auth_enabled=False 的纯代理模式也能记账;
4. channels.connect_timeout/read_timeout —— 运行时注册的超时配置重启后不丢;
5. usage_records(tenant_id, created_at) 复合索引 —— 租户时间窗对账。
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "7c1f4a9b2d10"
down_revision: Union[str, None] = "3215fe6889f5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("task_records", sa.Column("published_at", sa.DateTime(timezone=True), nullable=True))
    op.create_index("ix_task_records_published_at", "task_records", ["published_at"], unique=False)

    op.add_column(
        "channels",
        sa.Column("connect_timeout", sa.Float(), nullable=False, server_default="5.0"),
    )
    op.add_column(
        "channels",
        sa.Column("read_timeout", sa.Float(), nullable=False, server_default="300.0"),
    )

    # 旧数据可能已有重复 (tenant_id, request_id):保留最小 id,再上唯一约束。
    op.execute(
        "DELETE FROM usage_records WHERE id NOT IN ("
        "SELECT keep_id FROM ("
        "SELECT MIN(id) AS keep_id FROM usage_records GROUP BY tenant_id, request_id"
        ") AS keep_rows)"
    )
    with op.batch_alter_table("usage_records") as batch:
        batch.alter_column(
            "key_id", existing_type=sa.String(length=32), nullable=True
        )
        batch.create_unique_constraint(
            "uq_usage_tenant_request", ["tenant_id", "request_id"]
        )
    op.create_index(
        "ix_usage_records_tenant_created",
        "usage_records",
        ["tenant_id", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_usage_records_tenant_created", table_name="usage_records")
    with op.batch_alter_table("usage_records") as batch:
        batch.drop_constraint("uq_usage_tenant_request", type_="unique")
        batch.alter_column(
            "key_id", existing_type=sa.String(length=32), nullable=False
        )
    op.drop_column("channels", "read_timeout")
    op.drop_column("channels", "connect_timeout")
    op.drop_index("ix_task_records_published_at", table_name="task_records")
    op.drop_column("task_records", "published_at")
