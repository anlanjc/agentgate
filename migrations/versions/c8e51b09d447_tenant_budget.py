"""tenant budget

Revision ID: c8e51b09d447
Revises: a3d94f1c7b21
Create Date: 2026-11-03

P2 hardening:租户级 token 预算(NULL=不限)—— key 级预算的兜底,
堵"一个租户签多把 key 绕过单 key 预算"。
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "c8e51b09d447"
down_revision: Union[str, None] = "a3d94f1c7b21"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("tenants", sa.Column("budget_tokens", sa.BigInteger(), nullable=True))


def downgrade() -> None:
    op.drop_column("tenants", "budget_tokens")
