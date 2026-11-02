"""usage cost

Revision ID: f9a37c2d5b84
Revises: e6f22c81a903
Create Date: 2026-11-03

P4:账本金额列(元)—— 单价 × token,写入时计算;未登记价格 NULL。
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "f9a37c2d5b84"
down_revision: Union[str, None] = "e6f22c81a903"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("usage_records", sa.Column("cost", sa.Float(), nullable=True))


def downgrade() -> None:
    op.drop_column("usage_records", "cost")
