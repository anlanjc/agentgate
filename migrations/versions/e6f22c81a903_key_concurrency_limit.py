"""per-key concurrency limit

Revision ID: e6f22c81a903
Revises: c8e51b09d447
Create Date: 2026-11-03

P2 hardening:每 key 并发上限(NULL=不限)—— RPM/TPM 之外的第三道闸。
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "e6f22c81a903"
down_revision: Union[str, None] = "c8e51b09d447"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "virtual_keys", sa.Column("concurrency_limit", sa.Integer(), nullable=True)
    )


def downgrade() -> None:
    op.drop_column("virtual_keys", "concurrency_limit")
