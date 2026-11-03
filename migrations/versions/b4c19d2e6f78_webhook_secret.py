"""webhook secret

Revision ID: b4c19d2e6f78
Revises: f9a37c2d5b84
Create Date: 2026-11-04

P4 安全补强:webhook HMAC 签名密钥(NULL=不签名)。
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "b4c19d2e6f78"
down_revision: Union[str, None] = "f9a37c2d5b84"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("tenants", sa.Column("webhook_secret", sa.String(length=128), nullable=True))


def downgrade() -> None:
    op.drop_column("tenants", "webhook_secret")
