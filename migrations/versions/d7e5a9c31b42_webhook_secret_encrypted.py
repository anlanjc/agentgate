"""webhook secret encrypted at rest (widening for ciphertext)

Revision ID: d7e5a9c31b42
Revises: b4c19d2e6f78
Create Date: 2026-11-09

GW-8(评审 2.0):webhook_secret 入库走 Fernet 加密(与渠道密钥同纪律)。
密文带 `enc:v1:` 前缀,128 字符明文密钥的密文约 243 字符 —— 列宽 128 → 384。
SQLite 不校验 varchar 长度(aiosqlite 直存),PG 需要 ALTER TYPE。
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "d7e5a9c31b42"
down_revision: str | None = "b4c19d2e6f78"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.alter_column(
        "tenants", "webhook_secret",
        existing_type=sa.String(length=128), type_=sa.String(length=384),
    )


def downgrade() -> None:
    # 回滚有截断风险:仅当存量行都是旧明文(≤128)时安全;密文行需先解密回来。
    op.alter_column(
        "tenants", "webhook_secret",
        existing_type=sa.String(length=384), type_=sa.String(length=128),
    )
