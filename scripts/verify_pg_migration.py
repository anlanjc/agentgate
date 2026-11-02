"""PG 迁移验证:内嵌 PostgreSQL 16(pgserver)上跑 Alembic 迁移并验表。

证据:EVIDENCE §① —— 迁移在真 PG 上 upgrade head 成功,5 张表齐全。
运行:.venv/Scripts/python scripts/verify_pg_migration.py
注意:全程同步(pgserver.psql + alembic command),不可包在 asyncio.run 内
     —— alembic 的 env.py 自带 asyncio.run,嵌套会炸。
"""

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)


def main() -> None:
    import pgserver

    db = pgserver.get_server(str(ROOT / ".pgdata"))
    version = db.psql("select version();").splitlines()[2].split(",")[0].strip()
    pg_uri = db.get_uri()  # postgresql://postgres:@host:port/postgres
    sa_url = pg_uri.replace("postgresql://", "postgresql+asyncpg://", 1)
    print(f"[1] {version} @ {pg_uri.split('@')[1]}")

    # 幂等:pytest 的 create_all 可能已建表,先清掉再走迁移
    db.psql(
        "DROP TABLE IF EXISTS usage_records, task_records, virtual_keys, channels, tenants CASCADE;"
        "DROP TABLE IF EXISTS alembic_version CASCADE;"
    )

    # Alembic 编程式迁移(env.py 读 AGENTGATE_DB_URL;同步调用)
    os.environ["AGENTGATE_DB_URL"] = sa_url
    from alembic import command
    from alembic.config import Config

    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(ROOT / "migrations"))
    cfg.set_main_option("sqlalchemy.url", sa_url)
    command.upgrade(cfg, "head")
    print("[2] alembic upgrade head 完成")

    # 验表(psql 同步;默认对齐输出无分隔符,按行 strip 匹配表名)
    out = db.psql("select tablename from pg_tables where schemaname='public' order by 1;")
    tables = {line.strip() for line in out.splitlines()}
    expected = {"channels", "task_records", "tenants", "usage_records", "virtual_keys"}
    missing = expected - tables
    assert not missing, f"缺表: {missing}(实际输出: {sorted(tables)[:12]})"
    print(f"[3] PG 表验证通过: {sorted(expected)}")

    # 验 7c1f4a9b2d10 迁移产物(outbox 水位/账本幂等/渠道超时)
    def _col(table: str, col: str) -> str:
        return db.psql(
            "select is_nullable from information_schema.columns "
            f"where table_name='{table}' and column_name='{col}';"
        )

    published = db.psql(
        "select count(*) from information_schema.columns "
        "where table_name='task_records' and column_name='published_at';"
    )
    assert published.splitlines()[2].strip() == "1", "task_records.published_at 缺失"
    timeouts = db.psql(
        "select count(*) from information_schema.columns "
        "where table_name='channels' and column_name in ('connect_timeout','read_timeout');"
    )
    assert timeouts.splitlines()[2].strip() == "2", "channels 超时列缺失"
    key_nullable = _col("usage_records", "key_id").splitlines()[2].strip()
    assert key_nullable == "YES", f"usage_records.key_id 应可空(纯代理模式),实际 {key_nullable}"
    uq = db.psql(
        "select count(*) from information_schema.table_constraints "
        "where table_name='usage_records' and constraint_name='uq_usage_tenant_request';"
    )
    assert uq.splitlines()[2].strip() == "1", "uq_usage_tenant_request 唯一约束缺失"
    print("[4] 7c1f4a9b2d10 迁移产物验证通过:published_at/超时列/key_id 可空/唯一约束")

    # 幂等:再次 upgrade head 应为无操作(no-op)
    command.upgrade(cfg, "head")
    print("[5] alembic upgrade head 幂等重跑通过")
    print("✅ 迁移在真 PostgreSQL 上验证通过(含 hardening 迁移)")


if __name__ == "__main__":
    main()
