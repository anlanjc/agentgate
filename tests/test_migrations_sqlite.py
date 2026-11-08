"""GW-15(评审 3.0)回归:迁移链必须双方言可跑。

此前 CI 只在 PG 服务上跑 alembic upgrade head;SQLite 口径走 create_all 从不
执行迁移 —— 本仓没有任何一条路径验证过 SQLite 方言的迁移链,而 d7e5a9c31b42
的裸 alter_column 对 SQLite 必抛 NotImplementedError(迁移链断头)。

本测试补上这条路径:临时 SQLite 上编程式跑完整迁移链到 head,并钉住产物。
注意必须用同步测试:alembic env.py 自带 asyncio.run,不可包在运行中的事件循环里。
"""

import sqlite3
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]


def _alembic_config(db_url: str):
    from alembic.config import Config

    cfg = Config(str(_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(_ROOT / "migrations"))
    cfg.set_main_option("sqlalchemy.url", db_url)
    return cfg


def test_sqlite_migration_chain_upgrades_head(tmp_path, monkeypatch):
    """SQLite 方言全链迁移:upgrade head 成功、列宽产物正确、幂等重跑通过。
    修复前升到 d7e5a9c31b42 即 NotImplementedError(batch 模式修复)。"""
    from alembic import command

    db_file = tmp_path / "mig.db"
    db_url = f"sqlite+aiosqlite:///{db_file}"
    monkeypatch.setenv("AGENTGATE_DB_URL", db_url)  # env.py 读此变量

    cfg = _alembic_config(db_url)
    command.upgrade(cfg, "head")

    conn = sqlite3.connect(db_file)
    try:
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert {"tenants", "virtual_keys", "usage_records", "task_records",
                "channels", "audit_logs", "alembic_version"} <= tables
        cols = {r[1]: r[2] for r in conn.execute("PRAGMA table_info(tenants)").fetchall()}
        assert cols["webhook_secret"] == "VARCHAR(384)"  # GW-8 迁移产物(batch 模式重建)
        version = conn.execute("SELECT version_num FROM alembic_version").fetchone()[0]
        assert version == "d7e5a9c31b42"
    finally:
        conn.close()

    # 幂等:重复 upgrade 到 head 不炸
    command.upgrade(cfg, "head")


def test_sqlite_migration_downgrade_roundtrip(tmp_path, monkeypatch):
    """SQLite 方言 downgrade d7e5a9c31b42(batch 模式)再 upgrade 回来,链路可逆。"""
    from alembic import command

    db_file = tmp_path / "mig2.db"
    db_url = f"sqlite+aiosqlite:///{db_file}"
    monkeypatch.setenv("AGENTGATE_DB_URL", db_url)

    cfg = _alembic_config(db_url)
    command.upgrade(cfg, "head")
    command.downgrade(cfg, "b4c19d2e6f78")  # 回退 GW-8 迁移(batch downgrade)

    conn = sqlite3.connect(db_file)
    try:
        cols = {r[1]: r[2] for r in conn.execute("PRAGMA table_info(tenants)").fetchall()}
        assert cols["webhook_secret"] == "VARCHAR(128)"
    finally:
        conn.close()

    command.upgrade(cfg, "head")  # 再前进,链路完整可逆
