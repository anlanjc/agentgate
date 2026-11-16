"""P4-4:语义缓存 pgvector 后端 —— PG 16 + vector 0.6.2 KNN 检索(真库口径)。

双口径纪律:pgvector 只在 PG 口径跑(SQLite 无该扩展,skip 是诚实标注不是省事);
Redis 暴力余弦口径的测试见 test_m5_semantic.py,两后端走同一门控与租户隔离语义。
"""

import os

import pytest

from tests.conftest import auth_headers

pytestmark = pytest.mark.skipif(
    not os.environ.get("AGENTGATE_TEST_PG"),
    reason="pgvector 后端只在 PG 口径验证(SQLite 无 vector 扩展;AGENTGATE_TEST_PG=1 启用)",
)


def _body(q: str, **extra) -> dict:
    body = {
        "model": "mock-model",
        "messages": [{"role": "user", "content": q}],
        "temperature": 0,
    }
    body.update(extra)
    return body


async def _enable_pgvector(app):
    from app.cache import semantic_pg
    from app.cache.semantic import HashEmbedder

    cfg = app.state.settings.model_copy(update={
        "semantic_cache_enabled": True,
        "semantic_backend": "pgvector",
        "semantic_pg_vector_dim": 128,  # HashEmbedder 默认维度
        "semantic_cache_threshold": 0.75,  # 词面 embedder 下取宽松阈值保证确定性
        "semantic_cache_ttl": 600,
    })
    app.state.settings = cfg
    # embedder 在 create_app 时按开关装配:测试后开开关需手动补(与生产装配一致)
    if getattr(app.state, "embedder", None) is None:
        app.state.embedder = HashEmbedder()
    await semantic_pg.ensure_table(app.state.engine, 128)


async def test_pgvector_knn_roundtrip(auth_gateway):
    """写入 → 相近问题 KNN 召回 SEMANTIC_HIT;无关问题 MISS。"""
    client, app, mock, raw = auth_gateway
    await _enable_pgvector(app)
    h = auth_headers(raw)

    r1 = await client.post(
        "/v1/chat/completions", json=_body("杭州社保缴费基数上限是多少"), headers=h
    )
    assert r1.status_code == 200

    # 相近问题:精确缓存键不同(请求体不同),走语义召回
    r2 = await client.post(
        "/v1/chat/completions", json=_body("杭州社保缴费基数上限是多少钱"), headers=h
    )
    assert r2.status_code == 200
    assert r2.headers.get("x-cache") == "SEMANTIC_HIT", r2.headers
    assert float(r2.headers["x-cache-score"]) >= 0.75

    # 无关问题:向量距离远,不命中
    r3 = await client.post(
        "/v1/chat/completions", json=_body("今天天气怎么样啊朋友"), headers=h
    )
    assert r3.headers.get("x-cache") != "SEMANTIC_HIT"


async def test_pgvector_tenant_isolation(auth_gateway):
    """与 Redis 后端同一安全边界:租户 B 不召回租户 A 的答案。"""
    client, app, mock, raw_a = auth_gateway
    await _enable_pgvector(app)
    other = (await client.post("/admin/tenants", json={"name": "pgv-b"})).json()
    key_b = (await client.post(f"/admin/tenants/{other['id']}/keys", json={"name": "b"})).json()

    h_a = auth_headers(raw_a)
    await client.post(
        "/v1/chat/completions", json=_body("上海公积金贷款额度规则"), headers=h_a
    )
    r = await client.post(
        "/v1/chat/completions",
        json=_body("上海公积金贷款额度规则"),
        headers=auth_headers(key_b["key"]),
    )
    assert r.status_code == 200
    assert r.headers.get("x-cache") != "SEMANTIC_HIT"  # 跨租户不召回


def _vec_literal(vec: list[float]) -> str:
    return "[" + ",".join(f"{v:.8f}" for v in vec) + "]"


async def _count_entries(engine) -> int:
    from sqlalchemy import text

    async with engine.connect() as conn:
        return (await conn.execute(text("SELECT count(*) FROM semantic_entries"))).scalar_one()


async def test_pgvector_expired_rows_are_purged(auth_gateway, monkeypatch):
    """GATE-5(评审 4.0)回归:pgvector 语义缓存过期行必须有淘汰纪律。

    修复前每条成功响应 INSERT 一行且无唯一约束无淘汰(TTL 仅查询侧按 created_at
    过滤,注释自述'向量列不设 TTL —— 过期由查询侧按 created_at 过滤')—— 过期行
    连同 HNSW 索引永久留存,表与索引无界增长最终拖垮 PG,同问题重复插入还叠加
    重复行。修复后 cleanup_expired 按 created_at < now-ttl 批量 DELETE,store_pg
    消费点节流触发(默认 60s 周期,测试 patch 到 0 立即触发)。"""
    from sqlalchemy import text

    from app.cache import semantic_pg

    client, app, mock, raw = auth_gateway
    await _enable_pgvector(app)  # semantic_cache_ttl=600
    engine = app.state.engine

    # 3 行过期(2h 前,同问题重复插重复行——修复前无唯一约束的真实形态)+ 1 行新鲜
    async with engine.begin() as conn:
        for i in range(3):
            await conn.execute(text(
                "INSERT INTO semantic_entries (tenant_id, model, ctx, question, answer,"
                " embedding, created_at) VALUES ('t-old','mock-model','ctx','old q','{}'::jsonb,"
                " CAST(:v AS vector), now() - make_interval(secs => :age))"
            ).bindparams(v=_vec_literal([0.1] * 128), age=7200))
    await semantic_pg.store_pg(
        engine, app.state.embedder, "t-test", "mock-model", "ctx", "fresh q",
        {"answer": "ok"}, 600, dim=128,
    )
    assert await _count_entries(engine) == 4

    # 直接清理:恰好删掉 3 行过期,新鲜行保留
    deleted = await semantic_pg.cleanup_expired(engine, ttl=600)
    assert deleted == 3
    assert await _count_entries(engine) == 1

    # 消费点节流钩子:patch 周期到 0,再插 1 行过期后 store_pg 触发清理
    monkeypatch.setattr(semantic_pg, "CLEANUP_INTERVAL_SECONDS", 0.0)
    async with engine.begin() as conn:
        await conn.execute(text(
            "INSERT INTO semantic_entries (tenant_id, model, ctx, question, answer,"
            " embedding, created_at) VALUES ('t-old','mock-model','ctx','stale q','{}'::jsonb,"
            " CAST(:v AS vector), now() - make_interval(secs => :age))"
        ).bindparams(v=_vec_literal([0.1] * 128), age=7200))
    assert await _count_entries(engine) == 2
    await semantic_pg.store_pg(
        engine, app.state.embedder, "t-test", "mock-model", "ctx", "fresh q 2",
        {"answer": "ok"}, 600, dim=128,
    )
    assert await _count_entries(engine) == 2, "store_pg 节流清理未生效:过期行残留"
