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
