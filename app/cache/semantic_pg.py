"""语义缓存 pgvector 后端(P4-4):PG KNN 检索替代 Redis 暴力余弦扫描。

规模化路径落地(semantic.py 模块头预留的接口):
- Redis 暴力余弦 O(N) 上限 200 条 → pgvector `<=>`(余弦距离)KNN + HNSW 索引;
- 后端经 `AGENTGATE_SEMANTIC_BACKEND=pgvector` 显式开启(默认 redis —— SQLite
  测试口径/便携 Redis 无 pgvector,双口径纪律不破坏);
- 表不进 SQLAlchemy 模型(vector 类型不跨方言),由 `ensure_table` 幂等建
  (CREATE EXTENSION IF NOT EXISTS vector + HNSW);alembic 迁移带方言守卫,
  SQLite 口径自动跳过;
- 维度必须显式配置(`semantic_pg_vector_dim`),与 embedder 输出一致 —— 不从首条
  数据猜维度,错维度的 HNSW 索引比没有索引更糟;
- 降级语义同 semantic.py:任何 PG 故障当 miss(缓存是优化不是正确性)。
"""

import json

from app.log import get_logger

logger = get_logger("cache.semantic_pg")


async def ensure_table(engine, dim: int) -> None:
    """幂等建扩展/表/索引(启动时调用一次;dim 必须与 embedder 一致)。"""
    if dim <= 0:
        raise ValueError("semantic_pg_vector_dim must be set when backend=pgvector")
    if not isinstance(dim, int):
        raise ValueError("semantic_pg_vector_dim must be an integer")
    from sqlalchemy import text

    # asyncpg 不支持 DDL 绑定参数:dim 已验证为 int,内联安全
    async with engine.begin() as conn:
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
        await conn.execute(text(
            f"CREATE TABLE IF NOT EXISTS semantic_entries ("
            f" id BIGSERIAL PRIMARY KEY,"
            f" tenant_id VARCHAR(32) NOT NULL,"
            f" model VARCHAR(128) NOT NULL,"
            f" ctx VARCHAR(32) NOT NULL,"
            f" question TEXT NOT NULL,"
            f" answer JSONB NOT NULL,"
            f" embedding vector({int(dim)}) NOT NULL,"
            f" created_at TIMESTAMPTZ NOT NULL DEFAULT now()"
            f")"
        ))
        await conn.execute(text(
            "CREATE INDEX IF NOT EXISTS ix_semantic_tenant_ctx "
            "ON semantic_entries (tenant_id, model, ctx)"
        ))
        # HNSW:余弦距离,取舍 = 构建慢/查询快/内存换精度(mvp 规模 hnsw 足够)
        await conn.execute(text(
            "CREATE INDEX IF NOT EXISTS ix_semantic_embedding_hnsw "
            "ON semantic_entries USING hnsw (embedding vector_cosine_ops)"
        ))
    logger.info("semantic_pg_table_ready", dim=dim)


def _vec_literal(vec: list[float]) -> str:
    return "[" + ",".join(f"{v:.8f}" for v in vec) + "]"


async def store_pg(engine, embedder, tenant_id: str, model: str, ctx: str,
                   question: str, body: dict, ttl: int, dim: int) -> None:
    """写入(向量列不设 TTL —— 过期由查询侧按 created_at 过滤,见 search_pg)。"""
    if ttl <= 0:
        return
    from sqlalchemy import text

    vec = await embedder.embed(question)
    if len(vec) != dim:
        logger.error("semantic_pg_dim_mismatch", got=len(vec), want=dim)
        return
    try:
        async with engine.begin() as conn:
            await conn.execute(text(
                "INSERT INTO semantic_entries (tenant_id, model, ctx, question, answer, embedding)"
                " VALUES (:t, :m, :c, :q, CAST(:a AS jsonb), CAST(:v AS vector))"
            ).bindparams(t=tenant_id, m=model, c=ctx, q=question,
                         a=json.dumps(body, ensure_ascii=False), v=_vec_literal(vec)))
    except Exception as exc:  # noqa: BLE001 降级:缓存写失败不阻塞请求
        logger.warning("semantic_pg_store_degraded", error=str(exc))


async def search_pg(engine, embedder, tenant_id: str, model: str, ctx: str,
                    question: str, threshold: float, max_scan: int = 200,
                    ttl: int = 0, dim: int = 128) -> dict | None:
    """KNN 召回:1 - 余弦距离 = 相似度;阈值以上取最优;ttl>0 时过滤过期行。"""
    from sqlalchemy import text

    vec = await embedder.embed(question)
    if len(vec) != dim:
        logger.error("semantic_pg_dim_mismatch", got=len(vec), want=dim)
        return None
    try:
        async with engine.connect() as conn:
            rows = (await conn.execute(text(
                "SELECT answer, 1 - (embedding <=> CAST(:v AS vector)) AS score"
                " FROM semantic_entries"
                " WHERE tenant_id = :t AND model = :m AND ctx = :c"
                " AND (:ttl <= 0 OR created_at > now() - make_interval(secs => :ttl))"
                " ORDER BY embedding <=> CAST(:v AS vector) LIMIT :lim"
            ).bindparams(t=tenant_id, m=model, c=ctx, v=_vec_literal(vec),
                         ttl=ttl, lim=max_scan))).all()
    except Exception as exc:  # noqa: BLE001 降级:PG 故障当 miss
        logger.warning("semantic_pg_search_degraded", error=str(exc))
        return None
    if not rows:
        return None
    answer, score = rows[0]
    if score < threshold:
        return None
    return {"score": round(float(score), 4), "answer": answer}
