"""语义缓存:问题向量化 + 余弦相似度召回(精确缓存的补充层)。

架构(诚实边界全部写在这里):
- **Embedder 可插拔**:HashEmbedder(字符 n-gram 哈希,零依赖,测试/演示用——它是词面
  相似不是真语义)+ DashScopeEmbedder(text-embedding-v3,OpenAI 兼容 /embeddings,
  生产口径);实现 `embed(text) -> list[float]` 协议即可替换(pgvector/RediSearch 同理)。
- **存储与检索**:Redis 5.0 无向量检索模块——存 ZSET 索引 + 暴力余弦 O(N) 扫描;
  演示/小流量规模成立,规模化路径 = RediSearch KNN 或 pgvector(接口不变)。
- **租户隔离**:向量索引键含 tenant_id,与精确缓存同一安全边界。
- **门控**:与精确缓存一致(temperature≤0 非流式无 tools),阈值默认 0.92。
"""

import hashlib
import json
import math
import time
from typing import Protocol

from redis.exceptions import RedisError

from app.config import Settings
from app.log import get_logger

logger = get_logger("cache.semantic")

# 降级语义同 response.py:缓存是优化不是正确性,Redis 故障当 miss(可观测,不阻塞请求)。


class Embedder(Protocol):
    async def embed(self, text: str) -> list[float]: ...


class HashEmbedder:
    """字符 bigram 哈希到固定维度 —— 确定性、零网络、测试友好。

    诚实声明:这是词面相似度(共享字 n-gram),不是真正的语义相似;
    生产换 DashScopeEmbedder 即可(协议相同)。
    """

    def __init__(self, dim: int = 128) -> None:
        self.dim = dim

    async def embed(self, text: str) -> list[float]:
        vec = [0.0] * self.dim
        t = text.lower().replace(" ", "")
        for i in range(len(t) - 1):
            gram = t[i : i + 2]
            h = int(hashlib.md5(gram.encode()).hexdigest(), 16)
            vec[h % self.dim] += 1.0
        norm = math.sqrt(sum(v * v for v in vec)) or 1.0
        return [v / norm for v in vec]


class DashScopeEmbedder:
    """生产 embedder:OpenAI 兼容 /embeddings 端点(text-embedding-v3)。"""

    def __init__(self, api_key: str, base_url: str = "https://dashscope.aliyuncs.com/compatible-mode/v1",
                 model: str = "text-embedding-v3", trust_env: bool = False) -> None:
        self.api_key = api_key
        self.base_url = base_url
        self.model = model
        self.trust_env = trust_env

    async def embed(self, text: str) -> list[float]:
        import httpx

        async with httpx.AsyncClient(trust_env=self.trust_env, timeout=15.0) as c:
            r = await c.post(
                f"{self.base_url}/embeddings",
                headers={"Authorization": f"Bearer {self.api_key}"},
                json={"model": self.model, "input": text},
            )
            r.raise_for_status()
            return r.json()["data"][0]["embedding"]


def _cosine(a: list[float], b: list[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a)) or 1.0
    nb = math.sqrt(sum(y * y for y in b)) or 1.0
    return dot / (na * nb)


def semantic_enabled(settings: Settings, req) -> bool:
    return (
        settings.semantic_cache_enabled
        and not req.stream
        and not req.tools
        and req.temperature is not None
        and req.temperature <= 0
    )


def context_digest(model: str, payload: dict) -> str:
    """把"语义可比域"压成短指纹:模型 + system/历史消息 + 采样参数 + user。

    只把最后一条 user 消息当向量查询;其余上下文必须完全相同才允许语义命中。
    否则同一个问题在不同 system prompt 下会拿到别人的答案(串答案/提示词越权)。
    """
    messages = payload.get("messages") or []
    last_user_idx = -1
    for i, msg in enumerate(messages):
        if msg.get("role") == "user":
            last_user_idx = i
    context_messages = [m for i, m in enumerate(messages) if i != last_user_idx]
    context = {
        k: v for k, v in payload.items()
        if k not in ("messages", "stream", "stream_options")
    }
    context["messages"] = context_messages
    context["model"] = model
    raw = json.dumps(context, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _index_key(tenant_id: str, model: str, ctx: str) -> str:
    return f"semidx:{tenant_id}:{model}:{ctx}"


def _entry_key(tenant_id: str, model: str, ctx: str, digest: str) -> str:
    return f"sementry:{tenant_id}:{model}:{ctx}:{digest}"


def _digest(vec: list[float]) -> str:
    return hashlib.sha256(json.dumps([round(v, 6) for v in vec]).encode()).hexdigest()[:24]


async def search(
    redis,
    embedder: Embedder,
    tenant_id: str,
    model: str,
    ctx: str,
    question: str,
    threshold: float,
    max_scan: int = 200,
) -> dict | None:
    """余弦扫描召回;命中返回缓存条目(answer/usage),未命中 None。"""
    vec = await embedder.embed(question)
    idx = _index_key(tenant_id, model, ctx)
    try:
        members = await redis.zrange(idx, 0, max_scan - 1, desc=True)  # 时间倒序近似最近优先
    except RedisError as exc:
        logger.warning("semantic_cache_read_degraded", error=str(exc))
        return None
    if not members:
        return None
    # 一次 MGET 取候选向量,而不是 N 次串行 GET(N=200 时是 200 个 RTT)。
    keys = [
        _entry_key(
            tenant_id, model, ctx,
            m.decode() if isinstance(m, bytes) else str(m),
        )
        for m in members
    ]
    try:
        raws = await redis.mget(keys)
    except RedisError as exc:
        logger.warning("semantic_cache_read_degraded", error=str(exc))
        return None
    best: tuple[float, dict] | None = None
    for raw in raws:
        if raw is None:
            continue
        try:
            entry = json.loads(raw)
        except (ValueError, TypeError):
            continue
        score = _cosine(vec, entry.get("vector", []))
        if best is None or score > best[0]:
            best = (score, entry)
    if best is not None and best[0] >= threshold:
        return {"score": round(best[0], 4), **best[1]}
    return None


async def store(
    redis,
    embedder: Embedder,
    tenant_id: str,
    model: str,
    ctx: str,
    question: str,
    body: dict,
    ttl: int,
) -> None:
    if ttl <= 0:
        return
    vec = await embedder.embed(question)
    d = _digest(vec)
    idx = _index_key(tenant_id, model, ctx)
    try:
        pipe = redis.pipeline(transaction=False)
        pipe.set(
            _entry_key(tenant_id, model, ctx, d),
            json.dumps({"vector": vec, "question": question, "answer": body}, ensure_ascii=False),
            ex=ttl,
        )
        pipe.zadd(idx, {d: time.time()})
        pipe.zremrangebyrank(idx, 0, -201)  # 索引上限 200 条(演示规模;规模化见模块头)
        pipe.expire(idx, ttl)
        await pipe.execute()
    except RedisError as exc:
        logger.warning("semantic_cache_write_degraded", error=str(exc))
