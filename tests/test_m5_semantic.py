"""M5+ 验收:语义缓存(HashEmbedder 词面口径)—— 改写命中 / 阈值不中 / 租户隔离。

诚实口径:HashEmbedder 是字符 bigram 词面相似(测试确定性);生产换 DashScopeEmbedder
(协议相同)才是真语义 —— 两者都在 app/cache/semantic.py 里有明确标注。
"""

import httpx
import pytest

from app.cache.semantic import HashEmbedder, _cosine
from tests.conftest import auth_headers


def _body(**extra) -> dict:
    # 语义缓存同样要求显式 temperature=0(缺省 1.0 不是确定性请求)。
    body = {
        "model": "mock-model",
        "messages": [{"role": "user", "content": "上海公积金贷款额度是多少"}],
        "temperature": 0,
    }
    body.update(extra)
    return body


@pytest.fixture
def sem_settings(auth_settings):
    auth_settings.semantic_cache_enabled = True
    auth_settings.semantic_cache_threshold = 0.80
    auth_settings.semantic_cache_ttl = 600
    return auth_settings


@pytest.fixture
async def sem_gateway(sem_settings, redis_client):
    from app.api.main import create_app
    from app.auth.keys import generate_key
    from app.db.models import Tenant, VirtualKey
    from app.db.session import init_db
    from app.upstream.client import UpstreamClient
    from tests.mock_upstream import build_mock_upstream

    mock_app = build_mock_upstream()
    upstream = UpstreamClient(
        sem_settings.upstream_base_url,
        sem_settings.upstream_api_key,
        client=httpx.AsyncClient(
            transport=httpx.ASGITransport(app=mock_app), base_url=sem_settings.upstream_base_url
        ),
    )
    app = create_app(settings=sem_settings, upstream=upstream, redis_client=redis_client)
    await init_db(app.state.engine)
    raw, key_hash, prefix = generate_key()
    async with app.state.session_factory() as db:
        db.add(Tenant(id="t-s1", name="s1"))
        db.add(Tenant(id="t-s2", name="s2"))
        db.add(
            VirtualKey(
                id="k-s1", tenant_id="t-s1", key_hash=key_hash, key_prefix=prefix,
                rpm_limit=10_000, tpm_limit=10_000_000,
            )
        )
        await db.commit()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://gateway"
    ) as client:
        yield client, app, raw, (
            await client.post(
                "/admin/tenants/t-s2/keys", json={"name": "s2"}
            )
        ).json()["key"]
    await upstream.aclose()
    await app.state.task_queue.stop()


async def test_paraphrase_hits_semantic_cache(sem_gateway):
    """改写问题(高词面重叠)→ SEMANTIC_HIT,answer 与原答案一致。"""
    client, app, raw, _ = sem_gateway
    h = auth_headers(raw)
    r1 = await client.post("/v1/chat/completions", json=_body(), headers=h)
    assert r1.status_code == 200 and "x-cache" not in r1.headers  # 首问回源

    r2 = await client.post(
        "/v1/chat/completions",
        json=_body(messages=[{"role": "user", "content": "上海公积金贷款额度能贷多少"}]),
        headers=h,
    )
    assert r2.status_code == 200
    assert r2.headers["x-cache"] == "SEMANTIC_HIT"
    assert float(r2.headers["x-cache-score"]) >= 0.80
    assert r2.json()["choices"][0]["message"]["content"] == r1.json()["choices"][0]["message"]["content"]


async def test_unrelated_question_misses(sem_gateway):
    """无关问题(词面不重叠)→ 低于阈值,回源。"""
    client, app, raw, _ = sem_gateway
    h = auth_headers(raw)
    await client.post("/v1/chat/completions", json=_body(), headers=h)
    r2 = await client.post(
        "/v1/chat/completions",
        json=_body(messages=[{"role": "user", "content": "zzz qqq www"}]),
        headers=h,
    )
    assert r2.status_code == 200
    assert "SEMANTIC_HIT" != r2.headers.get("x-cache", "")


async def test_semantic_cache_tenant_isolation(sem_gateway):
    """租户隔离同样适用于语义缓存:租户 B 的改写不吃租户 A 的缓存。"""
    client, app, raw_a, raw_b = sem_gateway
    h_a, h_b = auth_headers(raw_a), auth_headers(raw_b)
    await client.post("/v1/chat/completions", json=_body(), headers=h_a)
    r2 = await client.post(
        "/v1/chat/completions",
        json=_body(messages=[{"role": "user", "content": "上海公积金贷款额度能贷多少"}]),
        headers=h_b,
    )
    assert r2.status_code == 200
    assert "SEMANTIC_HIT" != r2.headers.get("x-cache", "")


async def test_semantic_cache_context_is_isolated(sem_gateway):
    """同问题、不同 system prompt 不得命中:缓存域必须绑定上下文指纹。"""
    client, app, raw, _ = sem_gateway
    h = auth_headers(raw)
    r1 = await client.post(
        "/v1/chat/completions",
        json=_body(messages=[
            {"role": "system", "content": "你是公积金客服"},
            {"role": "user", "content": "上海公积金贷款额度是多少"},
        ]),
        headers=h,
    )
    assert r1.status_code == 200
    r2 = await client.post(
        "/v1/chat/completions",
        json=_body(messages=[
            {"role": "system", "content": "你是医疗客服"},
            {"role": "user", "content": "上海公积金贷款额度是多少"},
        ]),
        headers=h,
    )
    assert r2.status_code == 200
    assert "SEMANTIC_HIT" != r2.headers.get("x-cache", "")


def test_cosine_identical_and_orthogonal():
    """余弦实现自检:同向=1,正交=0。"""
    assert _cosine([1.0, 0.0], [1.0, 0.0]) == 1.0
    assert _cosine([1.0, 0.0], [0.0, 1.0]) == 0.0


async def test_hash_embedder_deterministic():
    e = HashEmbedder()
    v1 = await e.embed("公积金贷款")
    v2 = await e.embed("公积金贷款")
    assert v1 == v2 and abs(sum(abs(x) for x in v1)) > 0


# ---- GW-2 回归（评审 2.0 high）：embed/后端故障必须降级为 miss，不得 500+漏记账 ----

async def test_semantic_search_embed_failure_degrades_to_upstream(sem_gateway, monkeypatch):
    """embed 抛错 → 请求仍 200 回源，且账本正常落行（曾穿透主链路致 500 漏记账）。"""
    from sqlalchemy import select

    from app.db.models import UsageRecord

    client, app, raw, _ = sem_gateway
    h = auth_headers(raw)

    async def _boom(*args, **kwargs):
        raise RuntimeError("embed upstream down")

    monkeypatch.setattr(app.state.embedder, "embed", _boom)

    r = await client.post("/v1/chat/completions", json=_body(), headers=h)
    assert r.status_code == 200
    assert "choices" in r.json()  # 正常回源答案，不是网关错误信封

    # 记账走 BackgroundTask，响应返回后台任务才落库——轮询等齐再断言（AG-2 同款教训）
    import asyncio

    rows = []
    async with app.state.session_factory() as db:
        for _ in range(20):
            rows = (
                await db.execute(select(UsageRecord).where(UsageRecord.tenant_id == "t-s1"))
            ).scalars().all()
            if rows:
                break
            await asyncio.sleep(0.1)
    assert len(rows) == 1  # 漏记账回归锁：请求完成后账本恰有 1 行


async def test_semantic_store_embed_failure_does_not_break_response(sem_gateway, monkeypatch):
    """响应已到手后入库失败 → 响应原样返回 200（只丢缓存写入）。"""
    client, app, raw, _ = sem_gateway
    h = auth_headers(raw)

    async def _boom(*args, **kwargs):
        raise RuntimeError("embed upstream down")

    # search 放行（能走到 store 说明先 miss），只炸 store 里的 embed
    real_embed = app.state.embedder.embed
    state_box = {"phase": 0}

    async def _boom_second(*args, **kwargs):
        state_box["phase"] += 1
        if state_box["phase"] >= 2:
            raise RuntimeError("embed upstream down")
        return await real_embed(*args, **kwargs)

    monkeypatch.setattr(app.state.embedder, "embed", _boom_second)

    r = await client.post("/v1/chat/completions", json=_body(), headers=h)
    assert r.status_code == 200
    assert "choices" in r.json()
    assert "x-cache" not in r.headers  # 入库失败即无缓存命中可言
