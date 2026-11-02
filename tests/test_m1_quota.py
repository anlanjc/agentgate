"""M1 验收:配额与记账 —— 预算 402、TPM 窗口 429、账本对账、并发原子性、缓存重建。"""

import asyncio

from app.quota import service as quota_service
from tests.conftest import TENANT_ID, auth_headers


def _body(**extra) -> dict:
    body = {"model": "mock-model", "messages": [{"role": "user", "content": "hi"}]}
    body.update(extra)
    return body


async def test_budget_exhaustion_402(auth_gateway):
    """budget=18:首请求恰好 18 token 过,第二发预算预检拒绝(402,18>=18)。"""
    client, app, mock, _ = auth_gateway
    created = (
        await client.post(
            f"/admin/tenants/{TENANT_ID}/keys", json={"name": "budgeted", "budget_tokens": 18}
        )
    ).json()
    h = auth_headers(created["key"])
    r1 = await client.post("/v1/chat/completions", json=_body(), headers=h)
    assert r1.status_code == 200
    assert await quota_service.spent_tokens(app.state.redis, created["key_id"]) == 18
    r2 = await client.post("/v1/chat/completions", json=_body(), headers=h)
    assert r2.status_code == 402
    assert r2.json()["error"]["code"] == "budget_exceeded"


async def test_tpm_window_429(auth_gateway):
    """tpm=50:18×3=54 后,第 4 发窗口预检拒绝(429 tpm_exceeded)。"""
    client, app, mock, _ = auth_gateway
    created = (
        await client.post(f"/admin/tenants/{TENANT_ID}/keys", json={"name": "tpm", "tpm_limit": 50})
    ).json()
    h = auth_headers(created["key"])
    codes = []
    last = None
    for _ in range(4):
        last = await client.post("/v1/chat/completions", json=_body(), headers=h)
        codes.append(last.status_code)
    assert codes == [200, 200, 200, 429]
    assert last.json()["error"]["code"] == "tpm_exceeded"


async def test_ledger_nonstream_and_stream(auth_gateway):
    """非流式与流式(含 usage)各记一条,账本对账一致。"""
    client, app, mock, raw = auth_gateway
    h = auth_headers(raw)
    r1 = await client.post("/v1/chat/completions", json=_body(), headers=h)
    r2 = await client.post(
        "/v1/chat/completions", json=_body(stream=True, stream_options={"include_usage": True}), headers=h
    )
    assert r1.status_code == r2.status_code == 200
    await app.state.task_queue.flush()
    usage = (await client.get(f"/admin/tenants/{TENANT_ID}/usage")).json()
    assert usage["requests"] == 2
    assert usage["total_tokens"] == 36  # mock 每 次 18(流式由 usage 提取器从最终 chunk 解出)


async def test_stream_without_include_usage_is_still_billed(auth_gateway):
    """客户端不传 include_usage 时,网关也必须拿到 usage 记账(强制注入)。"""
    client, app, mock, raw = auth_gateway
    h = auth_headers(raw)
    r = await client.post("/v1/chat/completions", json=_body(stream=True), headers=h)
    assert r.status_code == 200
    await app.state.task_queue.flush()
    usage = (await client.get(f"/admin/tenants/{TENANT_ID}/usage")).json()
    assert usage["requests"] == 1
    assert usage["total_tokens"] == 18  # 不是 0:客户端没要 usage 也计费


async def test_concurrent_accounting_atomic(auth_gateway):
    """并发 100:TPM 计数器恰好 1800(原子 INCRBY 零丢失),账本恰好 100 行。"""
    client, app, mock, raw = auth_gateway
    h = auth_headers(raw)
    responses = await asyncio.gather(
        *[client.post("/v1/chat/completions", json=_body(), headers=h) for _ in range(100)]
    )
    assert all(r.status_code == 200 for r in responses)
    window = await quota_service.tpm_window_get(app.state.redis, "k-test")
    assert window == 1800, f"原子性破约:window={window}"
    await app.state.task_queue.flush()
    usage = (await client.get(f"/admin/tenants/{TENANT_ID}/usage")).json()
    assert usage["requests"] == 100
    assert usage["total_tokens"] == 1800


async def test_same_request_id_across_tenants_does_not_collide(auth_gateway):
    """request_id 可由客户端伪造:幂等域必须按租户隔离,不能吞掉别家账本。"""
    client, app, mock, raw_a = auth_gateway
    other = (await client.post("/admin/tenants", json={"name": "other-tenant"})).json()
    key_b = (
        await client.post(f"/admin/tenants/{other['id']}/keys", json={"name": "b"})
    ).json()
    rid = "forged-shared-request-id"
    r1 = await client.post(
        "/v1/chat/completions", json=_body(), headers={**auth_headers(raw_a), "X-Request-Id": rid}
    )
    r2 = await client.post(
        "/v1/chat/completions", json=_body(), headers={**auth_headers(key_b["key"]), "X-Request-Id": rid}
    )
    assert r1.status_code == r2.status_code == 200
    await app.state.task_queue.flush()
    ua = (await client.get(f"/admin/tenants/{TENANT_ID}/usage")).json()
    ub = (await client.get(f"/admin/tenants/{other['id']}/usage")).json()
    assert ua["requests"] == 1 and ub["requests"] == 1


async def test_duplicate_ledger_delivery_writes_once(auth_gateway):
    """模拟 handler 写库后崩溃重投:账本唯一键保证只落一行。"""
    client, app, mock, raw = auth_gateway
    from app.db.models import UsageRecord
    from app.queue.webhooks import write_usage

    payload = {
        "request_id": "dup-ledger-1", "key_id": "k-test", "tenant_id": TENANT_ID,
        "model": "mock-model", "prompt_tokens": 1, "completion_tokens": 1,
        "total_tokens": 2, "ttft_ms": None, "latency_ms": 1, "status": 200, "channel": "mock",
    }
    a = await write_usage(payload, app.state.session_factory)
    b = await write_usage(payload, app.state.session_factory)
    assert a["written"] is True and b["duplicate"] is True
    async with app.state.session_factory() as db:
        from sqlalchemy import func, select

        n = (
            await db.execute(
                select(func.count(UsageRecord.id)).where(
                    UsageRecord.tenant_id == TENANT_ID,
                    UsageRecord.request_id == "dup-ledger-1",
                )
            )
        ).scalar_one()
    assert n == 1


async def test_budget_rebuild_from_db(auth_gateway):
    """Redis 计数丢失后从 PG 账本重建 —— 缓存与账本一致性路径。"""
    client, app, mock, raw = auth_gateway
    h = auth_headers(raw)
    for _ in range(3):
        await client.post("/v1/chat/completions", json=_body(), headers=h)
    await app.state.task_queue.flush()

    await app.state.redis.delete(quota_service.budget_key("k-test"))
    async with app.state.session_factory() as db:
        rebuilt = await quota_service.rebuild_from_db(app.state.redis, db, "k-test")
    assert rebuilt == 54
    assert await quota_service.spent_tokens(app.state.redis, "k-test") == 54
