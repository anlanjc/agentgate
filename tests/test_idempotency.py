"""P2 hardening:客户端 Idempotency-Key —— 重试不重执行/不重计费/租户域隔离/流式边界。

GW-3(评审 2.0)回归:属主 token + compare-and-delete 释放 + 锁 TTL ≥ 合法执行期。
"""

import asyncio

from tests.conftest import TENANT_ID, auth_headers


def _body(**extra) -> dict:
    body = {"model": "mock-model", "messages": [{"role": "user", "content": "hi"}]}
    body.update(extra)
    return body


async def test_replay_skips_upstream_and_double_billing(auth_gateway):
    """同 Idempotency-Key 重试:第二次回放(x-idempotent-replay),上游不重调,
    账本回放行 token=0(预算/TPM 不重扣)。"""
    client, app, mock, raw = auth_gateway
    h = {**auth_headers(raw), "Idempotency-Key": "retry-safe-1"}

    r1 = await client.post("/v1/chat/completions", json=_body(), headers=h)
    assert r1.status_code == 200
    assert r1.headers.get("x-idempotent-replay") is None

    r2 = await client.post("/v1/chat/completions", json=_body(), headers=h)
    assert r2.status_code == 200
    assert r2.headers.get("x-idempotent-replay") == "true"
    assert r2.json() == r1.json()

    await app.state.task_queue.flush()
    usage = (await client.get(f"/admin/tenants/{TENANT_ID}/usage")).json()
    # 两行账本(首执行 mock 18 token + 回放 replay 0 token),总 billing 仍是 18
    assert usage["requests"] == 2
    assert usage["total_tokens"] == 18
    by_model = usage["by_model"]["mock-model"]
    assert by_model["requests"] == 2 and by_model["total_tokens"] == 18


async def test_in_flight_conflict_returns_409(auth_gateway):
    """首次请求仍在途(锁被占):重复请求 409,不并发执行两次。"""
    from app import idempotency

    client, app, mock, raw = auth_gateway
    sk = idempotency.storage_key(TENANT_ID, "k-test", "in-flight-1")
    acquired, token = await idempotency.acquire(app.state.redis, sk)  # 模拟首次在途
    assert acquired and token
    r = await client.post(
        "/v1/chat/completions", json=_body(),
        headers={**auth_headers(raw), "Idempotency-Key": "in-flight-1"},
    )
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "idempotency_key_conflict"
    await idempotency.release(app.state.redis, sk, token)


async def test_409_conflict_releases_concurrency_slot(auth_gateway):
    """GATE-2(评审 4.0)回归:幂等 409 冲突路径必须释放准入已占的并发槽。

    复现(确定性,零时序依赖):预置"首请求在途" = 直接占 1 个并发槽 + 占幂等锁;
    再带同一 Idempotency-Key 发请求 —— 过准入(conc 1→2 ≤ limit=2)后命中 409。
    修复前:409 raise 直达 errors.py 全局 handler(纯 JSONResponse,无 background),
    准入已占的槽永不释放 —— 每次 409 给 conc:{key_id} +1,ACQUIRE_LUA 每次 acquire
    还刷新 EXPIRE 300s,持续重试(恰是 Idempotency-Key 的文档化用法)逐步侵蚀额度
    直至整 key 429 锁死。对照 Go 版(agentgate-go main.go chatCompletions 统一收尾
    defer + slotHeld 标记):Go 版结构性避开同族缺陷,回归见其 m2_test.go。
    """
    from app import idempotency
    from app.ratelimit import concurrency

    client, app, mock, raw = auth_gateway
    k = (await client.post(
        f"/admin/tenants/{TENANT_ID}/keys",
        json={"name": "gate2-slot", "concurrency_limit": 2},
    )).json()
    h = auth_headers(k["key"])
    key_id = k["key_id"]

    async def conc() -> int:
        v = await app.state.redis.get(f"conc:{key_id}")
        return int(v or 0)

    # 预置"首请求在途":直接占 1 槽 + 占幂等锁(确定性,免时序编排)
    assert await concurrency.acquire(app.state.redis, key_id, 2)
    assert await conc() == 1
    sk = idempotency.storage_key(TENANT_ID, key_id, "gate2-slot-k")
    lock_acquired, lock_token = await idempotency.acquire(app.state.redis, sk)
    assert lock_acquired and lock_token

    # 第二请求:过准入(conc 1→2)→ 幂等锁被占 → 409
    r = await client.post(
        "/v1/chat/completions", json=_body(),
        headers={**h, "Idempotency-Key": "gate2-slot-k"},
    )
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "idempotency_key_conflict"
    # 修复前:泄漏 —— conc 停在 2(真实在途只有 1);修复后:释放回 1
    assert await conc() == 1, "409 冲突路径泄漏并发槽:准入已占的槽未释放"

    # 第三请求(不同幂等键):应可占槽执行 —— 修复前 conc=2 已吃满 limit=2,429 锁死
    r3 = await client.post(
        "/v1/chat/completions", json=_body(),
        headers={**h, "Idempotency-Key": "gate2-slot-k3"},
    )
    assert r3.status_code == 200, f"409 泄漏侵蚀并发额度:第三请求被拒({r3.status_code})"

    # 清理:释放预置槽与锁(r3 的槽由 _account background 自行释放)
    await idempotency.release(app.state.redis, sk, lock_token)
    await concurrency.release(app.state.redis, key_id)


async def test_idempotency_scoped_by_tenant(auth_gateway):
    """作用域=租户+key:另一租户用同一个 Idempotency-Key 是独立请求,不被回放。"""
    client, app, mock, raw_a = auth_gateway
    other = (await client.post("/admin/tenants", json={"name": "idem-other"})).json()
    key_b = (
        await client.post(f"/admin/tenants/{other['id']}/keys", json={"name": "b"})
    ).json()

    h_a = {**auth_headers(raw_a), "Idempotency-Key": "shared-key"}
    h_b = {**auth_headers(key_b["key"]), "Idempotency-Key": "shared-key"}
    r1 = await client.post("/v1/chat/completions", json=_body(), headers=h_a)
    r2 = await client.post("/v1/chat/completions", json=_body(), headers=h_b)
    assert r1.status_code == r2.status_code == 200
    assert r2.headers.get("x-idempotent-replay") is None  # 跨租户不回放


async def test_stream_requests_ignore_idempotency_key(auth_gateway):
    """流式响应体不可重放:带 Idempotency-Key 的流式请求照常执行(诚实边界,留痕告警)。"""
    client, app, mock, raw = auth_gateway
    h = {**auth_headers(raw), "Idempotency-Key": "stream-key"}
    body = _body(stream=True)
    r1 = await client.post("/v1/chat/completions", json=body, headers=h)
    r2 = await client.post("/v1/chat/completions", json=body, headers=h)
    assert r1.status_code == r2.status_code == 200
    assert r2.headers.get("x-idempotent-replay") is None


async def test_release_does_not_delete_taken_over_lock(auth_gateway):
    """GW-3 回归(属主校验):首请求锁 TTL 过期、重试方已接管后,
    首请求的 release 不得删除重试方的锁(compare-and-delete)。
    修复前 release 是无条件 delete —— 首请求收尾恰好删掉重试方的锁,
    第三次重试又能拿锁并发执行,交叉释放滚雪球。"""
    from app import idempotency

    client, app, mock, raw = auth_gateway
    sk = idempotency.storage_key(TENANT_ID, "k-test", "expired-takeover-1")
    ok_a, token_a = await idempotency.acquire(app.state.redis, sk, ttl_ms=50)
    assert ok_a and token_a

    await asyncio.sleep(0.1)  # 首请求的锁过期(模拟长执行期 > TTL)
    ok_b, token_b = await idempotency.acquire(app.state.redis, sk, ttl_ms=60_000)
    assert ok_b and token_b and token_b != token_a  # 重试方接管成功

    owned_a = await idempotency.release(app.state.redis, sk, token_a)
    assert owned_a is False  # 首请求不再持有:据此放弃 store
    assert await app.state.redis.get(f"{sk}:lock") == token_b  # 重试方的锁完好

    owned_b = await idempotency.release(app.state.redis, sk, token_b)
    assert owned_b is True  # 属主本人可释放
    assert await app.state.redis.get(f"{sk}:lock") is None


async def test_release_without_token_is_safe_noop(auth_gateway):
    """GW-3 回归(降级路径):Redis 故障降级 acquire 返回 token=None,
    release 对 None 是安全 no-op(修复前会无条件 delete,可能误删他人锁)。"""
    from app import idempotency

    client, app, mock, raw = auth_gateway
    sk = idempotency.storage_key(TENANT_ID, "k-test", "degraded-1")
    ok, token = await idempotency.acquire(app.state.redis, sk)
    assert ok and token
    # 模拟另一把"别人的锁"在场(降级路径没有 token 可删)
    await app.state.redis.set(f"{sk}:lock", "someone-else")
    assert await idempotency.release(app.state.redis, sk, None) is False
    assert await app.state.redis.get(f"{sk}:lock") == "someone-else"


def test_lock_ttl_default_covers_read_timeout():
    """GW-3 回归(TTL 口径):锁 TTL 缺省必须 ≥ 渠道读超时缺省 ——
    修复前 60s 硬编码短于 300s 合法执行期,重试在首请求执行中就并发进来双计费。"""
    from app.config import Settings

    s = Settings(_env_file=None)
    assert s.idempotency_lock_ttl_seconds >= s.upstream_read_timeout
