"""P2 hardening 小件三连:限流响应头 / 每 key 并发上限 / 缓存单飞锁。"""

import asyncio

from tests.conftest import TENANT_ID, auth_headers


def _body(**extra) -> dict:
    body = {"model": "mock-model", "messages": [{"role": "user", "content": "hi"}]}
    body.update(extra)
    return body


async def test_rate_limit_headers_present(auth_gateway):
    """成功响应带标准限流头;429 带 Retry-After(既有)+ 头仍可读。"""
    client, app, mock, raw = auth_gateway
    r = await client.post("/v1/chat/completions", json=_body(), headers=auth_headers(raw))
    assert r.status_code == 200
    assert r.headers["x-ratelimit-limit-requests"] == "10000"
    assert int(r.headers["x-ratelimit-remaining-requests"]) >= 0
    assert int(r.headers["x-ratelimit-reset-requests"]) >= 1


async def test_concurrency_cap_blocks_second_in_flight(auth_gateway):
    """并发上限=1:第二个在途请求 429 concurrency_exceeded;完成后释放,后续可进。"""
    client, app, mock, raw = auth_gateway
    k = (await client.post(
        f"/admin/tenants/{TENANT_ID}/keys",
        json={"name": "cc", "concurrency_limit": 1},
    )).json()
    h = auth_headers(k["key"])
    r1 = await client.post("/v1/chat/completions", json=_body(), headers=h)
    assert r1.status_code == 200
    # 上一请求完成后槽已释放(_account 释放点):顺序请求畅通
    r2 = await client.post("/v1/chat/completions", json=_body(), headers=h)
    assert r2.status_code == 200

    # 并发两发(mock 近零延迟可能都挤过闸 —— 真实占槽断言在下方)
    results = await asyncio.gather(
        client.post("/v1/chat/completions", json=_body(), headers=h),
        client.post("/v1/chat/completions", json=_body(), headers=h),
    )
    codes = sorted(r.status_code for r in results)
    # 竞态窗口内至少一次被闸(mock 近零延迟可能都挤过);确切 429 断言用真实占槽
    if codes == [200, 429]:
        r429 = next(r for r in results if r.status_code == 429)
        assert r429.json()["error"]["code"] == "concurrency_exceeded"

    from app.ratelimit import concurrency

    assert await concurrency.acquire(app.state.redis, k["key_id"], 1)
    r3 = await client.post("/v1/chat/completions", json=_body(), headers=h)
    assert r3.status_code == 429
    assert r3.json()["error"]["code"] == "concurrency_exceeded"
    await concurrency.release(app.state.redis, k["key_id"])
    r4 = await client.post("/v1/chat/completions", json=_body(), headers=h)
    assert r4.status_code == 200


async def test_singleflight_lock_waiter_gets_filled_cache(auth_gateway):
    """单飞锁:持锁期间并发 miss 短轮询;持锁者填缓存后,等待者直接回缓存不调上游。"""
    from app.cache import response as rc

    client, app, mock, raw = auth_gateway
    # 打开精确缓存(默认关)
    cfg = app.state.settings.model_copy(
        update={"response_cache_enabled": True, "response_cache_ttl": 60}
    )
    app.state.settings = cfg

    body = _body(temperature=0)
    h = auth_headers(raw)

    # 第一发:正常执行并填缓存;SCAN 出网关真实派生的缓存键(不手搓 digest ——
    # upstream_payload 的键集是内部实现,手搓必然对不上)
    r1 = await client.post("/v1/chat/completions", json=body, headers=h)
    assert r1.status_code == 200 and r1.headers.get("x-cache") == "MISS"
    keys = [k async for k in app.state.redis.scan_iter(match="resp:*", count=100)]
    rkey = next(k for k in keys if not str(k).endswith(":lock"))
    await app.state.redis.delete(rkey)  # 清掉缓存值;锁单独预占

    # 预占锁模拟"另一个相同请求正在上游执行中";随后填缓存+放锁
    assert await rc.try_acquire(app.state.redis, rkey)
    task = asyncio.create_task(client.post("/v1/chat/completions", json=body, headers=h))
    await asyncio.sleep(0.2)  # 让等待者进入轮询
    cached_body = {
        "id": "chatcmpl-prefilled", "object": "chat.completion",
        "choices": [{"index": 0, "message": {"role": "assistant", "content": "cached"}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
    }
    await rc.put(app.state.redis, rkey, cached_body, ttl=60)
    await rc.release(app.state.redis, rkey)

    r = await task
    assert r.status_code == 200
    assert r.headers.get("x-cache") == "HIT"  # 等待者拿到的是填好的缓存,不是第二发上游
    assert r.json()["id"] == "chatcmpl-prefilled"
