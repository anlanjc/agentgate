"""P2 hardening:租户级预算 —— 多 key 绕预算被租户计数器堵死。"""

import asyncio

from tests.conftest import TENANT_ID, auth_headers


async def _wait_tenant_spent(app, minimum: int) -> None:
    """等后台记账把租户计数器推到 minimum(计量在响应后的 BackgroundTask,
    下一发请求的预检与其存在天然竞态 —— 等待是测试确定性手段,非产品语义)。"""
    from app.quota.service import tenant_budget_key

    for _ in range(100):
        v = await app.state.redis.get(tenant_budget_key(TENANT_ID))
        if v and int(v) >= minimum:
            return
        await asyncio.sleep(0.05)


def _body(**extra) -> dict:
    body = {"model": "mock-model", "messages": [{"role": "user", "content": "hi"}]}
    body.update(extra)
    return body


async def test_tenant_budget_blocks_all_keys_of_tenant(auth_gateway):
    """预算 30(每请求 18):同租户第 2 把新 key 首请求即 402 —— 换 key 绕不过。"""
    client, app, mock, raw_a = auth_gateway

    # 设置租户预算=30(mock 每请求 18 token),计数器从账本校准(当前 0)
    r = await client.post(f"/admin/tenants/{TENANT_ID}/budget", json={"budget_tokens": 30})
    assert r.status_code == 200 and r.json()["spent_tokens"] == 0

    # 预算设置后签发的新 key 立即带预算读数;旧 key(auth 缓存 60s 窗口)不受影响
    k1 = (await client.post(f"/admin/tenants/{TENANT_ID}/keys", json={"name": "b1"})).json()
    k2 = (await client.post(f"/admin/tenants/{TENANT_ID}/keys", json={"name": "b2"})).json()

    r1 = await client.post("/v1/chat/completions", json=_body(), headers=auth_headers(k1["key"]))
    assert r1.status_code == 200  # spent 0 < 30 → 放行;记账后租户 spent=18
    r2 = await client.post("/v1/chat/completions", json=_body(), headers=auth_headers(k1["key"]))
    assert r2.status_code == 200  # 18 < 30 → 放行;记账后 36 ≥ 30
    await _wait_tenant_spent(app, 36)
    r3 = await client.post("/v1/chat/completions", json=_body(), headers=auth_headers(k1["key"]))
    assert r3.status_code == 402
    assert r3.json()["error"]["code"] == "tenant_budget_exceeded"

    # 关键断言:换一把新 key 同租户 → 仍然 402(单 key 预算防不住的绕法)
    r4 = await client.post("/v1/chat/completions", json=_body(), headers=auth_headers(k2["key"]))
    assert r4.status_code == 402
    assert r4.json()["error"]["code"] == "tenant_budget_exceeded"


async def test_tenant_budget_unset_is_unlimited(auth_gateway):
    """未设预算:行为与从前一致(只有 key 级预算)。"""
    client, app, mock, raw = auth_gateway
    for _ in range(3):
        r = await client.post("/v1/chat/completions", json=_body(), headers=auth_headers(raw))
        assert r.status_code == 200


async def test_budget_set_calibrates_counter_from_ledger(auth_gateway):
    """设置预算时计数器从账本重建(先消耗 18,后设预算 20,校准读数=18)。

    注:预算设置前签发的 key 其 auth 缓存里预算读数是 None(60s 窗口,文档语义),
    所以用设置后签发的新 key 验证拦截 —— 新 key miss 缓存,立即带预算。
    """
    client, app, mock, raw = auth_gateway
    k_old = (await client.post(f"/admin/tenants/{TENANT_ID}/keys", json={"name": "c"})).json()
    r1 = await client.post("/v1/chat/completions", json=_body(), headers=auth_headers(k_old["key"]))
    assert r1.status_code == 200
    await _wait_tenant_spent(app, 18)
    await app.state.task_queue.flush()  # 计数器到 18 只说明已入队;flush 确保账本行落库

    r = await client.post(f"/admin/tenants/{TENANT_ID}/budget", json={"budget_tokens": 20})
    assert r.json()["spent_tokens"] == 18  # 校准:计数器=账本事实

    k_new = (await client.post(f"/admin/tenants/{TENANT_ID}/keys", json={"name": "c2"})).json()
    r2 = await client.post("/v1/chat/completions", json=_body(), headers=auth_headers(k_new["key"]))
    assert r2.status_code == 200  # 18 < 20 → 放行,记账后 36
    await _wait_tenant_spent(app, 36)
    r3 = await client.post("/v1/chat/completions", json=_body(), headers=auth_headers(k_new["key"]))
    assert r3.status_code == 402
