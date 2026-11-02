"""P4:成本模型与账单——账本 cost 计算/汇总端点/CSV 导出/未登记价格不猜。"""

import asyncio

from tests.conftest import TENANT_ID, auth_headers


def _body(**extra) -> dict:
    body = {"model": "mock-model", "messages": [{"role": "user", "content": "hi"}]}
    body.update(extra)
    return body


async def _priced_gateway(auth_gateway):
    """注入 mock-model 价目快照(1 元/百万 input,2 元/百万 output)。"""
    from app import billing

    client, app, mock, raw = auth_gateway
    billing.set_price_table({"mock-model": {"input": 1.0, "output": 2.0}})
    return client, app, mock, raw


async def test_ledger_cost_computed_from_price_table(auth_gateway):
    """mock 每请求 18 token(12 prompt + 6 completion):cost = 12/1e6*1 + 6/1e6*2。"""
    client, app, mock, raw = await _priced_gateway(auth_gateway)
    r = await client.post("/v1/chat/completions", json=_body(), headers=auth_headers(raw))
    assert r.status_code == 200
    await app.state.task_queue.flush()
    await asyncio.sleep(0.05)

    from sqlalchemy import select

    from app.db.models import UsageRecord

    async with app.state.session_factory() as db:
        row = (await db.execute(select(UsageRecord).where(UsageRecord.tenant_id == TENANT_ID))).scalar_one()
        # 公式按账本实际拆分验证(不假设 mock 的 prompt/completion 具体配比)
        expected = round(row.prompt_tokens / 1e6 * 1.0 + row.completion_tokens / 1e6 * 2.0, 8)
        assert abs(row.cost - expected) < 1e-9
        assert row.prompt_tokens + row.completion_tokens == row.total_tokens == 18


async def test_unpriced_model_cost_is_null(auth_gateway):
    """未登记价格:cost=NULL(不猜价),账单汇总进 unpriced_tokens 而非 0 元。"""
    from app import billing

    billing.set_price_table({})  # 显式空表(隔离其他用例的快照)
    client, app, mock, raw = auth_gateway
    r = await client.post("/v1/chat/completions", json=_body(), headers=auth_headers(raw))
    assert r.status_code == 200
    await app.state.task_queue.flush()
    await asyncio.sleep(0.05)

    from sqlalchemy import select

    from app.db.models import UsageRecord

    async with app.state.session_factory() as db:
        row = (await db.execute(select(UsageRecord).where(UsageRecord.tenant_id == TENANT_ID))).scalar_one()
        assert row.cost is None


async def test_billing_summary_and_csv_export(auth_gateway):
    """汇总端点:requests/cost 聚合正确;CSV 导出含表头与明细行。"""
    client, app, mock, raw = await _priced_gateway(auth_gateway)
    h = auth_headers(raw)
    for _ in range(2):
        assert (await client.post("/v1/chat/completions", json=_body(), headers=h)).status_code == 200
    await app.state.task_queue.flush()
    await asyncio.sleep(0.05)

    summary = (await client.get(f"/admin/tenants/{TENANT_ID}/billing")).json()
    assert summary["requests"] == 2
    assert summary["unpriced_tokens"] == 0
    # 与账本行两两对账(汇总=明细之和)
    from sqlalchemy import select

    from app.db.models import UsageRecord

    async with app.state.session_factory() as db:
        rows = (await db.execute(select(UsageRecord).where(UsageRecord.tenant_id == TENANT_ID))).scalars().all()
    expected = round(sum(r.cost for r in rows), 8)
    assert len(rows) == 2
    assert abs(summary["total_cost_cny"] - expected) < 1e-9
    assert abs(summary["by_model"]["mock-model"]["cost"] - expected) < 1e-9

    csv_resp = await client.get(f"/admin/tenants/{TENANT_ID}/billing?format=csv")
    assert csv_resp.status_code == 200
    assert csv_resp.headers["content-type"].startswith("text/csv")
    lines = csv_resp.text.strip().splitlines()
    assert lines[0].startswith("request_id,model,channel")
    assert len(lines) == 3  # 表头 + 2 行明细
