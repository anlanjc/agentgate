"""M2 验收:Redis Streams 任务队列(幂等入队 / 至少一次+幂等消费 / 重试与死信 / 崩溃恢复 / webhook e2e)。"""

import asyncio
import json
import socket

import httpx
import pytest
import uvicorn

from app.db.session import init_db
from app.queue.streams import DLQ_STREAM, GROUP, TASK_STREAM, TaskQueue
from tests.mock_upstream import build_mock_upstream


def _body(**extra) -> dict:
    body = {"model": "mock-model", "messages": [{"role": "user", "content": "hi"}]}
    body.update(extra)
    return body


@pytest.fixture
async def queue_env(tmp_path, redis_client, pg_url):
    from app.db.session import build_engine as _be
    from app.db.session import build_session_factory as _bsf
    from app.db.session import init_db as _idb

    engine = _be(pg_url or f"sqlite+aiosqlite:///{tmp_path}/tasks.db")
    await _idb(engine)
    sf = _bsf(engine)
    received: list[dict] = []

    async def handler(task_type: str, payload: dict) -> dict:
        received.append({"type": task_type, **payload})
        return {"ok": True}

    q = TaskQueue(redis_client, sf, handler, max_attempts=3, claim_idle_ms=150)
    yield q, sf, received, redis_client
    await q.stop()
    await engine.dispose()


async def test_enqueue_idempotent(queue_env):
    """同一幂等键重复入队:同 task_id,流中只有一条消息。"""
    q, sf, received, redis = queue_env
    async with sf() as db:
        tid1 = await q.enqueue(db, "t", {"a": 1}, idempotency_key="idem-1")
        tid2 = await q.enqueue(db, "t", {"a": 1}, idempotency_key="idem-1")
    assert tid1 == tid2
    assert await redis.xlen(TASK_STREAM) == 1


async def test_worker_processes_once_and_dedupes_redelivery(queue_env):
    """worker 处理一次;同任务重复投递(模拟 at-least-once)被状态机幂等吞掉。"""
    q, sf, received, redis = queue_env
    async with sf() as db:
        tid = await q.enqueue(db, "t", {"x": 1}, idempotency_key="idem-r1")
    await q.start()
    for _ in range(100):
        if q.processed >= 1:
            break
        await asyncio.sleep(0.02)
    assert q.processed == 1 and received == [{"type": "t", "x": 1}]
    # 已 ACK 的条目必须 XDEL:否则 XLEN/queue_backlog 会随历史无限增长
    assert await redis.xlen(TASK_STREAM) == 0
    assert await q.backlog() == 0

    # 模拟重复投递:直接再 XADD 一条同 task_id 消息
    await redis.xadd(TASK_STREAM, {"task_id": tid, "type": "t", "payload": json.dumps({"x": 1})})
    await asyncio.sleep(0.3)
    assert q.processed == 1  # 未重复执行
    assert received == [{"type": "t", "x": 1}]
    assert await redis.xlen(TASK_STREAM) == 0  # 重复消息也被 ACK+DEL

    async with sf() as db:
        from sqlalchemy import select

        from app.db.models import TaskRecord

        row = (await db.execute(select(TaskRecord).where(TaskRecord.id == tid))).scalar_one()
        assert row.status == "done"


async def test_failure_retries_then_dead_letter(queue_env):
    """处理器恒失败:重试至 max_attempts 后进死信流,账本 status=dead。"""
    q, sf, received, redis = queue_env

    async def bad_handler(task_type: str, payload: dict) -> dict:
        raise RuntimeError("upstream webhook down")

    q._handler = bad_handler
    async with sf() as db:
        tid = await q.enqueue(db, "t", {"x": 1}, idempotency_key="idem-dead")
    await q.start()
    for _ in range(200):
        if q.dead >= 1:
            break
        await asyncio.sleep(0.03)
    assert q.dead == 1
    dlq = await redis.xrange(DLQ_STREAM)
    assert len(dlq) == 1 and dlq[0][1]["task_id"] == tid

    from sqlalchemy import select

    from app.db.models import TaskRecord

    async with sf() as db:
        row = (await db.execute(select(TaskRecord).where(TaskRecord.id == tid))).scalar_one()
        assert row.status == "dead" and row.attempts == 3


async def test_crash_recovery_claims_stale_pending(tmp_path, redis_client):
    """崩溃恢复:幽灵消费者认领后消失,新 worker 通过 XCLAIM 收编 pending 并完成。

    用 auto_start=False 的队列:enqueue 不拉 worker,保住"幽灵先读"的时序。
    """
    from app.db.session import build_engine, build_session_factory, init_db

    engine = build_engine(f"sqlite+aiosqlite:///{tmp_path}/crash.db")
    await init_db(engine)
    sf = build_session_factory(engine)
    received: list[dict] = []

    async def handler(task_type: str, payload: dict) -> dict:
        received.append({"type": task_type, **payload})
        return {"ok": True}

    q = TaskQueue(redis_client, sf, handler, max_attempts=3, claim_idle_ms=150, auto_start=False)
    async with sf() as db:
        await q.enqueue(db, "t", {"x": 9}, idempotency_key="idem-crash")

    await q.ensure_group()
    # 模拟崩溃:ghost 消费者读取但从不处理/ACK
    msgs = await redis_client.xreadgroup(GROUP, "ghost-consumer", {TASK_STREAM: ">"}, count=10, block=100)
    assert msgs  # ghost 拿到了消息,然后"崩溃"

    await q.start()  # 正式 worker 上线,claim_idle=150ms 后收编
    for _ in range(200):
        if q.processed >= 1:
            break
        await asyncio.sleep(0.03)
    assert q.processed == 1
    assert received == [{"type": "t", "x": 9}]
    await q.stop()
    await engine.dispose()


async def test_webhook_e2e(auth_settings, redis_client):
    """端到端:网关请求完成 → webhook 任务入队(幂等键=request_id)→ worker 投递到真 HTTP 接收端。"""
    # ---- 真 HTTP webhook 接收端(uvicorn 随机端口)----
    receipts: dict[str, int] = {}

    from fastapi import FastAPI, Request

    receiver = FastAPI()

    @receiver.post("/receive")
    async def receive(request: Request):
        idem = request.headers.get("Idempotency-Key", "")
        receipts[idem] = receipts.get(idem, 0) + 1
        return {"ok": True}

    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    config = uvicorn.Config(receiver, host="127.0.0.1", port=port, log_level="error")
    server = uvicorn.Server(config)
    server_task = asyncio.get_running_loop().create_task(server.serve())
    for _ in range(50):
        if server.started:
            break
        await asyncio.sleep(0.05)

    try:
        # ---- 网关:租户带 webhook_url ----
        from app.api.main import create_app
        from app.auth.keys import generate_key
        from app.db.models import Tenant, VirtualKey
        from app.upstream.client import UpstreamClient

        mock_app = build_mock_upstream()
        upstream = UpstreamClient(
            auth_settings.upstream_base_url,
            auth_settings.upstream_api_key,
            client=httpx.AsyncClient(
                transport=httpx.ASGITransport(app=mock_app),
                base_url=auth_settings.upstream_base_url,
            ),
        )
        app = create_app(settings=auth_settings, upstream=upstream, redis_client=redis_client)
        await init_db(app.state.engine)
        raw, key_hash, prefix = generate_key()
        async with app.state.session_factory() as db:
            db.add(Tenant(id="t-test", name="t", webhook_url=f"http://127.0.0.1:{port}/receive"))
            db.add(
                VirtualKey(
                    id="k-test", tenant_id="t-test", key_hash=key_hash, key_prefix=prefix,
                    rpm_limit=10_000, tpm_limit=10_000_000,
                )
            )
            await db.commit()
        await app.state.task_queue.start()

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://gateway"
        ) as client:
            resp = await client.post(
                "/v1/chat/completions", json=_body(), headers={"Authorization": f"Bearer {raw}"}
            )
        assert resp.status_code == 200
        request_id = resp.headers["x-request-id"]

        for _ in range(200):
            if receipts.get(request_id) == 1:
                break
            await asyncio.sleep(0.03)
        assert receipts.get(request_id) == 1, f"webhook 未投递: {receipts}"

        await app.state.task_queue.stop()
        await upstream.aclose()
        await app.state.task_queue.stop()
    finally:
        server.should_exit = True
        await server_task
