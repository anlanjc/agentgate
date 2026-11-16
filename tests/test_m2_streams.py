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


# ---------- outbox 故障注入(P0-C):双写空洞的闭环证据 ----------


async def test_outbox_sweeper_republishes_after_xadd_failure(queue_env, monkeypatch):
    """XADD 抛异常(DB 已提交、消息未投递)→ published_at 为空 → sweeper 补投 →
    消费成功且只执行一次。"DB 提交但 Redis 没收到"的双写空洞被补投路径闭环。"""
    from redis.exceptions import ConnectionError as RedisConnectionError
    from sqlalchemy import select

    from app.db.models import TaskRecord

    q, sf, received, redis = queue_env
    monkeypatch.setattr("app.queue.streams.OUTBOX_GRACE_SECONDS", 0.0)  # 免等 grace 期

    real_xadd = redis.xadd
    state = {"failed": False}

    async def flaky_xadd(*a, **k):
        if not state["failed"]:
            state["failed"] = True  # 只让第一次(XADD 入队)失败
            raise RedisConnectionError("redis down")
        return await real_xadd(*a, **k)

    monkeypatch.setattr(redis, "xadd", flaky_xadd)

    async with sf() as db:
        tid = await q.enqueue(db, "t", {"x": 42}, idempotency_key="outbox-1")
    async with sf() as db:
        row = (await db.execute(select(TaskRecord).where(TaskRecord.id == tid))).scalar_one()
        assert row.published_at is None  # DB 已提交、投递未确认 = outbox 待补
    assert await redis.xlen(TASK_STREAM) == 0

    assert await q._outbox_sweep() == 1  # Redis 恢复(flaky 放行),sweeper 补投
    async with sf() as db:
        row = (await db.execute(select(TaskRecord).where(TaskRecord.id == tid))).scalar_one()
        assert row.published_at is not None

    await q.start()
    for _ in range(100):
        if q.processed >= 1:
            break
        await asyncio.sleep(0.02)
    assert received == [{"type": "t", "x": 42}]
    assert await q._outbox_sweep() == 0  # 幂等:已投递的不再补


async def test_marker_loss_republishes_but_consumes_once(queue_env, monkeypatch):
    """反向用例:水位回写失败(消息已投递、尚未消费的真实窗口)→ sweeper 重复投递 →
    消费端状态机幂等吞掉,handler 仍只执行一次 —— at-least-once 不放大副作用。"""
    from sqlalchemy import update

    from app.db.models import TaskRecord

    _, sf, received, redis = queue_env
    monkeypatch.setattr("app.queue.streams.OUTBOX_GRACE_SECONDS", 0.0)
    q = TaskQueue(redis, sf, _collect_with(received), auto_start=False)

    async with sf() as db:
        tid = await q.enqueue(db, "t", {"x": 7}, idempotency_key="outbox-2")
    assert await redis.xlen(TASK_STREAM) == 1  # 已投递

    async with sf() as db:  # 模拟水位丢失(回写失败的历史窗口:已投递未消费)
        await db.execute(update(TaskRecord).where(TaskRecord.id == tid).values(published_at=None))
        await db.commit()
    assert await q._outbox_sweep() == 1  # 重复投递
    assert await redis.xlen(TASK_STREAM) == 2

    await q.start()  # 消费两条同 task_id 消息
    for _ in range(100):
        if await q.backlog() == 0:
            break
        await asyncio.sleep(0.02)
    assert received == [{"type": "t", "x": 7}]  # handler 只执行一次
    assert q.processed == 1
    assert await redis.xlen(TASK_STREAM) == 0  # 两条都被 ACK+DEL


def _collect_with(received: list):
    async def handler(task_type: str, payload: dict) -> dict:
        received.append({"type": task_type, **payload})
        return {"ok": True}
    return handler


# ---------- GW-4(评审 2.0)回归:终态回写状态守卫 + 租约时钟统一 ----------


async def test_terminal_write_lost_race_keeps_winner_result(queue_env):
    """GW-4 回归(done 守卫):另一副本已写 done 时,本副本收尾不得覆盖事实。

    修复前终态回写只按 id 无条件 UPDATE —— 双副本接管场景下后写者翻转前写者的
    终态/结果。修复后带 status='processing' 守卫,rowcount=0 放弃写入,消息不 ACK,
    由 reclaim 按"已 done"幂等清理。"""

    from sqlalchemy import select, update

    from app.db.models import TaskRecord

    q, sf, received, redis = queue_env

    async def racing_handler(task_type: str, payload: dict) -> dict:
        # 模拟接管副本在本次 handler 执行期间抢先写终态
        async with sf() as db:
            await db.execute(
                update(TaskRecord).where(TaskRecord.status == "processing")
                .values(status="done", result={"winner": "other-copy"})
            )
            await db.commit()
        return {"winner": "this-copy"}

    q._handler = racing_handler
    async with sf() as db:
        tid = await q.enqueue(db, "t", {"x": 1}, idempotency_key="gw4-done-race")

    await q.flush()  # 含 reclaim 清理:消息最终被"已 done"检查 ACK+DEL

    async with sf() as db:
        row = (await db.execute(select(TaskRecord).where(TaskRecord.id == tid))).scalar_one()
    assert row.status == "done"
    assert row.result == {"winner": "other-copy"}  # 接管者的结果,未被覆盖
    assert q.processed == 0  # 本副本输掉竞争:不计 processed
    assert await redis.xlen(TASK_STREAM) == 0  # 消息已被幂等清理


async def test_dead_write_lost_race_no_false_dlq(queue_env):
    """GW-4 回归(dead 守卫):副本已把任务写成 done,失败副本不得再投假死信、
    不得把 done 翻回 dead(修复前死信回写无守卫)。"""
    from sqlalchemy import select, update

    from app.db.models import TaskRecord

    q, sf, received, redis = queue_env
    q._auto_start = False  # 保住"先改 attempts 再投递"的时序

    async def racing_bad_handler(task_type: str, payload: dict) -> dict:
        async with sf() as db:
            await db.execute(
                update(TaskRecord).where(TaskRecord.status == "processing")
                .values(status="done", result={"winner": "other-copy"})
            )
            await db.commit()
        raise RuntimeError("boom after takeover")

    q._handler = racing_bad_handler
    async with sf() as db:
        tid = await q.enqueue(db, "t", {"x": 1}, idempotency_key="gw4-dead-race")
        await db.execute(update(TaskRecord).where(TaskRecord.id == tid).values(attempts=2))
        await db.commit()

    await q.start()
    await q.flush()

    async with sf() as db:
        row = (await db.execute(select(TaskRecord).where(TaskRecord.id == tid))).scalar_one()
    assert row.status == "done"  # 未被翻成 dead
    assert row.result == {"winner": "other-copy"}
    assert q.dead == 0
    assert await redis.xlen(DLQ_STREAM) == 0  # 没有假死信
    assert await redis.xlen(TASK_STREAM) == 0


async def test_reclaim_progresses_under_sustained_load(queue_env, monkeypatch):
    """GATE-4(评审 4.0)回归:持续负载下 reclaim 仍推进重试/死信(死信上界可达)。

    修复前失败任务的重试与死信推进完全依赖 _reclaim_stale,而它只在 _read_new
    返回 0 的空轮执行 —— _read_new block=200ms 批量读,持续负载(handler 为
    webhook 投递时约 ≥5 任务/s 即饱和)下每轮 handled>0,空闲轮永不出现:失败
    消息滞留 PEL 无上界、max_attempts 的死信推进被流量饿死 —— 高峰恰是最需要
    重试的场景。修复后 reclaim 挂进 _periodic_maintenance 维护周期(2s),空闲轮
    即时 reclaim 保留(快速恢复路径)。

    判别口径(结构化,不赌空轮时序):预置 300 条积压 → drain 期内每轮
    _read_new 必返回 >0(空轮结构性不存在);断言"死信推进"发生在"新消息
    尚未排空"的窗口内。维护周期压到 50ms 只是测试提速,生产缺省 2s 同一
    代码路径。"""
    q, sf, received, redis = queue_env
    monkeypatch.setattr("app.queue.streams.MAINTENANCE_INTERVAL_SECONDS", 0.05)
    q._auto_start = False  # 先备好积压再开闸,保住"负载窗口"结构

    async def bad_handler(task_type: str, payload: dict) -> dict:
        raise RuntimeError("webhook down under load")

    q._handler = bad_handler
    async with sf() as db:
        for i in range(300):
            await q.enqueue(db, "t", {"n": i}, idempotency_key=f"load-{i}")
    assert await redis.xlen(TASK_STREAM) == 300

    await q.start()
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 10.0
    while loop.time() < deadline:
        if q.dead >= 1 or await redis.xlen(TASK_STREAM) == 0:
            break
        await asyncio.sleep(0.02)
    remaining = await redis.xlen(TASK_STREAM)
    # 修复前:drain 期内每轮 handled>0 → reclaim 饿死 → 新消息排空时 dead 仍 0;
    # 修复后:维护周期 reclaim 推进 → 早期任务走完 3 次尝试进死信(远早于排空)
    assert q.dead >= 1, (
        f"持续负载(新消息积压期每轮 handled>0,无空轮)下死信推进被饿死:"
        f" dead={q.dead}, 新消息剩余={remaining}"
    )
    await q.stop()


async def test_lease_timestamp_uses_redis_clock_not_local(queue_env, monkeypatch):
    """GW-4 回归(租约时钟):把本进程本地时钟拨快 120s,updated_at 仍取
    Redis 服务器时钟 —— 修复前各副本用本地 _utcnow() 写租约、又用本地时钟
    比对别副本的租约,快钟副本会在慢钟副本 handler 执行中提前接管。"""
    from datetime import UTC, datetime, timedelta

    from sqlalchemy import select

    from app.db.models import TaskRecord
    from app.queue import streams as streams_mod

    q, sf, received, redis = queue_env

    real_utcnow = streams_mod._utcnow

    def skewed():
        return real_utcnow() + timedelta(seconds=120)

    monkeypatch.setattr(streams_mod, "_utcnow", skewed)

    async with sf() as db:
        tid = await q.enqueue(db, "t", {"x": 1}, idempotency_key="gw4-clock")
    await q.flush()

    async with sf() as db:
        row = (await db.execute(select(TaskRecord).where(TaskRecord.id == tid))).scalar_one()
    r_sec, _ = await redis.time()
    redis_now = datetime.fromtimestamp(r_sec, tz=UTC)
    updated = row.updated_at if row.updated_at.tzinfo else row.updated_at.replace(tzinfo=UTC)
    # 若用的是被拨快的本地时钟,偏差会是 120s —— 断言钉住"必须来自 Redis 时钟"
    assert abs(updated - redis_now) < timedelta(seconds=5)


async def test_worker_self_heals_after_redis_data_loss(queue_env):
    """redis 数据丢失(重启/flush)致 consumer group 消失后,worker 自愈重建并继续消费。

    混沌矩阵 2026-11-09 实测发现:修复前 NOGROUP 死循环使账本消费停摆至应用重启。
    """
    q, sf, received, redis = queue_env
    async with sf() as db:
        await q.enqueue(db, "t", {"x": 1}, idempotency_key="idem-h1")
    await q.start()
    for _ in range(200):
        if q.processed >= 1:
            break
        await asyncio.sleep(0.02)
    assert q.processed == 1
    # 模拟 redis 重启:数据全丢(stream 与 consumer group 一并消失)
    await redis.flushdb()
    # 期间的新投递:XADD 自动建流,但 group 不存在(生产端可用,消费端 NOGROUP)
    async with sf() as db:
        await q.enqueue(db, "t", {"x": 2}, idempotency_key="idem-h2")
    deadline = asyncio.get_running_loop().time() + 5.0
    while asyncio.get_running_loop().time() < deadline:
        if q.processed >= 2:
            break
        await asyncio.sleep(0.05)
    assert q.processed >= 2, f"worker 未自愈: processed={q.processed}"


# ---------- GATE-5(评审 4.0)回归:DLQ 淘汰纪律 ----------


async def test_dlq_trimmed_to_cap(queue_env, monkeypatch):
    """死信流只 XADD 从不裁剪 → 故障风暴下每个死任务永久占一条 Redis 流内存,
    违背模块头自述的"Stream 无限膨胀"纪律(主流 ACK 后 XDEL,DLQ 却无界)。
    对齐 Redis 语义缓存 200 条上限纪律:XADD 后 XTRIM 精确裁剪,保最近
    DLQ_MAX_ENTRIES 条(生产缺省 200,测试 patch 到 5 提速)。"""
    q, sf, received, redis = queue_env
    monkeypatch.setattr("app.queue.streams.DLQ_MAX_ENTRIES", 5)
    q._max_attempts = 1  # 一次失败即死信,提速

    async def bad_handler(task_type: str, payload: dict) -> dict:
        raise RuntimeError(f"boom {payload.get('n')}")

    q._handler = bad_handler
    async with sf() as db:
        for i in range(8):
            await q.enqueue(db, "t", {"n": i}, idempotency_key=f"dlq-{i}")
    await q.start()
    for _ in range(200):
        if q.dead >= 8:
            break
        await asyncio.sleep(0.03)
    assert q.dead == 8
    # 精确裁剪(非 approximate):上界硬保证,保最近 5 条
    assert await redis.xlen(DLQ_STREAM) == 5
    entries = await redis.xrange(DLQ_STREAM)
    kept_errors = [fields["error"] for _, fields in entries]
    assert kept_errors == [f"boom {i}" for i in range(3, 8)], (
        f"裁剪应保留最近 5 条: {kept_errors}"
    )
    await q.stop()
