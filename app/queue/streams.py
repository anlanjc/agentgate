"""Redis Streams 任务队列(ADR-003):at-least-once 投递 + 幂等消费 = 效果上的 exactly-once。

核心机制(consumer group 全链路,不用 Celery 黑盒):
- 生产:先提交 task_records(outbox 意图)→ XADD → 回写 published_at;XADD 失败由
  sweeper 重投,消除"DB 已提交但消息没进 Redis"的双写空洞;
- 消费:XREADGROUP(consumer group ">"),处理成功 XACK + XDEL;
- 失败与崩溃统一成一条恢复路径:未 ACK 的消息留在 PEL,XCLAIM 认领 idle 超时的,
  attempts +1,超过 max_attempts → XADD 死信流 + XACK + status=dead;
- 幂等:消费端用条件 UPDATE 原子抢占(同一 task_id 不会被两个消费者同时执行),
  账本再用 (tenant_id, request_id) 唯一键兜底;
- 语义诚实声明:投递是 at-least-once,副作用必须幂等;队列本身不承诺 exactly-once。

为什么 XACK 后还要 XDEL:XLEN 会把已 ACK 的历史消息也算进 backlog,队列深度只增不减,
既误导 /ready 与 Grafana,也让 Stream 内存无限增长。ACK 后删除已消费条目,backlog 才是
"真正待处理量"。
"""

import asyncio
import json
import time
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta

import redis.asyncio as aredis
from sqlalchemy import and_, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import TaskRecord
from app.log import get_logger

logger = get_logger("tasks")

TASK_STREAM = "gate:tasks"
DLQ_STREAM = "gate:tasks:dlq"
GROUP = "gate-workers"

OUTBOX_GRACE_SECONDS = 2.0
MAINTENANCE_INTERVAL_SECONDS = 2.0


def _utcnow() -> datetime:
    return datetime.now(UTC)


class TaskQueue:
    def __init__(
        self,
        redis: aredis.Redis,
        session_factory,
        handler: Callable[[dict, dict], Awaitable[dict | None]],
        *,
        consumer: str | None = None,
        max_attempts: int = 3,
        # visibility timeout 必须大于 handler 最大耗时(webhook timeout=5s),
        # 否则处理中的消息会被另一个 worker 认领,造成重复副作用。
        claim_idle_ms: int = 30_000,
        batch: int = 16,
        auto_start: bool = True,
    ) -> None:
        self._redis = redis
        self._session_factory = session_factory
        self._handler = handler
        self._consumer = consumer or f"c-{uuid.uuid4().hex[:8]}"
        self._max_attempts = max_attempts
        self._claim_idle_ms = claim_idle_ms
        self._batch = batch
        self._auto_start = auto_start  # False:测试显式控制启动时序(如幽灵消费者场景)
        self._task: asyncio.Task | None = None
        self._last_maintenance = 0.0
        self.processed = 0
        self.dead = 0

    # ---------- 生产端 ----------

    async def enqueue(
        self, db: AsyncSession, task_type: str, payload: dict, idempotency_key: str
    ) -> str:
        """幂等入队:UNIQUE 约束兜底,重复键返回既有 task_id,不再重复投递。

        注意顺序:先落 task_records,再 XADD。XADD 失败不会丢任务 —— published_at
        仍为空,sweeper 会补投;重复投递由消费端幂等吞掉。
        """
        row = await db.execute(
            select(TaskRecord).where(TaskRecord.idempotency_key == idempotency_key)
        )
        existing = row.scalar_one_or_none()
        if existing is not None:
            if existing.published_at is None and existing.status == "pending":
                await self._publish_record(existing.id, existing.type, existing.payload or {})
            return existing.id

        record = TaskRecord(
            id=uuid.uuid4().hex[:32],
            idempotency_key=idempotency_key,
            type=task_type,
            payload=payload,
        )
        db.add(record)
        try:
            await db.commit()
        except Exception:
            await db.rollback()
            row = await db.execute(
                select(TaskRecord).where(TaskRecord.idempotency_key == idempotency_key)
            )
            existing = row.scalar_one_or_none()
            if existing is None:
                raise
            return existing.id

        await self._publish_record(record.id, task_type, payload)
        return record.id

    async def _publish_record(self, task_id: str, task_type: str, payload: dict) -> bool:
        """XADD + 回写 published_at;Redis 故障时返回 False,留给 outbox sweeper。"""
        if self._auto_start:
            try:
                await self.ensure_started()
            except Exception as exc:
                logger.warning("task_worker_start_deferred", error=str(exc))

        try:
            await self._redis.xadd(
                TASK_STREAM,
                {
                    "task_id": task_id,
                    "type": task_type,
                    "payload": json.dumps(payload, ensure_ascii=False),
                },
            )
        except Exception as exc:
            logger.warning("task_publish_deferred", task_id=task_id, error=str(exc))
            return False

        try:
            async with self._session_factory() as db:
                await db.execute(
                    update(TaskRecord)
                    .where(TaskRecord.id == task_id, TaskRecord.published_at.is_(None))
                    .values(published_at=_utcnow())
                )
                await db.commit()
        except Exception as exc:
            # 消息已投递但水位没写成功:下次 sweep 会重复投递,消费端幂等消化。
            logger.warning("task_publish_marker_failed", task_id=task_id, error=str(exc))
        return True

    async def _outbox_sweep(self, limit: int = 100) -> int:
        """补投"DB 已提交、XADD 未成功"的任务(transactional outbox relay)。"""
        cutoff = _utcnow() - timedelta(seconds=OUTBOX_GRACE_SECONDS)
        async with self._session_factory() as db:
            rows = (
                await db.execute(
                    select(TaskRecord.id, TaskRecord.type, TaskRecord.payload)
                    .where(
                        TaskRecord.status == "pending",
                        TaskRecord.published_at.is_(None),
                        TaskRecord.created_at <= cutoff,
                    )
                    .order_by(TaskRecord.created_at)
                    .limit(limit)
                )
            ).all()

        published = 0
        for task_id, task_type, payload in rows:
            try:
                await self._redis.xadd(
                    TASK_STREAM,
                    {
                        "task_id": task_id,
                        "type": task_type,
                        "payload": json.dumps(payload or {}, ensure_ascii=False),
                    },
                )
            except Exception as exc:
                logger.warning("outbox_sweep_deferred", task_id=task_id, error=str(exc))
                break
            try:
                async with self._session_factory() as db:
                    await db.execute(
                        update(TaskRecord)
                        .where(TaskRecord.id == task_id, TaskRecord.published_at.is_(None))
                        .values(published_at=_utcnow())
                    )
                    await db.commit()
            except Exception as exc:
                logger.warning("outbox_marker_failed", task_id=task_id, error=str(exc))
            published += 1
        if published:
            logger.info("outbox_republished", count=published)
        return published

    # ---------- 消费端 ----------

    async def ensure_group(self) -> None:
        try:
            await self._redis.xgroup_create(TASK_STREAM, GROUP, id="0", mkstream=True)
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                raise

    async def start(self) -> None:
        await self.ensure_group()
        if self._task is None:
            self._task = asyncio.get_running_loop().create_task(self._run())

    async def ensure_started(self) -> None:
        """惰性启动(首次 enqueue 即拉起 worker;幂等,测试/生产同一语义)。"""
        await self.ensure_group()
        if self._task is None:
            await self.start()

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def backlog(self) -> int:
        """待处理量:未投递 + pending(已 ACK 的条目会 XDEL,不虚增)。"""
        total = int(await self._redis.xlen(TASK_STREAM))
        try:
            info = await self._redis.xpending(TASK_STREAM, GROUP)
            total += int(info.get("pending", 0) or 0)
        except Exception as e:
            if "NOGROUP" not in str(e):
                logger.warning("backlog_probe_failed", error=str(e))
        return total

    async def flush(self, timeout: float = 5.0) -> None:
        """测试/收尾语义:等待流水清空(未投递与 pending 均归零)。"""
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            if await self.backlog() == 0:
                return
            if self._task is None:
                await self._read_new()
                await self._reclaim_stale()
                await self._outbox_sweep()
            await asyncio.sleep(0.02)
        logger.warning("queue_flush_timeout", backlog=await self.backlog(), timeout=timeout)

    async def _run(self) -> None:
        while True:
            try:
                handled = await self._read_new()
                if handled == 0:
                    await self._reclaim_stale()
                await self._periodic_maintenance()
                if handled == 0:
                    await asyncio.sleep(0.02)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("worker_loop_error")
                await asyncio.sleep(0.5)

    async def _periodic_maintenance(self) -> None:
        now = time.monotonic()
        if now - self._last_maintenance < MAINTENANCE_INTERVAL_SECONDS:
            return
        self._last_maintenance = now
        try:
            await self._outbox_sweep()
        except Exception as exc:
            logger.warning("outbox_sweep_failed", error=str(exc))
        try:
            from app.observability import metrics

            metrics.USAGE_QUEUE_DEPTH.set(await self.backlog())
        except Exception as exc:
            logger.warning("queue_depth_metric_failed", error=str(exc))

    async def _read_new(self) -> int:
        results = await self._redis.xreadgroup(
            GROUP, self._consumer, {TASK_STREAM: ">"}, count=self._batch, block=200
        )
        n = 0
        for _stream, messages in results or []:
            for msg_id, fields in messages:
                await self._process(msg_id, fields)
                n += 1
        return n

    async def _reclaim_stale(self) -> None:
        """崩溃恢复路径:认领 idle 超时的 pending 消息(XCLAIM,Redis 5.0 兼容)。

        兼容性注记:Redis 6.2+ 的 XPENDING 支持 IDLE 过滤参数,5.0 不支持 ——
        因此取全量 pending 后在 Python 侧按 time_since_delivered 过滤,再用
        XCLAIM(min_idle_time)原子认领,避免与并发消费者重复处理。
        """
        pending = await self._redis.xpending_range(
            TASK_STREAM, GROUP, min="-", max="+", count=max(self._batch, 100)
        )
        if not pending:
            return
        stale_ids = [
            p["message_id"]
            for p in pending
            if p["time_since_delivered"] >= self._claim_idle_ms
        ]
        if not stale_ids:
            return
        claimed = await self._redis.xclaim(
            TASK_STREAM, GROUP, self._consumer, min_idle_time=self._claim_idle_ms, message_ids=stale_ids
        )
        for msg_id, fields in claimed or []:
            await self._process(msg_id, fields, reclaimed=True)

    async def _ack_and_delete(self, msg_id) -> None:
        """XACK 后删除已消费条目:backlog 不随历史增长,Stream 不再无限膨胀。"""
        await self._redis.xack(TASK_STREAM, GROUP, msg_id)
        await self._redis.xdel(TASK_STREAM, msg_id)

    async def _process(self, msg_id, fields: dict, *, reclaimed: bool = False) -> None:
        task_id = fields.get("task_id", "")
        task_type = fields.get("type", "")
        try:
            payload = json.loads(fields.get("payload", "{}"))
        except ValueError:
            payload = {}

        async with self._session_factory() as db:
            row = await db.execute(select(TaskRecord).where(TaskRecord.id == task_id))
            record = row.scalar_one_or_none()
            if record is None:
                # 队列里有、账本没有:孤消息,不能反复投毒。
                await self._ack_and_delete(msg_id)
                return
            if record.status in ("done", "dead"):
                # 幂等消费:已完成/已死信(可能由其他副本处理),直接 ACK 丢弃重复投递。
                await self._ack_and_delete(msg_id)
                return

            # 原子抢占:同一 task_id 的重复投递只有一个消费者能拿到处理权。
            # processing 超过 visibility timeout 视为上一个消费者崩溃,允许接管。
            # 先在 UPDATE 前记下旧值:SQLAlchemy 的 synchronize_session 会把
            # record.attempts 原地改成新值,更新后再 +1 会多算一次。
            prev_attempts = int(record.attempts)
            stale_cutoff = _utcnow() - timedelta(milliseconds=self._claim_idle_ms)
            claimed = await db.execute(
                update(TaskRecord)
                .where(
                    TaskRecord.id == task_id,
                    or_(
                        TaskRecord.status == "pending",
                        and_(
                            TaskRecord.status == "processing",
                            TaskRecord.updated_at <= stale_cutoff,
                        ),
                    ),
                )
                .values(
                    status="processing",
                    attempts=TaskRecord.attempts + 1,
                    updated_at=_utcnow(),
                )
            )
            await db.commit()
            if claimed.rowcount != 1:
                await self._ack_and_delete(msg_id)
                return
            attempts = prev_attempts + 1

            try:
                result = await self._handler(task_type, payload)
                await db.execute(
                    update(TaskRecord)
                    .where(TaskRecord.id == task_id)
                    .values(status="done", result=result, updated_at=_utcnow())
                )
                await db.commit()
                await self._ack_and_delete(msg_id)
                self.processed += 1
                logger.info("task_done", task_id=task_id, attempts=attempts, reclaimed=reclaimed)
            except Exception as exc:
                # 失败不 ACK:留在 PEL 等待 reclaim 重试;超过上限进死信。
                if attempts >= self._max_attempts:
                    await db.execute(
                        update(TaskRecord)
                        .where(TaskRecord.id == task_id)
                        .values(
                            status="dead",
                            result={"error": str(exc)},
                            updated_at=_utcnow(),
                        )
                    )
                    await db.commit()
                    await self._redis.xadd(
                        DLQ_STREAM,
                        {"task_id": task_id, "type": task_type, "error": str(exc)[:500]},
                    )
                    await self._ack_and_delete(msg_id)
                    self.dead += 1
                    try:
                        from app.observability import metrics

                        metrics.TASKS_DEAD.inc()
                    except Exception as metric_exc:
                        logger.warning("task_dead_metric_failed", error=str(metric_exc))
                    logger.warning("task_dead", task_id=task_id, attempts=attempts, error=str(exc))
                else:
                    await db.execute(
                        update(TaskRecord)
                        .where(TaskRecord.id == task_id)
                        .values(status="pending", updated_at=_utcnow())
                    )
                    await db.commit()
                    logger.info(
                        "task_retry_scheduled", task_id=task_id, attempts=attempts, error=str(exc)
                    )
