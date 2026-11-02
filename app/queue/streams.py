"""Redis Streams 任务队列(ADR-003):at-least-once 投递 + 幂等消费 = 效果上的 exactly-once。

核心机制(consumer group 全链路,不用 Celery 黑盒):
- 生产:XADD gate:tasks;幂等键在 PG task_records UNIQUE 兜底(重复入队直接跳过);
- 消费:XREADGROUP(consumer group ">"),处理成功 XACK;
- 失败与崩溃统一成一条恢复路径:未 ACK 的消息留在 PEL,XCLAIM 认领 idle 超时的,
  attempts +1,超过 max_attempts → XADD 死信流 + XACK + status=dead;
- 语义诚实声明:投递是 at-least-once,消费端幂等靠 TaskRecord 状态机 + 业务幂等键。
"""

import asyncio
import json
import uuid
from collections.abc import Awaitable, Callable

import redis.asyncio as aredis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import TaskRecord
from app.log import get_logger

logger = get_logger("tasks")

TASK_STREAM = "gate:tasks"
DLQ_STREAM = "gate:tasks:dlq"
GROUP = "gate-workers"


class TaskQueue:
    def __init__(
        self,
        redis: aredis.Redis,
        session_factory,
        handler: Callable[[dict, dict], Awaitable[dict | None]],
        *,
        consumer: str | None = None,
        max_attempts: int = 3,
        claim_idle_ms: int = 500,
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
        self.processed = 0
        self.dead = 0

    # ---------- 生产端 ----------

    async def enqueue(
        self, db: AsyncSession, task_type: str, payload: dict, idempotency_key: str
    ) -> str:
        """幂等入队:UNIQUE 约束兜底,重复键返回既有 task_id,不再 XADD。"""
        if self._auto_start:
            await self.ensure_started()
        row = await db.execute(
            select(TaskRecord).where(TaskRecord.idempotency_key == idempotency_key)
        )
        existing = row.scalar_one_or_none()
        if existing is not None:
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
            return row.scalar_one().id  # 并发入队竞态:唯一约束兜底,取既有记录

        await self._redis.xadd(
            TASK_STREAM,
            {"task_id": record.id, "type": task_type, "payload": json.dumps(payload)},
        )
        return record.id

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
        """待处理量:未投递 + pending(可观测口径)。"""
        total = int(await self._redis.xlen(TASK_STREAM))
        try:
            info = await self._redis.xpending(TASK_STREAM, GROUP)
            total += int(info.get("pending", 0) or 0)
        except Exception as e:
            logger.warning("backlog_probe_failed", error=str(e))
        return total

    async def flush(self, timeout: float = 5.0) -> None:
        """测试/收尾语义:等待流水清空(未投递与 pending 均归零)。

        worker 在跑时被动等待;worker 未启动时主动同步抽干(不依赖事件循环调度)。
        """
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            if await self.backlog() == 0:
                return
            if self._task is None:
                await self._read_new()
                await self._reclaim_stale()
            await asyncio.sleep(0.02)

    async def _run(self) -> None:
        while True:
            try:
                handled = await self._read_new()
                if handled == 0:
                    await self._reclaim_stale()
                    await asyncio.sleep(0.02)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("worker_loop_error")
                await asyncio.sleep(0.5)

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
            TASK_STREAM, GROUP, min="-", max="+", count=self._batch
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
                await self._redis.xack(TASK_STREAM, GROUP, msg_id)
                return
            if record.status == "done":
                # 幂等消费:任务已完成(可能由其他副本处理),直接 ACK 丢弃重复投递
                await self._redis.xack(TASK_STREAM, GROUP, msg_id)
                return

            record.status = "processing"
            record.attempts += 1
            await db.commit()
            attempts = record.attempts

            try:
                result = await self._handler(task_type, payload)
                record.status = "done"
                record.result = result
                await db.commit()
                await self._redis.xack(TASK_STREAM, GROUP, msg_id)
                self.processed += 1
                logger.info("task_done", task_id=task_id, attempts=attempts, reclaimed=reclaimed)
            except Exception as exc:
                # 失败不 ACK:留在 PEL 等待 reclaim 重试;超过上限进死信
                if attempts >= self._max_attempts:
                    record.status = "dead"
                    record.result = {"error": str(exc)}
                    await db.commit()
                    await self._redis.xadd(
                        DLQ_STREAM,
                        {"task_id": task_id, "type": task_type, "error": str(exc)[:500]},
                    )
                    await self._redis.xack(TASK_STREAM, GROUP, msg_id)
                    self.dead += 1
                    logger.warning("task_dead", task_id=task_id, attempts=attempts, error=str(exc))
                else:
                    record.status = "pending"
                    await db.commit()
                    logger.info(
                        "task_retry_scheduled", task_id=task_id, attempts=attempts, error=str(exc)
                    )
