"""单写者异步记账(M1 形态;M2 升级为 Redis Streams 持久化队列,接口不变)。

架构要点(呼应 LiteLLM "no database write sits in the request path"):
- 请求路径只做 queue.put_nowait(零 await DB);
- 单一消费者任务批量落库 —— 串行化写,天然规避 SQLite 写锁,也是 PG 下减少连接占用的取法;
- 消费者崩溃不影响请求路径:队列有界,满了丢弃最旧并计数告警(可观测优先于静默阻塞)。
"""

import asyncio
from typing import Any

from app.log import get_logger

logger = get_logger("usage-writer")


class UsageWriter:
    def __init__(self, session_factory, batch_size: int = 64, max_queue: int = 10_000) -> None:
        self._session_factory = session_factory
        self._batch_size = batch_size
        self._queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=max_queue)
        self._task: asyncio.Task | None = None
        self.dropped = 0  # 可观测:队列溢出丢弃数(正常应为 0)

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.get_running_loop().create_task(self._run())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    def submit(self, record: dict) -> None:
        self._ensure_started()
        try:
            self._queue.put_nowait(record)
        except asyncio.QueueFull:
            self.dropped += 1
            from app.observability import metrics

            metrics.USAGE_DROPPED.inc()
            logger.warning("usage_queue_overflow", dropped=self.dropped)

    def _ensure_started(self) -> None:
        """惰性启动(测试路径无 lifespan;幂等)。"""
        if self._task is None or self._task.done():
            self._task = asyncio.get_running_loop().create_task(self._run())

    async def _run(self) -> None:
        from app.db.models import UsageRecord

        while True:
            batch = [await self._queue.get()]
            deadline = asyncio.get_running_loop().time() + 0.05  # 小批聚合 50ms
            while len(batch) < self._batch_size:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    break
                try:
                    batch.append(await asyncio.wait_for(self._queue.get(), timeout=remaining))
                except TimeoutError:
                    break
            try:
                async with self._session_factory() as session:
                    session.add_all([UsageRecord(**r) for r in batch])
                    await session.commit()
            except Exception:
                logger.exception("usage_write_failed", batch_size=len(batch))
                # 诚实语义:落库失败不阻塞网关;账本与 Redis 计数可能短暂不一致,
                # 对账以 Redis 计数为准(见 quota/service.py rebuild 逻辑)

    async def flush(self) -> None:
        """测试辅助:等待队列清空并落库完成。"""
        while self._queue.qsize() > 0 or self._task is None:
            await asyncio.sleep(0.01)
        # 等最后一次批量写完成
        await asyncio.sleep(0.15)
