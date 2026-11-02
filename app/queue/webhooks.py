"""任务处理器注册表:webhook 回调 + 用量落库(自吃狗粮——账本写走持久队列)。

usage_write 的幂等键 = "usage:{request_id}"(task_records UNIQUE 兜底):
at-least-once 投递下,同一请求的账本行最多落一次 —— 网关崩溃不再丢在途账本,
账本与 Redis 计数最终一致(rebuild 有测试)。这是把 ADR-003 的自研队列
从"只服务 webhook"升级为"网关核心记账通道"的落地(原进程内单写者退役)。
"""

import httpx

from app.log import get_logger

logger = get_logger("webhooks")


def make_task_handler(session_factory):
    """网关装配点:注入共享 session_factory(避免每任务建引擎;测试亦可注入假工厂)。"""

    async def handler(task_type: str, payload: dict) -> dict:
        if task_type == "webhook_callback":
            return await deliver_webhook(payload)
        if task_type == "usage_write":
            return await write_usage(payload, session_factory)
        return {"skipped": True, "reason": f"unknown type {task_type}"}

    return handler


async def deliver_webhook(payload: dict) -> dict:
    url = payload["webhook_url"]
    async with httpx.AsyncClient(trust_env=False, timeout=5.0) as client:  # 该机代理坑:直连
        resp = await client.post(
            url,
            json={
                "request_id": payload.get("request_id"),
                "model": payload.get("model"),
                "total_tokens": payload.get("total_tokens"),
            },
            headers={"Idempotency-Key": payload.get("request_id", "")},
        )
    if resp.status_code >= 500 or resp.status_code == 429:
        resp.raise_for_status()  # 可重试错误:抛出走队列重试/死信路径
    return {"delivered": True, "status": resp.status_code}


async def write_usage(payload: dict, session_factory) -> dict:
    """账本落库任务体:由 Streams 消费者执行(at-least-once + 幂等键去重)。"""
    from app.db.models import UsageRecord

    record = {k: v for k, v in payload.items() if k in UsageRecord.__table__.columns}
    async with session_factory() as db:
        db.add(UsageRecord(**record))
        await db.commit()
    return {"written": True, "request_id": payload.get("request_id")}
