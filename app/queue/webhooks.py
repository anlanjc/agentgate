"""webhook 回调投递器:任务处理器(队列的载荷,幂等键由调用方以 Header 传递)。"""

import httpx

from app.log import get_logger

logger = get_logger("webhooks")


async def webhook_handler(task_type: str, payload: dict) -> dict:
    if task_type != "webhook_callback":
        return {"skipped": True, "reason": f"unknown type {task_type}"}
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
