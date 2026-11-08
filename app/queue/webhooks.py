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
            return await deliver_webhook(payload, session_factory)
        if task_type == "usage_write":
            return await write_usage(payload, session_factory)
        return {"skipped": True, "reason": f"unknown type {task_type}"}

    return handler


async def _resolve_webhook_target(payload: dict, session_factory) -> tuple[str, str] | None:
    """GW-8:投递目标(url, 明文 secret)在投递时才查库解密。

    队列 payload 只带 tenant_id(修复前 webhook_secret 明文随 payload XADD 进
    Redis 流/AOF 快照并永久存 task_records.payload,拖备份即得全部租户验签密钥)。
    兼容边界:旧格式 payload(webhook_url/webhook_secret 直带)原样支持 ——
    部署窗口内未消费的历史消息不丢。
    """
    if payload.get("tenant_id"):
        from sqlalchemy import select

        from app.db.models import Tenant
        from app.security.crypto import decrypt_secret

        async with session_factory() as db:
            row = await db.execute(select(Tenant).where(Tenant.id == payload["tenant_id"]))
            tenant = row.scalar_one_or_none()
        if tenant is None or not tenant.webhook_url:
            return None
        return tenant.webhook_url, decrypt_secret(tenant.webhook_secret) if tenant.webhook_secret else ""
    # 旧格式(部署窗口兼容):payload 自带 url/secret
    url = payload.get("webhook_url")
    if not url:
        return None
    return url, payload.get("webhook_secret") or ""


async def deliver_webhook(payload: dict, session_factory) -> dict:
    """投递回调;租户配了 webhook_secret 时附 HMAC 签名(P4 安全补强)。

    验签协议(接收端):
        X-Agentgate-Timestamp: unix 秒
        X-Agentgate-Signature: sha256=<HMAC(secret, "{timestamp}.{raw_body}")>
    时间戳参与签名防重放;接收端应自行拒绝过旧时间戳(建议 5 分钟窗)。
    """
    import hashlib
    import hmac as hmac_mod
    import json as json_mod
    import time as time_mod

    resolved = await _resolve_webhook_target(payload, session_factory)
    if resolved is None:
        # 租户已删/未配 url:投递目标不存在,按完成处理(不重试不留死信)
        return {"skipped": True, "reason": "webhook target missing",
                "tenant_id": payload.get("tenant_id")}
    url, secret = resolved
    body = json_mod.dumps(
        {
            "request_id": payload.get("request_id"),
            "model": payload.get("model"),
            "total_tokens": payload.get("total_tokens"),
        },
        ensure_ascii=False,
    ).encode()
    headers = {"Idempotency-Key": payload.get("request_id", ""), "Content-Type": "application/json"}
    if secret:
        ts = str(int(time_mod.time()))
        mac = hmac_mod.new(secret.encode(), f"{ts}.".encode() + body, hashlib.sha256)
        headers["X-Agentgate-Timestamp"] = ts
        headers["X-Agentgate-Signature"] = "sha256=" + mac.hexdigest()
    async with httpx.AsyncClient(trust_env=False, timeout=5.0) as client:  # 该机代理坑:直连
        resp = await client.post(url, content=body, headers=headers)
    if resp.status_code >= 500 or resp.status_code == 429:
        resp.raise_for_status()  # 可重试错误:抛出走队列重试/死信路径
    return {"delivered": True, "status": resp.status_code}


async def write_usage(payload: dict, session_factory) -> dict:
    """账本落库任务体:由 Streams 消费者执行。

    两层幂等:
    1. TaskRecord 状态机保证同一任务不重复执行(正常路径);
    2. usage_records(tenant_id, request_id) UNIQUE 兜底 —— handler 写库成功后、
       任务状态回写前进程崩溃时,重投会再次执行 handler,唯一键让账本仍只有一行。
    """
    from sqlalchemy import select
    from sqlalchemy.exc import IntegrityError

    from app.db.models import UsageRecord

    record = {k: v for k, v in payload.items() if k in UsageRecord.__table__.columns}
    if record.get("key_id") == "anonymous":
        record["key_id"] = None
    # 成本(P4):账本写入点计算(所有路径同源);未登记价格的模型 cost=None
    from app.billing import compute_cost

    record["cost"] = compute_cost(
        record.get("model", ""), int(record.get("prompt_tokens") or 0),
        int(record.get("completion_tokens") or 0),
    )
    async with session_factory() as db:
        exists = await db.execute(
            select(UsageRecord.id).where(
                UsageRecord.tenant_id == record.get("tenant_id", ""),
                UsageRecord.request_id == record.get("request_id", ""),
            )
        )
        if exists.scalar_one_or_none() is not None:
            return {"written": False, "duplicate": True, "request_id": record.get("request_id")}
        db.add(UsageRecord(**record))
        try:
            await db.commit()
        except IntegrityError:
            await db.rollback()
            return {"written": False, "duplicate": True, "request_id": record.get("request_id")}
    return {"written": True, "request_id": record.get("request_id")}
