"""管理端 API:租户 / virtual key 签发与吊销 / 用量对账。

安全:settings.admin_token 非空时校验 X-Admin-Token(生产必配);为空仅限本机开发。
raw key 只在签发响应中出现一次,服务端只存哈希(泄露库≠泄露可用 key)。
"""

import uuid

from fastapi import APIRouter, Request
from pydantic import BaseModel
from sqlalchemy import func, select

from app.auth.keys import generate_key, invalidate
from app.db.models import Tenant, UsageRecord, VirtualKey
from app.errors import GatewayError

router = APIRouter(prefix="/admin")


def _require_admin(request: Request) -> None:
    token = request.app.state.settings.admin_token
    if token and request.headers.get("x-admin-token") != token:
        raise GatewayError(401, "admin token invalid", "authentication_error", "admin_unauthorized")


class TenantCreate(BaseModel):
    name: str
    webhook_url: str | None = None


class KeyCreate(BaseModel):
    name: str = ""
    rpm_limit: int = 60
    tpm_limit: int = 100_000
    budget_tokens: int | None = None
    model_whitelist: list[str] | None = None


@router.post("/tenants")
async def create_tenant(body: TenantCreate, request: Request) -> dict:
    _require_admin(request)
    async with request.app.state.session_factory() as db:
        tenant = Tenant(id=uuid.uuid4().hex[:32], name=body.name, webhook_url=body.webhook_url)
        db.add(tenant)
        await db.commit()
        return {"id": tenant.id, "name": tenant.name, "webhook_url": tenant.webhook_url}


@router.post("/tenants/{tenant_id}/keys")
async def create_key(tenant_id: str, body: KeyCreate, request: Request) -> dict:
    _require_admin(request)
    raw, key_hash, prefix = generate_key()
    async with request.app.state.session_factory() as db:
        row = await db.execute(select(Tenant).where(Tenant.id == tenant_id))
        if row.scalar_one_or_none() is None:
            raise GatewayError(404, "tenant not found", "invalid_request_error", "tenant_not_found")
        vk = VirtualKey(
            id=uuid.uuid4().hex[:32],
            tenant_id=tenant_id,
            key_hash=key_hash,
            key_prefix=prefix,
            name=body.name,
            model_whitelist=body.model_whitelist,
            rpm_limit=body.rpm_limit,
            tpm_limit=body.tpm_limit,
            budget_tokens=body.budget_tokens,
        )
        db.add(vk)
        await db.commit()
        return {"key_id": vk.id, "key": raw, "prefix": prefix}  # raw 仅此一次


@router.post("/keys/{key_id}/deactivate")
async def deactivate_key(key_id: str, request: Request) -> dict:
    _require_admin(request)
    async with request.app.state.session_factory() as db:
        row = await db.execute(select(VirtualKey).where(VirtualKey.id == key_id))
        vk = row.scalar_one_or_none()
        if vk is None:
            raise GatewayError(404, "key not found", "invalid_request_error", "key_not_found")
        vk.is_active = False
        await db.commit()
        await invalidate(request.app.state.redis, vk.key_hash)  # 主动失效缓存:吊销立即生效
        return {"key_id": key_id, "is_active": False}


@router.get("/tenants/{tenant_id}/usage")
async def tenant_usage(tenant_id: str, request: Request) -> dict:
    _require_admin(request)
    async with request.app.state.session_factory() as db:
        total = await db.execute(
            select(
                func.count(UsageRecord.id),
                func.coalesce(func.sum(UsageRecord.total_tokens), 0),
            ).where(UsageRecord.tenant_id == tenant_id)
        )
        requests_n, tokens_n = total.one()
        by_model = await db.execute(
            select(
                UsageRecord.model,
                func.count(UsageRecord.id),
                func.coalesce(func.sum(UsageRecord.total_tokens), 0),
            )
            .where(UsageRecord.tenant_id == tenant_id)
            .group_by(UsageRecord.model)
        )
        return {
            "tenant_id": tenant_id,
            "requests": int(requests_n),
            "total_tokens": int(tokens_n),
            "by_model": {m: {"requests": int(c), "total_tokens": int(t)} for m, c, t in by_model.all()},
        }


class ChannelCreate(BaseModel):
    """运行时注册上游渠道(M4:AskBI/PolicyCompass 等自有 Agent 服务即渠道)。"""

    name: str
    base_url: str
    api_key: str = ""
    models: list[str] = []
    connect_timeout: float = 5.0
    read_timeout: float = 120.0


@router.post("/channels")
async def register_channel(body: ChannelCreate, request: Request) -> dict:
    """把任意 OpenAI 兼容服务注册为网关渠道(含自家 Agent 应用),无需重启。

    渠道健康状态(EMA/冷却)存 Redis —— 多副本共享;注册动作进程内生效。
    进程内测试请直接调 ChannelRouter.register(不走本端点)。
    """
    _require_admin(request)
    import httpx

    from app.router.failover import ChannelRouter
    from app.upstream.client import UpstreamClient

    router: ChannelRouter = request.app.state.channel_router
    if body.name in router.names:
        raise GatewayError(409, "channel already registered", "invalid_request_error", "channel_exists")

    client = UpstreamClient(
        body.base_url,
        body.api_key,
        connect_timeout=body.connect_timeout,
        read_timeout=body.read_timeout,
        trust_env=request.app.state.settings.llm_trust_env,
    )
    router.register(body.name, client, body.models)
    return {"channel": body.name, "models": body.models, "registered": True}
