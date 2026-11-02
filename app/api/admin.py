"""管理端 API:租户 / virtual key 签发与吊销 / 用量对账。

安全:settings.admin_token 非空时校验 X-Admin-Token(生产必配);为空仅限本机开发。
raw key 只在签发响应中出现一次,服务端只存哈希(泄露库≠泄露可用 key)。
"""

import secrets
import uuid
from datetime import UTC, datetime

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from app.auth.keys import generate_key, invalidate
from app.db.models import AuditLog, Tenant, UsageRecord, VirtualKey
from app.errors import GatewayError

router = APIRouter(prefix="/admin")


def _require_admin(request: Request) -> None:
    token = request.app.state.settings.admin_token
    supplied = request.headers.get("x-admin-token", "")
    # 常量时间比较:避免 token 前缀逐字节探测。
    if token and not secrets.compare_digest(supplied, token):
        raise GatewayError(401, "admin token invalid", "authentication_error", "admin_unauthorized")


def _parse_dt(value: str | None, field: str) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        raise GatewayError(400, f"invalid {field} timestamp", "invalid_request_error", "invalid_timestamp")
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


class TenantCreate(BaseModel):
    name: str = Field(min_length=1, max_length=128)
    webhook_url: str | None = None


class KeyCreate(BaseModel):
    name: str = ""
    rpm_limit: int = Field(default=60, ge=1)
    tpm_limit: int = Field(default=100_000, ge=1)
    budget_tokens: int | None = Field(default=None, ge=0)
    model_whitelist: list[str] | None = None


def _audit(db, action: str, target_type: str, target_id: str, detail: dict | None = None) -> None:
    """审计行与业务变更同事务(add 不提交,由端点现有 commit 一并落库)。"""
    from app.db.models import AuditLog

    db.add(
        AuditLog(
            actor="admin", action=action, target_type=target_type,
            target_id=target_id[:64], detail=detail,
        )
    )


@router.post("/tenants")
async def create_tenant(body: TenantCreate, request: Request) -> dict:
    _require_admin(request)
    async with request.app.state.session_factory() as db:
        tenant = Tenant(id=uuid.uuid4().hex[:32], name=body.name, webhook_url=body.webhook_url)
        db.add(tenant)
        _audit(db, "tenant.create", "tenant", tenant.id, {"name": body.name})
        try:
            await db.commit()
        except IntegrityError:
            await db.rollback()
            raise GatewayError(
                409, "tenant name already exists", "invalid_request_error", "tenant_exists"
            )
        return {"id": tenant.id, "name": tenant.name, "webhook_url": tenant.webhook_url}


class TenantBudgetUpdate(BaseModel):
    """租户级 token 预算(NULL=不限):key 预算的兜底,堵"多签几把 key 绕预算"。"""

    budget_tokens: int | None = Field(default=None, ge=0)


@router.post("/tenants/{tenant_id}/budget")
async def set_tenant_budget(tenant_id: str, body: TenantBudgetUpdate, request: Request) -> dict:
    """设置租户预算;计数器即时从账本校准(设置的是限额,已花费是账本事实)。

    已缓存 key 的预算读数最长 60s 后生效(auth 缓存 TTL;预算是计量控制非安全
    边界,窗口可接受)—— 新签发的 key 立即生效。
    """
    _require_admin(request)
    async with request.app.state.session_factory() as db:
        row = await db.execute(select(Tenant).where(Tenant.id == tenant_id))
        tenant = row.scalar_one_or_none()
        if tenant is None:
            raise GatewayError(404, "tenant not found", "invalid_request_error", "tenant_not_found")
        tenant.budget_tokens = body.budget_tokens
        from app.quota.service import rebuild_tenant_from_db

        spent = await rebuild_tenant_from_db(request.app.state.redis, db, tenant_id)
        _audit(db, "tenant.budget_set", "tenant", tenant_id, {"budget_tokens": body.budget_tokens})
        await db.commit()
    return {"tenant_id": tenant_id, "budget_tokens": body.budget_tokens, "spent_tokens": spent}


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
        # detail 只放 prefix:raw key 绝不入审计(泄露审计=泄露可用 key)
        _audit(db, "key.issue", "virtual_key", vk.id, {"tenant_id": tenant_id, "prefix": prefix})
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
        _audit(db, "key.revoke", "virtual_key", key_id, {"prefix": vk.key_prefix})
        await db.commit()
        await invalidate(request.app.state.redis, vk.key_hash)  # 主动失效缓存:吊销立即生效
        return {"key_id": key_id, "is_active": False}


@router.get("/tenants/{tenant_id}/usage")
async def tenant_usage(
    tenant_id: str,
    request: Request,
    start: str | None = None,
    end: str | None = None,
) -> dict:
    """租户用量对账;start/end 为 ISO-8601,左闭右开。

    生产账单不会扫全表:默认时间窗由调用方给,底层走 (tenant_id, created_at)
    复合索引。全量聚合仍可用(不传参数),但大租户应改用日汇总表。
    """
    _require_admin(request)
    start_dt = _parse_dt(start, "start")
    end_dt = _parse_dt(end, "end")
    if start_dt and end_dt and start_dt >= end_dt:
        raise GatewayError(400, "start must be before end", "invalid_request_error", "invalid_time_range")

    filters = [UsageRecord.tenant_id == tenant_id]
    if start_dt:
        filters.append(UsageRecord.created_at >= start_dt)
    if end_dt:
        filters.append(UsageRecord.created_at < end_dt)

    async with request.app.state.session_factory() as db:
        total = await db.execute(
            select(
                func.count(UsageRecord.id),
                func.coalesce(func.sum(UsageRecord.total_tokens), 0),
            ).where(*filters)
        )
        requests_n, tokens_n = total.one()
        by_model = await db.execute(
            select(
                UsageRecord.model,
                func.count(UsageRecord.id),
                func.coalesce(func.sum(UsageRecord.total_tokens), 0),
            )
            .where(*filters)
            .group_by(UsageRecord.model)
        )
        return {
            "tenant_id": tenant_id,
            "start": start_dt.isoformat() if start_dt else None,
            "end": end_dt.isoformat() if end_dt else None,
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

    双写:运行时注册(立即生效)+ channels 表持久化(重启由 load_channels_from_db 恢复)。
    进程内测试请直接调 ChannelRouter.register(不走本端点)。
    """
    _require_admin(request)

    import uuid as _uuid

    from app.db.models import Channel
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

    # 持久化(ADR-005):重启恢复;超时参数也必须入库,否则重启后静默回默认值。
    # 密钥静态加密(P2 hardening):入库永远是密文(无主密钥的 dev 口径原样),
    # 运行时客户端持有的仍是调用方传入的明文 —— 明文不落库。
    from app.security.crypto import encrypt_secret

    stored_key = encrypt_secret(body.api_key)
    async with request.app.state.session_factory() as db:
        from sqlalchemy import select

        row = await db.execute(select(Channel).where(Channel.name == body.name))
        ch = row.scalar_one_or_none()
        try:
            if ch is None:
                db.add(
                    Channel(
                        id=_uuid.uuid4().hex[:32], name=body.name, base_url=body.base_url,
                        api_key=stored_key, models=body.models,
                        connect_timeout=body.connect_timeout, read_timeout=body.read_timeout,
                    )
                )
            else:
                ch.base_url, ch.api_key, ch.models, ch.is_active = (
                    body.base_url, stored_key, body.models, True,
                )
                ch.connect_timeout, ch.read_timeout = body.connect_timeout, body.read_timeout
            # 审计 detail 不放 api_key(密文也不放):渠道凭据不入任何日志/审计面
            _audit(db, "channel.register", "channel", body.name, {"models": body.models})
            await db.commit()
        except IntegrityError:
            await db.rollback()
            raise GatewayError(409, "channel already persisted", "invalid_request_error", "channel_exists")
    return {
        "channel": body.name,
        "models": body.models,
        "connect_timeout": body.connect_timeout,
        "read_timeout": body.read_timeout,
        "registered": True,
        "persisted": True,
    }


@router.delete("/cache")
async def purge_response_cache(request: Request) -> dict:
    """全量清空精确响应缓存(TTL 惰性失效之外的主动手段;无按 key 精确失效 —— 见 response.py 语义)。"""
    _require_admin(request)
    removed = 0
    async for key in request.app.state.redis.scan_iter(match="resp:*", count=500):
        await request.app.state.redis.delete(key)
        removed += 1
    async with request.app.state.session_factory() as db:
        _audit(db, "cache.purge", "response_cache", "*", {"removed": removed})
        await db.commit()
    return {"purged": removed}


@router.get("/audit")
async def list_audit(request: Request, limit: int = 100) -> dict:
    """管理面审计流水(只追加):谁/何时/对什么/做了什么,排障与合规的第一手证据。"""
    _require_admin(request)
    limit = max(1, min(limit, 500))
    async with request.app.state.session_factory() as db:
        rows = (
            await db.execute(
                select(AuditLog).order_by(AuditLog.id.desc()).limit(limit)
            )
        ).scalars().all()
    return {
        "entries": [
            {
                "id": r.id, "actor": r.actor, "action": r.action,
                "target_type": r.target_type, "target_id": r.target_id,
                "detail": r.detail, "created_at": r.created_at.isoformat(),
            }
            for r in rows
        ]
    }


@router.get("/channels")
async def list_channels(request: Request) -> dict:
    """渠道注册表 + 运行态健康(EMA/冷却/连续失败)—— 管理界面与排障第一屏。"""
    _require_admin(request)
    import time

    from app.router.failover import _state_key

    router = request.app.state.channel_router
    now_ms = int(time.time() * 1000)
    out = []
    for name in router.names:
        raw = await request.app.state.redis.hgetall(_state_key(name))
        out.append(
            {
                "name": name,
                "models": router.models_of(name),
                "ema_ms": round(float(raw.get("ema_ms", 1000) or 1000), 1),
                "cooling": int(raw.get("cooldown_until_ms", 0) or 0) > now_ms,
                "fail_streak": int(raw.get("fail_streak", 0) or 0),
            }
        )
    return {"channels": out}


@router.get("/tenants")
async def list_tenants(request: Request) -> dict:
    _require_admin(request)
    async with request.app.state.session_factory() as db:
        from sqlalchemy import select

        from app.db.models import Tenant as T

        rows = (await db.execute(select(T.id, T.name, T.webhook_url).order_by(T.created_at))).all()
        return {"tenants": [{"id": r[0], "name": r[1], "webhook_url": r[2]} for r in rows]}


@router.get("/tenants/{tenant_id}/keys")
async def list_keys(tenant_id: str, request: Request) -> dict:
    """租户的 key 清单(只有前缀与元数据,无 raw key —— 哈希存储纪律)。"""
    _require_admin(request)
    async with request.app.state.session_factory() as db:
        from sqlalchemy import select

        from app.db.models import VirtualKey as K

        rows = (
            await db.execute(
                select(K.id, K.key_prefix, K.name, K.rpm_limit, K.tpm_limit, K.budget_tokens, K.is_active)
                .where(K.tenant_id == tenant_id)
                .order_by(K.created_at)
            )
        ).all()
        return {
            "keys": [
                {
                    "key_id": r[0], "prefix": r[1], "name": r[2],
                    "rpm_limit": r[3], "tpm_limit": r[4],
                    "budget_tokens": r[5], "is_active": r[6],
                }
                for r in rows
            ]
        }
