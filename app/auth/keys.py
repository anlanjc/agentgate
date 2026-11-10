"""virtual key 鉴权:缓存优先,数据库兜底(LiteLLM "Life of a Request" 同款架构)。

路径:SHA-256(raw) → Redis GET vk:{hash} → miss → PG 查询 → 回填(SETEX 60s)。
请求路径零 DB 写(鉴权只读);负结果不缓存(可逆操作,避免误伤新签发 key)。
"""

import hashlib
import secrets
from dataclasses import dataclass

import redis.asyncio as aredis
from redis.exceptions import RedisError
from sqlalchemy import select

from app.db.models import Tenant, VirtualKey
from app.log import get_logger

logger = get_logger("auth")

KEY_PREFIX = "ag-"
CACHE_TTL_SECONDS = 60

# 降级语义(三分):鉴权的事实来源是 PG,Redis 只是快路径 —— 缓存读/写失败都
# 降级为直查 DB(网关不死);key 能判定时按事实回 401;PG 也不可用 = "无法判定"
# (KeyStoreUnavailable → 上层 503 auth_unavailable),与准入的 fail-closed 同侧:
# "查不了"不等于"不存在",不落 401 也不落 500,让客户端按 Retry-After 重试。


def hash_key(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()


def generate_key() -> tuple[str, str, str]:
    """返回 (raw_key, key_hash, key_prefix)。raw 只在签发时可见一次。"""
    raw = KEY_PREFIX + secrets.token_urlsafe(24)
    return raw, hash_key(raw), raw[:10]


@dataclass
class AuthResult:
    key_id: str
    tenant_id: str
    model_whitelist: list | None
    rpm_limit: int
    tpm_limit: int
    budget_tokens: int | None
    cache_hit: bool
    # 租户级预算(P2:堵多 key 绕预算)。随 key 缓存:TTL 60s 内租户预算变更
    # 有最长 60s 的生效延迟 —— 预算是计量控制非安全边界,该窗口可接受(admin
    # 设置预算时主动重建计数器,limit 读数偏差以账本对账为准)。
    tenant_budget_tokens: int | None = None
    # 每 key 并发上限(NULL=不限);同样随缓存有 60s 变更窗口(保护闸非计费)
    concurrency_limit: int | None = None


class KeyNotFound(Exception):
    pass


class KeyInactive(Exception):
    pass


class KeyStoreUnavailable(Exception):
    """key 存储不可用(断连/超时):无法判定 key 有效性,由上层映射 503。"""


def _cache_key(key_hash: str) -> str:
    return f"vk:{key_hash}"


def _serialize(vk: VirtualKey, tenant_budget_tokens: int | None) -> str:
    import json

    return json.dumps(
        {
            "id": vk.id,
            "tenant_id": vk.tenant_id,
            "model_whitelist": vk.model_whitelist,
            "rpm_limit": vk.rpm_limit,
            "tpm_limit": vk.tpm_limit,
            "budget_tokens": vk.budget_tokens,
            "is_active": vk.is_active,
            "tenant_budget_tokens": tenant_budget_tokens,
            "concurrency_limit": vk.concurrency_limit,
        }
    )


async def lookup(
    redis: aredis.Redis,
    session_factory,
    raw_key: str,
) -> AuthResult:
    """缓存优先:命中则零 DB 资源(调优:旧版无条件开 session,压测定位后改为 miss 才开)。"""
    import json

    key_hash = hash_key(raw_key)

    try:
        cached = await redis.get(_cache_key(key_hash))
    except RedisError as exc:
        logger.warning("auth_cache_read_degraded", error=str(exc))
        cached = None
    if cached:
        data = json.loads(cached)
        if not data["is_active"]:
            raise KeyInactive(raw_key[:10])
        return AuthResult(
            key_id=data["id"],
            tenant_id=data["tenant_id"],
            model_whitelist=data["model_whitelist"],
            rpm_limit=data["rpm_limit"],
            tpm_limit=data["tpm_limit"],
            budget_tokens=data["budget_tokens"],
            cache_hit=True,
            tenant_budget_tokens=data.get("tenant_budget_tokens"),
            concurrency_limit=data.get("concurrency_limit"),
        )

    try:
        async with session_factory() as db:
            row = await db.execute(select(VirtualKey).where(VirtualKey.key_hash == key_hash))
            vk = row.scalar_one_or_none()
            if vk is None:
                raise KeyNotFound(raw_key[:10])
            if not vk.is_active:
                raise KeyInactive(raw_key[:10])
            # 租户预算随 miss 一次性带出(同会话 join 语义,不多一次往返)
            tb = await db.execute(
                select(Tenant.budget_tokens).where(Tenant.id == vk.tenant_id)
            )
            tenant_budget = tb.scalar_one_or_none()
    except (KeyNotFound, KeyInactive):
        raise
    except Exception as exc:
        # PG 故障域混沌(2026-11-11 P1)实锤:此前 DB 断连异常穿透全链 → 裸 500
        # (无信封、无 Retry-After)。DB 故障是"无法判定"而非"key 无效",上抛由
        # 路由层映射 503 auth_unavailable 统一信封。
        logger.warning("auth_store_unavailable", error=str(exc))
        raise KeyStoreUnavailable(str(exc)) from exc

    try:
        await redis.set(
            _cache_key(key_hash), _serialize(vk, tenant_budget), ex=CACHE_TTL_SECONDS
        )  # 请求路径唯一一次 Redis 写:缓存回填(LiteLLM 同款)
    except RedisError as exc:
        # 鉴权已成功(来源 DB),回填失败只损失快路径,不阻塞请求。
        logger.warning("auth_cache_backfill_failed", error=str(exc))
    return AuthResult(
        key_id=vk.id,
        tenant_id=vk.tenant_id,
        model_whitelist=vk.model_whitelist,
        rpm_limit=vk.rpm_limit,
        tpm_limit=vk.tpm_limit,
        budget_tokens=vk.budget_tokens,
        cache_hit=False,
        tenant_budget_tokens=tenant_budget,
        concurrency_limit=vk.concurrency_limit,
    )


async def invalidate(redis: aredis.Redis, key_hash: str) -> None:
    """管理端变更(吊销/改额)时主动失效缓存,保证 60s TTL 内的一致性窗口可消除。"""
    await redis.delete(_cache_key(key_hash))
