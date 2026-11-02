"""精确响应缓存(对标 Higress/LiteLLM 的 cache 层;默认关闭,按部署开启)。

设计要点:
- **租户隔离是安全边界**:缓存键含 tenant_id —— 租户 A 永远读不到租户 B 的缓存答案;
- **只缓确定性请求**:temperature ≤ 0(或未设)+ 非流式 + 无 tools(工具调用有副作用);
- **TTL 惰性失效**:无主动失效(上游答案不可主动作废,语义上是"快照缓存");
  管理端可全量清空(DELETE /admin/cache);
- 命中时记账照走,channel='cache' 标注 —— 租户账本完整,上游成本为零。
"""

import hashlib
import json

from app.config import Settings


def cache_enabled(settings: Settings, req) -> bool:
    if not settings.response_cache_enabled:
        return False
    if req.stream or req.tools:
        return False
    # 必须显式 temperature=0 才缓存。OpenAI 的默认温度是 1.0,把 None 当确定性
    # 会把随机答案缓存成"精确命中"——这是缓存正确性红线。
    return req.temperature is not None and req.temperature <= 0


def cache_key(tenant_id: str, model: str, payload: dict) -> str:
    """缓存键:租户 + 模型 + 归一化请求体(exclude stream/stream_options 传输字段)。

    user 参与键:同租户下不同终端用户不共享响应缓存(账号级隐私边界)。
    """
    normalized = {k: v for k, v in payload.items() if k not in ("stream", "stream_options")}
    digest = hashlib.sha256(
        json.dumps([tenant_id, model, normalized], sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()
    return f"resp:{digest}"


async def get(redis, key: str) -> dict | None:
    raw = await redis.get(key)
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except ValueError:
        return None


async def put(redis, key: str, body: dict, ttl: int) -> None:
    if ttl <= 0:
        return
    await redis.set(key, json.dumps(body, ensure_ascii=False), ex=ttl)
