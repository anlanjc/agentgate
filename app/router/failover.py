"""渠道注册表与故障转移状态机(师承:LiteLLM routing/reliability)。

状态全部存 Redis hash(多副本一致 —— 与限流同一个"为什么必须 Redis"的理由):
    gate:ch:{name} = {ema_ms, cooldown_until_ms, fail_streak}

状态机:
- 成功:EMA 更新(0.7 旧 + 0.3 新),fail_streak 清零 —— 原子 Lua 读改写(单往返,调优产物);
- 429:立即冷却至 now + Retry-After(尊重上游指令,LiteLLM 同款);
- 5xx/连接失败/读超时:连续失败 +1,冷却 60s(LiteLLM cooldown_time 缺省值);
- 候选排序:健康渠道按 EMA 升序(延迟优先路由),冷却中的渠道不可见;
- Redis 不可用:降级为无状态路由(网关不死,限流层另有兜底)。
"""

import time

import redis.asyncio as aredis

from app.log import get_logger
from app.ratelimit.lua import eval_cached

logger = get_logger("router")

COOLDOWN_MS = 60_000  # LiteLLM cooldown_time 默认 60s
DEFAULT_EMA_MS = 1000.0
EMA_ALPHA = 0.3

# KEYS[1]=渠道状态键;ARGV[1]=本次延迟ms —— 原子 EMA 读改写(HMGET+计算+HSET 合一次往返)
EMA_SUCCESS_LUA = """
local st = redis.call('HMGET', KEYS[1], 'ema_ms', 'fail_streak')
local ema = tonumber(st[1]) or 1000.0
local latency = tonumber(ARGV[1])
local new_ema = ema * 0.7 + latency * 0.3
redis.call('HSET', KEYS[1], 'ema_ms', new_ema, 'fail_streak', 0)
"""


def _state_key(name: str) -> str:
    return f"gate:ch:{name}"


class ChannelRouter:
    def __init__(self, redis: aredis.Redis) -> None:
        self._redis = redis
        self._clients: dict[str, object] = {}
        self._models: dict[str, list] = {}

    def register(self, name: str, client, models: list | None = None) -> None:
        """注册渠道(进程内注册;状态在 Redis,跨副本共享)。"""
        self._clients[name] = client
        self._models[name] = models or []

    def client(self, name: str):
        return self._clients[name]

    @property
    def names(self) -> list[str]:
        return list(self._clients)

    async def _state(self, name: str) -> dict:
        try:
            raw = await self._redis.hgetall(_state_key(name))
        except Exception as e:  # Redis 不可用:降级为无状态默认(网关不死,限流层另有兜底)
            logger.warning("router_state_degraded", channel=name, error=str(e))
            raw = {}
        return {
            "ema_ms": float(raw.get("ema_ms", DEFAULT_EMA_MS)),
            "cooldown_until_ms": int(raw.get("cooldown_until_ms", 0)),
            "fail_streak": int(raw.get("fail_streak", 0)),
        }

    async def candidates(self) -> list[str]:
        """健康渠道按 EMA 升序(延迟优先路由);状态读取走 pipeline(单往返)。"""
        now_ms = int(time.time() * 1000)
        try:
            pipe = self._redis.pipeline(transaction=False)
            for name in self._clients:
                pipe.hgetall(_state_key(name))
            states = await pipe.execute()
        except Exception as e:  # Redis 不可用:降级为无状态
            logger.warning("router_state_degraded", error=str(e))
            return list(self._clients)
        healthy: list[tuple[float, str]] = []
        for name, raw in zip(self._clients, states):
            cooldown_until_ms = int(raw.get("cooldown_until_ms", 0) or 0)
            if cooldown_until_ms > now_ms:
                continue
            ema_ms = float(raw.get("ema_ms", DEFAULT_EMA_MS) or DEFAULT_EMA_MS)
            healthy.append((ema_ms, name))
        return [n for _, n in sorted(healthy)]

    async def mark_success(self, name: str, latency_ms: float) -> None:
        try:
            await eval_cached(self._redis, EMA_SUCCESS_LUA, 1, _state_key(name), latency_ms)
        except Exception as e:
            logger.warning("router_mark_degraded", channel=name, error=str(e))

    async def mark_rate_limited(self, name: str, retry_after_s: int) -> None:
        until = int(time.time() * 1000) + max(1, retry_after_s) * 1000
        await self._safe_hset(name, {"cooldown_until_ms": until})
        logger.warning("channel_cooling_429", channel=name, retry_after_s=retry_after_s)

    async def mark_failure(self, name: str) -> None:
        st = await self._state(name)
        streak = st["fail_streak"] + 1
        until = int(time.time() * 1000) + COOLDOWN_MS
        await self._safe_hset(
            name, {"fail_streak": streak, "cooldown_until_ms": until}
        )
        logger.warning("channel_cooling_failure", channel=name, fail_streak=streak)

    async def _safe_hset(self, name: str, mapping: dict) -> None:
        try:
            await self._redis.hset(_state_key(name), mapping=mapping)
        except Exception as e:
            logger.warning("router_mark_degraded", channel=name, error=str(e))

    async def state_of(self, name: str) -> dict:
        return await self._state(name)
