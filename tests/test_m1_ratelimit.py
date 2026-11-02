"""M1 验收:令牌桶限流 —— 原子性、零超卖、补给、路由级 429。"""

import asyncio

from app.ratelimit.token_bucket import consume
from tests.conftest import auth_headers


def _body(**extra) -> dict:
    body = {"model": "mock-model", "messages": [{"role": "user", "content": "hi"}]}
    body.update(extra)
    return body


async def test_bucket_exhaustion_and_retry_after(real_redis):
    key = "rl:test-exhaust"
    results = [await consume(real_redis, key, capacity=3) for _ in range(3)]
    assert all(r.allowed for r in results)
    denied = await consume(real_redis, key, capacity=3)
    assert not denied.allowed
    assert denied.retry_after_ms > 0


async def test_bucket_refill(real_redis):
    """高补给速率下短睡即可回填(容量 100 / 速率 1000/s,测试注入)。

    确定性设计:一次大额扣减(100)清空 → 短睡回填 ~60 → 50 可过、200 不可过。
    不用串行小额定额消耗(消耗期间一直在回填,非确定)。
    """
    key = "rl:test-refill"
    r = await consume(real_redis, key, capacity=100, per_second=1000.0, amount=100)
    assert r.allowed and r.remaining == 0
    assert not (
        await consume(real_redis, key, capacity=100, per_second=1000.0, amount=50)
    ).allowed  # 立即再扣:额度不足
    await asyncio.sleep(0.06)  # ~60 token 补给
    r2 = await consume(real_redis, key, capacity=100, per_second=1000.0, amount=50)
    assert r2.allowed
    r3 = await consume(real_redis, key, capacity=100, per_second=1000.0, amount=200)
    assert not r3.allowed  # 超过容量上限,即使回填也拒绝


async def test_bucket_concurrent_zero_oversell(real_redis):
    """并发 100 × 容量 50:恰好放行 50,拒绝 50 —— 硬上界零超卖(原子性证据)。"""
    key = "rl:test-concurrent"
    results = await asyncio.gather(*[consume(real_redis, key, capacity=50) for _ in range(100)])
    allowed = sum(1 for r in results if r.allowed)
    assert allowed == 50, f"零超卖破约:allowed={allowed}"


async def test_bucket_never_negative(real_redis):
    """桶余量永不为负(容量 5,并发 40)。"""
    key = "rl:test-negative"
    results = await asyncio.gather(*[consume(real_redis, key, capacity=5) for _ in range(40)])
    allowed = sum(1 for r in results if r.allowed)
    assert allowed == 5


async def test_route_rpm_429(auth_gateway):
    """路由级:专用 key(rpm=2)第 3 发被拒,带 Retry-After 头。"""
    client, app, mock, _ = auth_gateway
    created = (
        await client.post("/admin/tenants/t-test/keys", json={"name": "tight", "rpm_limit": 2})
    ).json()
    h = auth_headers(created["key"])
    r1 = await client.post("/v1/chat/completions", json=_body(), headers=h)
    r2 = await client.post("/v1/chat/completions", json=_body(), headers=h)
    assert r1.status_code == r2.status_code == 200
    r3 = await client.post("/v1/chat/completions", json=_body(), headers=h)
    assert r3.status_code == 429
    assert r3.json()["error"]["code"] == "rpm_exceeded"
    assert int(r3.headers["Retry-After"]) >= 1


# ---------- Redis 故障语义(P0-C):准入 fail-closed,鉴权降级 ----------


class _DeadRedis:
    """全故障注入:所有直连方法抛 redis.exceptions.ConnectionError(模拟宕机)。

    必须用 redis 自己的异常类型:代码层捕获 RedisError,redis-py 连不上时抛的
    正是它的 ConnectionError(RedisError 子类)——注入要贴近真实故障面。"""

    def __init__(self, real):
        from redis.exceptions import ConnectionError as RedisConnectionError

        self._exc = RedisConnectionError("redis down")
        self._real = real

    async def get(self, *a, **k):
        raise self._exc

    async def set(self, *a, **k):
        raise self._exc

    async def evalsha(self, *a, **k):
        raise self._exc

    async def script_load(self, *a, **k):
        raise self._exc

    def __getattr__(self, name):
        return getattr(self._real, name)


async def test_redis_down_admission_fails_closed(auth_gateway):
    """Redis 挂:准入 fail-closed 503(明确错误码+Retry-After),不是未处理 500。

    设计依据:准入承载计费硬上界(限流桶/TPM/预算),跳过 = 限额全部失效,
    宁可拒绝也不裸放;与鉴权/缓存的降级语义相反(那些是只读快路径)。
    """
    client, app, mock, raw = auth_gateway
    app.state.redis = _DeadRedis(app.state.redis)
    r = await client.post("/v1/chat/completions", json=_body(), headers=auth_headers(raw))
    assert r.status_code == 503
    body = r.json()["error"]
    assert body["code"] == "admission_unavailable"
    assert int(r.headers["Retry-After"]) >= 1


class _AuthCacheDeadRedis:
    """部分故障注入:只挂鉴权缓存的 get/set,其余(Lua 准入等)放行 ——
    对应真实故障面:缓存不可用但 Lua 脚本可用(或反之),各路径独立降级。"""

    def __init__(self, real):
        from redis.exceptions import ConnectionError as RedisConnectionError

        self._exc = RedisConnectionError("redis down")
        self._real = real

    async def get(self, *a, **k):
        raise self._exc

    async def set(self, *a, **k):
        raise self._exc

    def __getattr__(self, name):
        return getattr(self._real, name)


async def test_redis_down_auth_degrades_to_db(auth_gateway):
    """鉴权缓存挂:降级直查 DB(事实来源),请求照常成功——缓存读/回填失败不阻塞。

    注意与 fail-closed 的边界:准入(Lua)在此可用;若准入也不可用,上一条测试
    证明会 503。两条故障面独立降级,语义各自成立。
    """
    client, app, mock, raw = auth_gateway
    app.state.redis = _AuthCacheDeadRedis(app.state.redis)
    r = await client.post("/v1/chat/completions", json=_body(), headers=auth_headers(raw))
    assert r.status_code == 200
