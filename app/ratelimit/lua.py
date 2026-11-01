"""Lua 脚本执行辅助:EVALSHA 优先(脚本 SHA 进程内缓存),NOSCRIPT 时回退加载。

为什么:朴素 EVAL 每次请求都上传整段脚本文本,Redis 每次重新编译 ——
压测定位出的每请求 CPU 开销之一(EVIDENCE 调优前后对比)。
"""

import redis.asyncio as aredis

_SHA_CACHE: dict[str, str] = {}


async def eval_cached(redis: aredis.Redis, script: str, numkeys: int, *args):
    sha = _SHA_CACHE.get(script)
    if sha is None:
        sha = await redis.script_load(script)
        _SHA_CACHE[script] = sha
    try:
        return await redis.evalsha(sha, numkeys, *args)
    except Exception as exc:
        if "NOSCRIPT" in str(exc):  # Redis 重启/SCRIPT FLUSH 后 SHA 失效
            sha = await redis.script_load(script)
            _SHA_CACHE[script] = sha
            return await redis.evalsha(sha, numkeys, *args)
        raise
