"""管理台演示栈:起网关(mock 上游)+ 预置演示数据(租户/用量/渠道状态),供浏览器截图与试用。

运行:python scripts/demo_admin.py  (Ctrl+C 或 kill 退出;端口 8123)
预置:租户 acme/northwind,各 3-5 条用量;渠道 mock(健康);演示 key 签发过 2 把。
"""

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import os

os.chdir(ROOT)


async def main() -> None:
    import httpx
    import uvicorn

    from app.api.main import create_app
    from app.auth.keys import generate_key
    from app.config import Settings
    from app.db.models import Tenant, VirtualKey
    from app.db.session import build_session_factory, init_db
    from app.upstream.client import UpstreamClient
    from tests.mock_upstream import build_mock_upstream

    for f in ("demo_admin.db",):
        if os.path.exists(f):
            os.remove(f)

    settings = Settings(
        upstream_name="mock",
        upstream_base_url="http://mock/v1",
        upstream_api_key="sk-demo",
        models=["mock-model", "qwen-flash"],
        db_url="sqlite+aiosqlite:///./demo_admin.db",
        redis_url="redis://127.0.0.1:6379/6",
        auth_enabled=True,
        response_cache_enabled=True,  # 演示口径:Playground 可现场看到 MISS→HIT
        log_level="WARNING",
    )
    import redis.asyncio as aredis

    redis = aredis.from_url(settings.redis_url, decode_responses=True, protocol=2)
    await redis.flushdb()
    mock = build_mock_upstream()
    upstream = UpstreamClient(
        settings.upstream_base_url,
        settings.upstream_api_key,
        client=httpx.AsyncClient(
            transport=httpx.ASGITransport(app=mock), base_url=settings.upstream_base_url
        ),
    )
    app = create_app(settings=settings, upstream=upstream, redis_client=redis)
    await init_db(app.state.engine)

    # 租户 + key
    seeds = [("t-acme", "acme", None), ("t-nw", "northwind", None)]
    keys = {}
    async with build_session_factory(app.state.engine)() as db:
        for tid, name, hook in seeds:
            db.add(Tenant(id=tid, name=name, webhook_url=hook))
        await db.commit()
        for tid, _, _ in seeds:
            raw, kh, px = generate_key()
            db.add(
                VirtualKey(
                    id=f"k-{tid}", tenant_id=tid, key_hash=kh, key_prefix=px,
                    name="demo", rpm_limit=10_000, tpm_limit=10_000_000,
                )
            )
            keys[tid] = raw
        await db.commit()

    # 渠道注册(演示口径:mock 一个健康渠道 + 一个冷却渠道状态由后续失败注入)
    app.state.channel_router.register("dashscope", upstream, ["mock-model", "qwen-flash"])

    # 产生用量:acme 5 次,northwind 2 次
    config = uvicorn.Config(app, host="127.0.0.1", port=8123, log_level="error", access_log=False)
    server = uvicorn.Server(config)
    server_task = asyncio.get_running_loop().create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.05)

    async with httpx.AsyncClient(base_url="http://127.0.0.1:8123", timeout=30) as c:
        for tid, raw in keys.items():
            n = 5 if tid == "t-acme" else 2
            for i in range(n):
                r = await c.post(
                    "/v1/chat/completions",
                    json={"model": "mock-model", "messages": [{"role": "user", "content": f"q{i}"}]},
                    headers={"Authorization": f"Bearer {raw}"},
                )
                assert r.status_code == 200, r.text
    await app.state.task_queue.flush()
    print("管理台演示栈就绪:http://127.0.0.1:8123/admin/web")

    try:
        await server_task
    except asyncio.CancelledError:
        pass
    finally:
        await upstream.aclose()
        await app.state.task_queue.stop()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
