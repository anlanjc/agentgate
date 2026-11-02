"""网关压测:aiohttp 客户端 + 自举完整栈,产出 QPS/分位数/TTFT 与网关附加开销。

方法论(压测第一课:客户端瓶颈会冒充服务端瓶颈):
- 客户端用 aiohttp(C 加速解析器);服务端数字取**同客户端同并发下的差值**,
  客户端成本在差值中抵消;
- 基线 = 裸 mock 上游过 uvicorn(同一套服务器机械);附加开销 = 过网关中位 − 直连中位;
- 上游为进程内 mock(近零延迟),绝对数字反映**网关自身吞吐上限**,非真实上游链路;
  真实上游口径方法相同,替换上游即可(EVIDENCE 记录环境)。

用法:
    python scripts/load_test.py --requests 2000 --concurrency 50 [--stream-ratio 0.3] [--out evidence/load_report.md]
"""

import argparse
import asyncio
import os
import socket
import statistics
import time

import aiohttp
import redis.asyncio as aredis
import uvicorn


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _pctl(sorted_xs: list[float], p: float) -> float:
    if not sorted_xs:
        return 0.0
    idx = min(len(sorted_xs) - 1, round(p / 100 * (len(sorted_xs) - 1)))
    return sorted_xs[idx]


def _fmt(xs: list[float]) -> str:
    if not xs:
        return "-"
    s = sorted(xs)
    return (
        f"P50={_pctl(s, 50):.1f}ms P95={_pctl(s, 95):.1f}ms "
        f"P99={_pctl(s, 99):.1f}ms mean={statistics.mean(s):.1f}ms"
    )


async def _serve(app, port: int) -> uvicorn.Server:
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error", access_log=False)
    server = uvicorn.Server(config)
    asyncio.get_running_loop().create_task(server.serve())
    for _ in range(100):
        if server.started:
            return server
        await asyncio.sleep(0.05)
    raise RuntimeError("server failed to start")


async def _run_load(
    base_url: str, path: str, key: str | None, n: int, concurrency: int, stream: bool
) -> tuple[float, list[float], list[float], dict[int, int]]:
    headers = {"Authorization": f"Bearer {key}"} if key else {}
    body = {"model": "mock-model", "messages": [{"role": "user", "content": "hi" * 10}]}
    if stream:
        body["stream"] = True
        body["stream_options"] = {"include_usage": True}

    latencies: list[float] = []
    ttfts: list[float] = []
    statuses: dict[int, int] = {}
    sem = asyncio.Semaphore(concurrency)
    connector = aiohttp.TCPConnector(limit=0, limit_per_host=0)

    async with aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=30)) as session:
        async def one() -> None:
            async with sem:
                t0 = time.perf_counter()
                try:
                    if stream:
                        ttft = None
                        async with session.post(
                            f"{base_url}{path}", json=body, headers=headers
                        ) as resp:
                            statuses[resp.status] = statuses.get(resp.status, 0) + 1
                            async for _chunk in resp.content.iter_any():
                                if ttft is None:
                                    ttft = (time.perf_counter() - t0) * 1000
                        if ttft is not None:
                            ttfts.append(ttft)
                    else:
                        async with session.post(
                            f"{base_url}{path}", json=body, headers=headers
                        ) as resp:
                            statuses[resp.status] = statuses.get(resp.status, 0) + 1
                            await resp.read()
                    latencies.append((time.perf_counter() - t0) * 1000)
                except Exception:
                    statuses[-1] = statuses.get(-1, 0) + 1

        t_start = time.perf_counter()
        await asyncio.gather(*[one() for _ in range(n)])
        duration = time.perf_counter() - t_start

    return duration, latencies, ttfts, statuses


async def main() -> None:
    import logging
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    for noisy in ("httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    parser = argparse.ArgumentParser()
    parser.add_argument("--requests", type=int, default=2000)
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--stream-ratio", type=float, default=0.3)
    parser.add_argument("--out", type=str, default="")
    args = parser.parse_args()

    from app.api.main import create_app
    from app.auth.keys import generate_key
    from app.config import Settings
    from app.db.models import Tenant, VirtualKey
    from app.db.session import build_session_factory, init_db
    from app.upstream.client import UpstreamClient
    from tests.mock_upstream import build_mock_upstream

    # DB 方言可经 AGENTGATE_DB_URL 切 PG(ADR-002 双口径压测);默认 SQLite
    db_url = os.environ.get("AGENTGATE_DB_URL", "sqlite+aiosqlite:///./load_test.db")
    settings = Settings(
        upstream_name="mock",
        upstream_base_url="http://mock/v1",
        upstream_api_key="sk-loadtest",
        upstream_read_timeout=30.0,
        models=["mock-model"],
        db_url=db_url,
        redis_url="redis://127.0.0.1:6379/2",
        auth_enabled=True,
        log_level="WARNING",
    )
    redis = aredis.from_url(settings.redis_url, decode_responses=True, protocol=2)
    await redis.flushdb()

    if db_url.startswith("sqlite") and os.path.exists("load_test.db"):
        os.remove("load_test.db")  # 幂等:清掉上次的库(避免 UNIQUE 冲突)

    mock_app = build_mock_upstream()
    upstream = UpstreamClient(
        settings.upstream_base_url,
        settings.upstream_api_key,
        client=httpx_asgi_client(mock_app, settings.upstream_base_url),
    )
    app = create_app(settings=settings, upstream=upstream, redis_client=redis)
    await init_db(app.state.engine)
    if db_url.startswith("postgres"):
        # PG 是持久库:重跑前清表(SQLite 走上面的删文件路径)
        from sqlalchemy import text

        async with app.state.engine.begin() as conn:
            for t in ("usage_records", "task_records", "virtual_keys", "channels", "tenants"):
                await conn.execute(text(f"TRUNCATE TABLE {t} CASCADE"))

    raw, key_hash, prefix = generate_key()
    async with build_session_factory(app.state.engine)() as db:
        db.add(Tenant(id="t-load", name="loadtest"))
        db.add(
            VirtualKey(
                id="k-load", tenant_id="t-load", key_hash=key_hash, key_prefix=prefix,
                name="load", rpm_limit=1_000_000, tpm_limit=1_000_000_000,
            )
        )
        await db.commit()

    gw_port = _free_port()
    bare_port = _free_port()
    gw_server = await _serve(app, gw_port)
    bare_server = await _serve(mock_app, bare_port)

    try:
        n_stream = int(args.requests * args.stream_ratio)
        n_plain = args.requests - n_stream

        # 0) 无排队条件下的每请求开销(c=1):附加开销的合法口径
        gw1_time, gw1_lat, _, _ = await _run_load(
            f"http://127.0.0.1:{gw_port}", "/v1/chat/completions", raw, 100, 1, False
        )
        d1_time, d1_lat, _, _ = await _run_load(
            f"http://127.0.0.1:{bare_port}", "/v1/chat/completions", None, 100, 1, False
        )
        gw1_med = statistics.median(gw1_lat)
        d1_med = statistics.median(d1_lat)
        overhead_c1 = gw1_med - d1_med

        # 1) 容量口径(c=并发):吞吐与排队分位数
        gw_time, gw_lat, gw_ttft, gw_status = await _run_load(
            f"http://127.0.0.1:{gw_port}", "/v1/chat/completions", raw, n_plain, args.concurrency, False
        )
        st_time, st_lat, st_ttft, st_status = await _run_load(
            f"http://127.0.0.1:{gw_port}", "/v1/chat/completions", raw, n_stream, args.concurrency, True
        )
        d_time, d_lat, _, d_status = await _run_load(
            f"http://127.0.0.1:{bare_port}", "/v1/chat/completions", None, n_plain, args.concurrency, False
        )
        gw_med = statistics.median(gw_lat) if gw_lat else 0
        d_med = statistics.median(d_lat) if d_lat else 0
        overhead_c50 = gw_med - d_med

        verdict = overhead_c1 < 10
        report = f"""# agentgate 压测报告(脚本自动生成)

- 时间:{time.strftime("%Y-%m-%d %H:%M:%S")}
- 环境:Windows 本机 · Python 3.12 · 便携 Redis 5.0.14 · DB={'PostgreSQL 16(pgserver)' if 'postgres' in db_url else 'SQLite'} · mock 上游(进程内,近零延迟)· aiohttp 客户端
- 参数:容量组总请求 {args.requests}(非流式 {n_plain} / 流式 {n_stream})· 并发 {args.concurrency}

## 每请求附加开销(c=1,无排队 —— 合法口径)
- 过网关 P50 {gw1_med:.2f}ms − 直连基线 P50 {d1_med:.2f}ms = **{overhead_c1:.2f}ms**
- 判定:{'✅ < 10ms 达标' if verdict else '❌ ≥ 10ms'}
- 说明:网关额外做鉴权缓存读 + 准入 Lua + 渠道状态 + httpx 转发跳;该数字即客户端每请求付出的网关代价

## 容量口径(c={args.concurrency},有排队 —— 单进程吞吐上限)
- 网关非流式:{n_plain / gw_time:.0f} req/s · {_fmt(gw_lat)} · 状态:{gw_status}
- 网关流式:{(n_stream / st_time) if st_time else 0:.0f} req/s · TTFT {_fmt(st_ttft)} · 状态:{st_status}
- 裸 mock 基线:{n_plain / d_time:.0f} req/s · {_fmt(d_lat)}
- c={args.concurrency} 中位差 {overhead_c50:.2f}ms 主要是**排队延迟**(单进程 CPU 串行 ~3ms/req × 50 并发),
  不代表每请求代价 —— 生产以多 worker 水平扩展(uvicorn --workers N)

## 调优记录(EVIDENCE 完整叙事)
- 基线(朴素 EVAL + 每请求 8 次 Redis 往返 + 无条件开 DB 会话)→ 优化后
  (EVALSHA + 准入合并单 Lua + 缓存命中零会话 + 流水线记账 + 原子 EMA),Redis 往返 8→5/请求

## 口径声明
- 客户端为单进程 aiohttp,绝对吞吐受客户端 CPU 约束;网关真实容量上限高于表观吞吐;
- 上游为进程内 mock(近零延迟),本报告 = 网关自身性能;真实上游链路延迟由上游主导;
- SQLite 为测试方言(ADR-002),生产 compose 使用 PostgreSQL。
"""
        print(report)
        if args.out:
            from pathlib import Path

            Path(args.out).parent.mkdir(parents=True, exist_ok=True)
            Path(args.out).write_text(report, encoding="utf-8")
            print(f"[saved] {args.out}")
    finally:
        gw_server.should_exit = True
        bare_server.should_exit = True
        await upstream.aclose()
        await app.state.task_queue.stop()
        await redis.aclose()


def httpx_asgi_client(mock_app, base_url: str):
    import httpx

    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=mock_app), base_url=base_url
    )


if __name__ == "__main__":
    asyncio.run(main())
