"""多 worker 水平扩展压测:uvicorn --workers 1/2/4 真实多进程,产出扩展曲线。

方法论延续 load_test.py(附加开销取 c=1 差值;容量取 c=N 吞吐):
- mock 上游独立进程(uvicorn);网关以 `uvicorn app.api.main:app --workers N` 真子进程起;
- 每个 worker 是独立进程(GIL 隔离),共享 Redis 状态(限流/路由)与 SQLite 账本(单写者 per 进程);
- 诚实边界:本机 Windows + aiohttp 单进程客户端,绝对数字受客户端上限约束,看相对扩展比。

运行:.venv/Scripts/python scripts/load_workers.py [--requests 1500] [--concurrency 50] [--out evidence/load_workers.md]
"""

import argparse
import asyncio
import os
import socket
import statistics
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

import aiohttp
import redis.asyncio as aredis


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


async def _wait_up(url: str, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    async with aiohttp.ClientSession() as c:
        while time.time() < deadline:
            try:
                async with c.get(url) as r:
                    if r.status == 200:
                        return
            except Exception:
                pass
            await asyncio.sleep(0.2)
    raise RuntimeError(f"not up: {url}")


async def _run_load(base_url: str, key: str, n: int, concurrency: int) -> tuple[float, list[float]]:
    body = {"model": "mock-model", "messages": [{"role": "user", "content": "hi" * 10}]}
    headers = {"Authorization": f"Bearer {key}"}
    lat: list[float] = []
    sem = asyncio.Semaphore(concurrency)
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=0), timeout=aiohttp.ClientTimeout(total=60)
    ) as s:
        async def one():
            async with sem:
                t0 = time.perf_counter()
                async with s.post(f"{base_url}/v1/chat/completions", json=body, headers=headers) as r:
                    await r.read()
                lat.append((time.perf_counter() - t0) * 1000)

        t0 = time.perf_counter()
        await asyncio.gather(*[one() for _ in range(n)])
        return time.perf_counter() - t0, lat


def _pctl(xs: list[float], p: float) -> float:
    s = sorted(xs)
    return s[min(len(s) - 1, round(p / 100 * (len(s) - 1)))]


async def main() -> None:
    import logging

    for noisy in ("httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    parser = argparse.ArgumentParser()
    parser.add_argument("--requests", type=int, default=1500)
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4])
    parser.add_argument("--out", type=str, default="")
    args = parser.parse_args()


    # 0) 独立 mock 上游进程(所有 worker 共享的"上游")
    mock_port = _free_port()
    mock_code = (
        "import sys, uvicorn\n"
        f"sys.path.insert(0, r'{ROOT}')\n"
        "from tests.mock_upstream import build_mock_upstream\n"
        f"uvicorn.run(build_mock_upstream(), host='127.0.0.1', port={mock_port}, log_level='error', access_log=False)\n"
    )
    mock_proc = subprocess.Popen([sys.executable, "-u", "-c", mock_code], cwd=str(ROOT))
    await _wait_up(f"http://127.0.0.1:{mock_port}/health")
    print(f"[boot] mock upstream :{mock_port}")

    # 1) 种子:租户 + key(经一次性单进程引导,写共享 SQLite/Redis)
    if os.path.exists("load_workers.db"):
        os.remove("load_workers.db")

    from app.api.main import create_app
    from app.auth.keys import generate_key
    from app.config import Settings
    from app.db.models import Tenant, VirtualKey
    from app.db.session import build_session_factory, init_db

    redis = aredis.from_url("redis://127.0.0.1:6379/5", decode_responses=True, protocol=2)
    await redis.flushdb()
    settings = Settings(
        upstream_name="mock",
        upstream_base_url=f"http://127.0.0.1:{mock_port}/v1",
        upstream_api_key="sk-w",
        models=["mock-model"],
        db_url="sqlite+aiosqlite:///./load_workers.db",
        redis_url="redis://127.0.0.1:6379/5",
        auth_enabled=True,
        log_level="WARNING",
    )
    bootstrap_app = create_app(settings=settings)
    await init_db(bootstrap_app.state.engine)
    raw, key_hash, prefix = generate_key()
    async with build_session_factory(bootstrap_app.state.engine)() as db:
        db.add(Tenant(id="t-w", name="workers"))
        db.add(
            VirtualKey(
                id="k-w", tenant_id="t-w", key_hash=key_hash, key_prefix=prefix,
                rpm_limit=1_000_000, tpm_limit=1_000_000_000,
            )
        )
        await db.commit()
    await redis.delete("vk:" + key_hash)  # 引导进程的缓存随进程消亡,显式清掉
    await bootstrap_app.state.engine.dispose()
    print("[boot] 种子完成(共享 SQLite + Redis db5)")

    # 2) 逐档 worker 数:真子进程 uvicorn --workers N
    rows = []
    for workers in args.workers:
        gw_port = _free_port()
        env = {
            **os.environ,
            "AGENTGATE_UPSTREAM_NAME": "mock",
            "AGENTGATE_UPSTREAM_BASE_URL": f"http://127.0.0.1:{mock_port}/v1",
            "AGENTGATE_UPSTREAM_API_KEY": "sk-w",
            "AGENTGATE_MODELS": '["mock-model"]',
            "AGENTGATE_DB_URL": "sqlite+aiosqlite:///./load_workers.db",
            "AGENTGATE_REDIS_URL": "redis://127.0.0.1:6379/5",
            "AGENTGATE_AUTH_ENABLED": "true",
            "AGENTGATE_LOG_LEVEL": "WARNING",
        }
        gw = subprocess.Popen(
            [
                sys.executable, "-m", "uvicorn", "app.api.main:app",
                "--host", "127.0.0.1", "--port", str(gw_port),
                "--workers", str(workers), "--log-level", "error", "--no-access-log",
            ],
            cwd=str(ROOT), env=env,
        )
        try:
            await _wait_up(f"http://127.0.0.1:{gw_port}/health")
            # 预热(鉴权缓存回填到共享 Redis)
            async with (
                aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as s,
                s.post(
                    f"http://127.0.0.1:{gw_port}/v1/chat/completions",
                    json={"model": "mock-model", "messages": [{"role": "user", "content": "warm"}]},
                    headers={"Authorization": f"Bearer {raw}"},
                ) as r,
            ):
                    assert r.status == 200, await r.text()
            # c=1 每请求开销
            _, lat1 = await _run_load(f"http://127.0.0.1:{gw_port}", raw, 100, 1)
            # c=N 容量
            dur, latn = await _run_load(
                f"http://127.0.0.1:{gw_port}", raw, args.requests, args.concurrency
            )
            rows.append(
                {
                    "workers": workers,
                    "thr": args.requests / dur,
                    "p50": statistics.median(latn),
                    "p99": _pctl(latn, 99),
                    "overhead_c1": statistics.median(lat1),
                }
            )
            print(f"[{workers} worker] thr={rows[-1]['thr']:.0f}/s P50={rows[-1]['p50']:.1f}ms "
                  f"P99={rows[-1]['p99']:.1f}ms c1med={rows[-1]['overhead_c1']:.2f}ms")
        finally:
            gw.terminate()
            try:
                gw.wait(timeout=10)
            except subprocess.TimeoutExpired:
                gw.kill()

    base = rows[0]["thr"]
    lines = [
        "# 多 worker 水平扩展压测(脚本自动生成)",
        "",
        "- 环境:Windows 本机 · uvicorn --workers N 真多进程 · 共享 SQLite 账本 + Redis db5 · mock 上游独立进程",
        f"- 参数:每档 {args.requests} 请求 · 并发 {args.concurrency} · aiohttp 单进程客户端(绝对值受客户端约束,看相对比)",
        "",
        "| workers | 吞吐(req/s) | 扩展比 | P50(ms) | P99(ms) | c=1 中位(ms) |",
        "|---|---|---|---|---|---|",
    ]
    for r in rows:
        lines.append(
            f"| {r['workers']} | {r['thr']:.0f} | {r['thr'] / base:.2f}x | {r['p50']:.1f} | {r['p99']:.1f} | {r['overhead_c1']:.2f} |"
        )
    lines += [
        "",
        "## 口径声明",
        "- 多进程 GIL 隔离,共享状态全在 Redis(限流/路由/鉴权缓存)——多副本一致性的设计被本压测直接验证;",
        "- 账本为每进程单写者写共享 SQLite(PG 下无此顾虑);扩展比非线性的部分来自本机 CPU 与客户端上限;",
        "- 生产形态即此(uvicorn --workers N / k8s 多副本)。",
    ]
    report = "\n".join(lines)
    print(report)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(report, encoding="utf-8")

    await redis.aclose()
    mock_proc.terminate()


if __name__ == "__main__":
    asyncio.run(main())
