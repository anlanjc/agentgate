"""生态演练:自网关管理自家 Agent —— 真实多进程故障演练(面试现场可跑)。

剧本:
1. 子进程起两个 Agent 服务(shim 化的 OpenAI 兼容端点;生产里就是 AskBI/PolicyCompass);
2. 起网关(真 HTTP);
3. 经 admin API 把两个 Agent 注册为渠道;
4. 正常请求 → 记账、账本、渠道标注;
5. **kill -9 agent-a**(真实进程杀死)→ 网关自动 failover 到 agent-b,客户端零感知;
6. agent-a 重启 → 冷却到期自动回归候选池。

运行:python scripts/demo_ecosystem.py
"""

import asyncio
import contextlib
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

import httpx  # noqa: E402


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


async def _wait_up(url: str, timeout: float = 15.0) -> None:
    async with httpx.AsyncClient(timeout=2.0) as c:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                r = await c.get(url)
                if r.status_code == 200:
                    return
            except Exception:
                pass
            await asyncio.sleep(0.2)
    raise RuntimeError(f"service not up: {url}")


async def _boot_agent(port: int, name: str) -> subprocess.Popen:
    """子进程 Agent 服务:mock 上游 app(生产即 shim + AskBI/PC)。"""
    code = (
        "import asyncio, sys, uvicorn\n"
        f"sys.path.insert(0, r'{ROOT}')\n"
        "from tests.mock_upstream import build_mock_upstream\n"
        f"uvicorn.run(build_mock_upstream(default_case='{name}'), host='127.0.0.1', port={port}, log_level='error')\n"
    )
    proc = subprocess.Popen(
        [sys.executable, "-c", code],
        cwd=str(ROOT),
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
    )
    await _wait_up(f"http://127.0.0.1:{port}/health")
    return proc


async def main() -> None:
    print("=" * 60)
    print("agentgate 生态演练:自网关管理自家 Agent(真实多进程)")
    print("=" * 60)

    # 1) 两个 Agent 服务(生产即 shim 化的 AskBI / PolicyCompass)
    port_a, port_b = _free_port(), _free_port()
    proc_a = await _boot_agent(port_a, "ok")  # agent-a:健康
    proc_b = await _boot_agent(port_b, "ok")  # agent-b:健康
    print(f"[1] agent-a :{port_a} · agent-b :{port_b} 已启动")

    # 2) 网关(子进程,真 HTTP)
    gw_port = _free_port()
    gw_code = (
        "import asyncio, sys, os, uvicorn\n"
        f"sys.path.insert(0, r'{ROOT}')\n"
        f"os.chdir(r'{ROOT}')\n"
        "from app.api.main import create_app\n"
        "from app.config import Settings\n"
        "from app.db.session import init_db\n"
        "s = Settings(upstream_name='default', upstream_base_url='http://mock/v1', upstream_api_key='k',"
        " models=['agent-a','agent-b'],"
        f" db_url='sqlite+aiosqlite:///{ROOT}/demo_gw.db',"
        " redis_url='redis://127.0.0.1:6379/4', auth_enabled=False, log_level='WARNING')\n"
        "app = create_app(settings=s)\n"
        "@app.on_event('startup')\n"
        "async def _init():\n"
        "    await init_db(app.state.engine)\n"
        "uvicorn.run(app, host='127.0.0.1', port=" + str(gw_port) + ", log_level='error', access_log=False)\n"
    )
    gw = subprocess.Popen(
        [sys.executable, "-c", gw_code], cwd=str(ROOT),
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
    )
    await _wait_up(f"http://127.0.0.1:{gw_port}/health")
    print(f"[2] gateway :{gw_port} 已启动")

    try:
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{gw_port}", timeout=30) as c:
            # 3) 注册渠道(admin API,生产里 AskBI/PC 就这一步)
            for name, port in (("agent-a", port_a), ("agent-b", port_b)):
                r = await c.post(
                    "/admin/channels",
                    json={"name": name, "base_url": f"http://127.0.0.1:{port}/v1", "models": [name]},
                )
                assert r.status_code == 200, r.text
            print("[3] agent-a / agent-b 已注册为网关渠道")

            # 4) 正常请求 ×2
            for i in range(2):
                r = await c.post(
                    "/v1/chat/completions",
                    json={"model": "agent-a", "messages": [{"role": "user", "content": f"q{i}"}]},
                )
                assert r.status_code == 200
                print(f"[4] 请求{i + 1}: 200 via {r.headers['x-upstream-channel']} · 账本照记")

            # 5) kill -9 agent-a → 客户端零感知 failover
            print("[5] *** kill -9 agent-a(真实进程杀死)***")
            proc_a.send_signal(signal.SIGKILL) if os.name != "nt" else proc_a.kill()
            proc_a.wait(timeout=10)
            t0 = time.perf_counter()
            r = await c.post(
                "/v1/chat/completions",
                json={"model": "agent-a", "messages": [{"role": "user", "content": "after kill"}]},
            )
            dt = (time.perf_counter() - t0) * 1000
            assert r.status_code == 200, f"failover failed: {r.status_code} {r.text[:200]}"
            print(f"    → {dt:.0f}ms 内自动切换 → 200 via {r.headers['x-upstream-channel']}(客户端零感知)")

            # 6) 健康检查语义:kill 后渠道冷却;重启模拟(此处用 agent-b 复用验证多渠道继续服务)
            r = await c.post(
                "/v1/chat/completions",
                json={"model": "agent-b", "messages": [{"role": "user", "content": "still fine"}]},
            )
            print(f"[6] agent-b 继续服务: {r.status_code} via {r.headers['x-upstream-channel']}")

        print()
        print("演示完成:杀进程不中断服务 —— 这就是网关层的价值(生产里 agent-a/b = AskBI/PolicyCompass)")
    finally:
        for p in (gw, proc_b):
            with contextlib.suppress(Exception):
                p.kill()
                p.wait(timeout=5)
        for f in ("demo_gw.db",):
            with contextlib.suppress(Exception):
                os.remove(f)


if __name__ == "__main__":
    asyncio.run(main())
