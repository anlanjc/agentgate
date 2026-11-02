"""生态实接之二:AskBI(多 Agent ChatBI)接入 agentgate —— SSE/结构化答案的 shim 映射。

与 PC 实接(demo_pc_gateway.py)互补:AskBI 是 SSE 流式 + 结构化答案(kind: answer/refusal),
shim 需消费 SSE 等 final 事件并渲染 —— 展示"渠道层协议适配"的另一种形态。

运行:.venv/Scripts/python scripts/demo_askbi_gateway.py
证据:evidence/askbi_gateway.md
"""

import asyncio
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
AB = ROOT.parent / "askbi"
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

import httpx


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


async def _wait_up(url: str, timeout: float = 90.0) -> None:
    deadline = time.time() + timeout
    async with httpx.AsyncClient(timeout=3.0) as c:
        while time.time() < deadline:
            try:
                r = await c.get(url)
                if r.status_code == 200:
                    return
            except Exception:
                pass
            await asyncio.sleep(1.0)
    raise RuntimeError(f"not up in {timeout}s: {url}")


async def main() -> None:
    ag_py = str(ROOT / ".venv" / "Scripts" / "python.exe")
    ab_py = str(AB / ".venv" / "Scripts" / "python.exe")

    port_ab = _free_port()
    port_shim = _free_port()
    port_gw = _free_port()
    procs: list[subprocess.Popen] = []

    try:
        # 1) AskBI(自带 venv;llm_provider 默认 mock,clarify 默认关)
        procs.append(
            subprocess.Popen(
                [ab_py, "-m", "uvicorn", "app.api.main:app", "--host", "127.0.0.1",
                 "--port", str(port_ab), "--log-level", "warning"],
                cwd=str(AB),
            )
        )
        print(f"[1] AskBI 启动中 :{port_ab}(mock provider)…")
        await _wait_up(f"http://127.0.0.1:{port_ab}/api/health")
        print("    AskBI 就绪")

        # 2) AskBI shim(SSE 消费 + 结构化答案渲染)
        procs.append(
            subprocess.Popen(
                [ag_py, "-u", "-c",
                 (
                     "import sys, uvicorn\n"
                     f"sys.path.insert(0, r'{ROOT}')\n"
                     "from scripts.askbi_shim import build_askbi_shim\n"
                     f"uvicorn.run(build_askbi_shim('http://127.0.0.1:{port_ab}/api/ask', 'askbi'),"
                     f" host='127.0.0.1', port={port_shim}, log_level='error')\n"
                 )],
                cwd=str(ROOT),
            )
        )
        await _wait_up(f"http://127.0.0.1:{port_shim}/v1/models")
        print(f"[2] AskBI shim 就绪 :{port_shim}")

        # 3) 网关(默认渠道 = askbi shim)
        import redis.asyncio as aredis
        import uvicorn

        from app.api.main import create_app
        from app.auth.keys import generate_key
        from app.config import Settings
        from app.db.models import Tenant, VirtualKey
        from app.db.session import build_session_factory, init_db
        from app.upstream.client import UpstreamClient

        if os.path.exists("ab_gw.db"):
            os.remove("ab_gw.db")
        settings = Settings(
            upstream_name="askbi",
            upstream_base_url=f"http://127.0.0.1:{port_shim}/v1",
            upstream_api_key="",
            models=["askbi"],
            db_url="sqlite+aiosqlite:///./ab_gw.db",
            redis_url="redis://127.0.0.1:6379/8",
            auth_enabled=True,
            log_level="WARNING",
        )
        redis = aredis.from_url(settings.redis_url, decode_responses=True, protocol=2)
        await redis.flushdb()
        upstream = UpstreamClient(settings.upstream_base_url, "", trust_env=False)
        app = create_app(settings=settings, upstream=upstream, redis_client=redis)
        await init_db(app.state.engine)
        raw, key_hash, prefix = generate_key()
        async with build_session_factory(app.state.engine)() as db:
            db.add(Tenant(id="t-ab", name="chatbi"))
            db.add(
                VirtualKey(
                    id="k-ab", tenant_id="t-ab", key_hash=key_hash, key_prefix=prefix,
                    rpm_limit=10_000, tpm_limit=10_000_000,
                )
            )
            await db.commit()
        config = uvicorn.Config(app, host="127.0.0.1", port=port_gw, log_level="error", access_log=False)
        server = uvicorn.Server(config)
        server_task = asyncio.get_running_loop().create_task(server.serve())
        while not server.started:
            await asyncio.sleep(0.05)
        print(f"[3] agentgate 就绪 :{port_gw}(渠道 askbi)")

        # 4) OpenAI 请求穿全链路(ChatBI 场景问题)
        questions = ["总销售额是多少", "各渠道销售占比"]
        lines = [
            "# 生态实接:agentgate 管理 AskBI(真实多进程)",
            "",
            f"- 时间:{time.strftime('%Y-%m-%d %H:%M:%S')}",
            "- 链路:OpenAI 请求 → agentgate(Lua 准入/记账)→ AskBI shim(SSE 消费+结构化渲染)→ AskBI /api/ask(mock NL2SQL 多 Agent)",
            "",
        ]
        for q in questions:
            t0 = time.perf_counter()
            async with httpx.AsyncClient(
                base_url=f"http://127.0.0.1:{port_gw}", timeout=180.0
            ) as c:
                r = await c.post(
                    "/v1/chat/completions",
                    json={"model": "askbi", "messages": [{"role": "user", "content": q}]},
                    headers={"Authorization": f"Bearer {raw}"},
                )
            dt = (time.perf_counter() - t0) * 1000
            assert r.status_code == 200, f"{r.status_code} {r.text[:300]}"
            data = r.json()
            answer = data["choices"][0]["message"]["content"]
            channel = r.headers["x-upstream-channel"]
            usage = data.get("usage") or {}
            lines += [
                f"## Q: {q}",
                f"- 200 · 渠道 {channel} · 延迟 {dt:.0f}ms · usage={usage}",
                f"- A(截断 200 字):{answer[:200]}",
            ]
            print(f"[4] Q={q} → 200 via {channel}({dt:.0f}ms)答案 {len(answer)} 字")

        await app.state.task_queue.flush()
        from sqlalchemy import func, select

        from app.db.models import UsageRecord

        async with build_session_factory(app.state.engine)() as db:
            n, tokens = (
                await db.execute(
                    select(func.count(UsageRecord.id), func.coalesce(func.sum(UsageRecord.total_tokens), 0))
                )
            ).one()
        lines.append(f"## 账本对账:{n} 行 · 总 token {tokens}")
        lines.append("")
        lines.append("**判定:三项目互联成立** —— agentgate 的渠道注册表里,PolicyCompass 与 AskBI")
        lines.append("都是普通渠道:统一 key/限流/计费/监控,应用零改动。")

        report = "\n".join(lines)
        print(report)
        Path("evidence").mkdir(exist_ok=True)
        Path("evidence/askbi_gateway.md").write_text(report, encoding="utf-8")
        print("[saved] evidence/askbi_gateway.md")

        await server_task
    finally:
        for p in procs:
            try:
                p.terminate()
                p.wait(timeout=8)
            except Exception:
                try:
                    p.kill()
                except Exception:
                    pass
        if os.path.exists("ab_gw.db"):
            try:
                os.remove("ab_gw.db")
            except Exception:
                pass


if __name__ == "__main__":
    asyncio.run(main())
