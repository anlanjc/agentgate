"""生态实接演示:自家网关管理自家 Agent —— 真实 PolicyCompass 接入 agentgate。

链路:OpenAI 格式请求 → agentgate(鉴权/Lua 准入/记账)→ shim(协议映射)→ PolicyCompass(/chat,CRAG 管线,mock LLM)
         ← answers(社保公积金真实语料的抽取式答案,含引用)

运行:.venv/Scripts/python scripts/demo_pc_gateway.py
证据:evidence/pc_gateway.md
"""

import asyncio
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PC = ROOT.parent / "policy-compass"
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

import httpx


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


async def _wait_up(url: str, timeout: float = 120.0) -> None:
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
    import shutil

    # PC 子进程必须用系统 python(含 langgraph/jieba/fitz 等 PC 依赖;agent venv 没有这些)
    py = shutil.which("python") or r"D:\Python\python.exe"
    ag_py = str(ROOT / ".venv" / "Scripts" / "python.exe")

    port_pc = _free_port()
    port_shim = _free_port()
    port_gw = _free_port()

    procs: list[subprocess.Popen] = []
    try:
        # 1) PolicyCompass(LLM_MODE=mock,不产生 API 花费)
        procs.append(
            subprocess.Popen(
                [py, "-m", "uvicorn", "app.api.main:app", "--host", "127.0.0.1",
                 "--port", str(port_pc), "--log-level", "warning"],
                cwd=str(PC),
                env={**os.environ, "LLM_MODE": "mock", "LLM_TRUST_ENV": "false",
                     "APP_PORT": str(port_pc)},
            )
        )
        print(f"[1] PolicyCompass 启动中 :{port_pc}(mock LLM,首次需摄取语料)…")
        await _wait_up(f"http://127.0.0.1:{port_pc}/api/health")  # PC 路由挂 /api 前缀
        print("    PC 就绪")

        # 2) shim:OpenAI 协议 ↔ PC /api/chat 映射
        procs.append(
            subprocess.Popen(
                [ag_py, "-u", "-c",
                 (
                     "import sys, uvicorn\n"
                     f"sys.path.insert(0, r'{ROOT}')\n"
                     "from scripts.agent_shim_example import build_agent_shim\n"
                     f"uvicorn.run(build_agent_shim('http://127.0.0.1:{port_pc}/api/chat', 'policy-compass'),"
                     f" host='127.0.0.1', port={port_shim}, log_level='error')\n"
                 )],
                cwd=str(ROOT),
            )
        )
        await _wait_up(f"http://127.0.0.1:{port_shim}/v1/models")
        print(f"[2] shim(OpenAI 兼容适配)就绪 :{port_shim}")

        # 3) 网关(进程内)+ 种子 —— 默认渠道即 PC shim,无假渠道
        import redis.asyncio as aredis
        import uvicorn

        from app.api.main import create_app
        from app.auth.keys import generate_key
        from app.config import Settings
        from app.db.models import Tenant, VirtualKey
        from app.db.session import build_session_factory, init_db
        from app.upstream.client import UpstreamClient

        if os.path.exists("pc_gw.db"):
            os.remove("pc_gw.db")
        settings = Settings(
            upstream_name="policy-compass",
            upstream_base_url=f"http://127.0.0.1:{port_shim}/v1",
            upstream_api_key="",
            models=["policy-compass"],
            db_url="sqlite+aiosqlite:///./pc_gw.db",
            redis_url="redis://127.0.0.1:6379/7",
            auth_enabled=True,
            log_level="WARNING",
        )
        redis = aredis.from_url(settings.redis_url, decode_responses=True, protocol=2)
        await redis.flushdb()
        pc_upstream = UpstreamClient(settings.upstream_base_url, "", trust_env=False)
        app = create_app(settings=settings, upstream=pc_upstream, redis_client=redis)
        await init_db(app.state.engine)
        raw, key_hash, prefix = generate_key()
        async with build_session_factory(app.state.engine)() as db:
            db.add(Tenant(id="t-eco", name="ecosystem"))
            db.add(
                VirtualKey(
                    id="k-eco", tenant_id="t-eco", key_hash=key_hash, key_prefix=prefix,
                    rpm_limit=10_000, tpm_limit=10_000_000,
                )
            )
            await db.commit()

        # 渠道已在 create_app 注册为 policy-compass(默认渠道 = PC shim,无假渠道)

        config = uvicorn.Config(app, host="127.0.0.1", port=port_gw, log_level="error", access_log=False)
        server = uvicorn.Server(config)
        server_task = asyncio.get_running_loop().create_task(server.serve())
        while not server.started:
            await asyncio.sleep(0.05)
        print(f"[3] agentgate 就绪 :{port_gw}(渠道 policy-compass 已注册)")

        # 4) OpenAI 格式请求穿过全链路
        questions = [
            "上海公积金贷款额度是多少?",
            "公积金断缴有什么影响?",
        ]
        lines = [
            "# 生态实接:agentgate 管理 PolicyCompass(真实多进程)",
            "",
            f"- 时间:{time.strftime('%Y-%m-%d %H:%M:%S')}",
            "- 链路:OpenAI 格式请求 → agentgate(鉴权/Lua 准入/记账)→ shim(协议映射)→ PolicyCompass /chat(CRAG,mock LLM)",
            "",
        ]
        for q in questions:
            t0 = time.perf_counter()
            async with httpx.AsyncClient(
                base_url=f"http://127.0.0.1:{port_gw}", timeout=120.0
            ) as c:
                r = await c.post(
                    "/v1/chat/completions",
                    json={"model": "policy-compass",
                          "messages": [{"role": "user", "content": q}]},
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
                f"- 状态 200 · 渠道 {channel} · 延迟 {dt:.0f}ms · usage={usage}",
                f"- A(截断):{answer[:120]}…",
            ]
            print(f"[4] Q={q[:20]}… → 200 via {channel}({dt:.0f}ms)答案 {len(answer)} 字")

        # 5) 账本对账
        await app.state.task_queue.flush()
        from sqlalchemy import func, select

        from app.db.models import UsageRecord

        async with build_session_factory(app.state.engine)() as db:
            n, tokens = (
                await db.execute(
                    select(func.count(UsageRecord.id), func.coalesce(func.sum(UsageRecord.total_tokens), 0))
                )
            ).one()
        lines.append(f"## 账本对账:{n} 行 · 总 token {tokens}(请求路径零 DB 写,后台异步落库)")
        lines.append("")
        lines.append("**判定:自家网关管理自家 Agent 成立** —— 租户/key/限流/计费/监控统一收口,")
        lines.append("Agent 应用(AskBI/PolicyCompass)经 ~60 行 shim 即可接入,应用零改动。")

        report = "\n".join(lines)
        print(report)
        Path("evidence").mkdir(exist_ok=True)
        Path("evidence/pc_gateway.md").write_text(report, encoding="utf-8")
        print("[saved] evidence/pc_gateway.md")

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
        if os.path.exists("pc_gw.db"):
            try:
                os.remove("pc_gw.db")
            except Exception:
                pass


if __name__ == "__main__":
    asyncio.run(main())
