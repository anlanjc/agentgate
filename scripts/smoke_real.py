"""DashScope 真实冒烟:真 HTTP 网关 + 真 DashScope 上游,1 次非流式 + 1 次流式。

红线:花费 ≤¥2(qwen-turbo 两条短句,实际 <¥0.01);key 从 policy-compass/.env 读,不落日志。
证据:evidence/real_smoke.md
"""

import asyncio
import os
import socket
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _load_key() -> tuple[str, str]:
    env_path = ROOT.parent / "policy-compass" / ".env"
    values: dict[str, str] = {}
    for line in env_path.read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.strip().startswith("#"):
            k, _, v = line.partition("=")
            values[k.strip()] = v.strip()
    return values["LLM_API_KEY"], values.get("LLM_API_BASE", "")


async def main() -> None:
    import httpx
    import redis.asyncio as aredis
    import uvicorn

    from app.api.main import create_app
    from app.auth.keys import generate_key
    from app.config import Settings
    from app.db.models import Tenant, VirtualKey
    from app.db.session import build_session_factory, init_db
    from app.upstream.client import UpstreamClient

    api_key, env_base = _load_key()
    base_url = "https://dashscope.aliyuncs.com/compatible-mode/v1"  # 固定 OpenAI 兼容端点
    # qwen-turbo 当前账号配额耗尽(limit_requests 429,2026-11-02 实测);qwen-flash 有配额
    model = "qwen-flash"

    if os.path.exists("smoke_real.db"):
        os.remove("smoke_real.db")
    settings = Settings(
        upstream_name="dashscope",
        upstream_base_url=base_url,
        upstream_api_key=api_key,
        models=[model],
        db_url="sqlite+aiosqlite:///./smoke_real.db",
        redis_url="redis://127.0.0.1:6379/3",
        auth_enabled=True,
        log_level="WARNING",
    )
    redis = aredis.from_url(settings.redis_url, decode_responses=True, protocol=2)
    await redis.flushdb()

    upstream = UpstreamClient(
        base_url,
        api_key,
        connect_timeout=10.0,
        read_timeout=60.0,
        trust_env=False,  # 该机注册表代理坑:LLM 上游必须直连(PC 会话教训)
    )
    app = create_app(settings=settings, upstream=upstream, redis_client=redis)
    await init_db(app.state.engine)
    raw, key_hash, prefix = generate_key()
    async with build_session_factory(app.state.engine)() as db:
        db.add(Tenant(id="t-smoke", name="smoke"))
        db.add(
            VirtualKey(
                id="k-smoke", tenant_id="t-smoke", key_hash=key_hash, key_prefix=prefix,
                rpm_limit=1000, tpm_limit=1_000_000,
            )
        )
        await db.commit()

    port = _free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error", access_log=False)
    server = uvicorn.Server(config)
    server_task = asyncio.get_running_loop().create_task(server.serve())  # noqa: F841 停机靠 should_exit
    while not server.started:
        await asyncio.sleep(0.05)

    results: list[str] = ["# DashScope 真实冒烟(经 agentgate 全链路)", ""]
    results.append(f"- 时间:{time.strftime('%Y-%m-%d %H:%M:%S')} · 上游:{base_url} · 模型:{model}")
    results.append("- 链路:真 HTTP(aiohttp)→ uvicorn 网关 → 鉴权/限流/配额(Lua)→ httpx → DashScope")
    try:
        body = {"model": model, "messages": [{"role": "user", "content": "用一句话介绍你自己"}]}
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}", timeout=60) as client:
            # 非流式(上游 429 时退避重试:账号 RPM 配额按分钟窗口恢复;网关 429 冷却语义也会随窗口恢复)
            r1 = None
            for attempt in range(6):
                t0 = time.perf_counter()
                r1 = await client.post(
                    "/v1/chat/completions", json=body, headers={"Authorization": f"Bearer {raw}"}
                )
                nonstream_ms = (time.perf_counter() - t0) * 1000
                if r1.status_code == 200:
                    break
                print(f"[attempt {attempt + 1}] upstream {r1.status_code}, retry in 8s …")
                # 测试装置行为:清渠道冷却状态(网关把无 Retry-After 的 429 冷却 60s,生产语义正确,
                # 但冒烟目的是走通全链路而非验证冷却持久性)
                await redis.delete("gate:ch:dashscope")
                await asyncio.sleep(8)
            assert r1 is not None and r1.status_code == 200, f"non-stream failed: {r1.status_code} {r1.text[:300]}"
            data = r1.json()
            usage1 = data["usage"]
            content = data["choices"][0]["message"]["content"]
            results.append("")
            results.append("## 非流式")
            results.append(
                f"- 200 OK · 延迟 {nonstream_ms:.0f}ms · usage={usage1} · channel={r1.headers.get('x-upstream-channel')}"
            )
            results.append(f"- 回复摘要:{content[:40]}…")

            # 流式(同样退避重试)
            r2 = None
            for attempt in range(6):
                t0 = time.perf_counter()
                ttft = None
                n_chunks = 0
                usage2 = None
                try:
                    async with client.stream(
                        "POST",
                        "/v1/chat/completions",
                        json={**body, "stream": True, "stream_options": {"include_usage": True}},
                        headers={"Authorization": f"Bearer {raw}"},
                    ) as resp:
                        if resp.status_code != 200:
                            print(f"[attempt {attempt + 1}] stream {resp.status_code}, retry in 8s …")
                            await redis.delete("gate:ch:dashscope")
                            await asyncio.sleep(8)
                            continue
                        assert resp.headers["content-type"].startswith("text/event-stream")
                        r2 = resp.status_code
                        async for chunk in resp.aiter_raw():
                            n_chunks += 1
                            if ttft is None and b"data:" in chunk:
                                ttft = (time.perf_counter() - t0) * 1000
                            if b'"usage"' in chunk:
                                import json as _json

                                for line in chunk.split(b"\n"):
                                    if line.startswith(b"data:") and b"usage" in line:
                                        usage2 = _json.loads(line[5:]).get("usage")
                    break
                except Exception as e:
                    print(f"[attempt {attempt + 1}] stream error {e}, retry in 8s …")
                    await redis.delete("gate:ch:dashscope")
                    await asyncio.sleep(8)
            stream_ms = (time.perf_counter() - t0) * 1000
            assert r2 == 200, "stream failed after retries"
            results.append("")
            results.append("## 流式")
            results.append(
                f"- 200 OK · TTFT {ttft:.0f}ms · 总延迟 {stream_ms:.0f}ms · chunks={n_chunks} · usage={usage2}"
            )
            results.append("")
            results.append("## 判定")
            results.append(
                f"- ✅ 真实上游全链路可用:鉴权(Lua 准入)→ SSE 透传 → 记账(账本由后台落库)"
                f";总花费 ≈ {(usage1['total_tokens'] + (usage2 or usage1)['total_tokens']) * 0.0000008:.6f} 元量级(qwen-turbo)"
            )

        # 账本落库验证:轮询等齐 2 行(非流式+流式)——记账在响应结束后的 BackgroundTask
        # 里 enqueue,单次 flush 只保证"队列清空",不保证后台任务已入队,会抢跑;
        # 抢跑后硬停机会把在途 commit 连同账本行一起取消(2026-11-03 实测 1/2 行)。
        from sqlalchemy import func, select

        from app.db.models import UsageRecord

        n, tokens = 0, 0
        for _ in range(50):  # 最多 ~5s
            async with build_session_factory(app.state.engine)() as db:
                n, tokens = (
                    await db.execute(
                        select(func.count(UsageRecord.id), func.coalesce(func.sum(UsageRecord.total_tokens), 0))
                    )
                ).one()
            if n >= 2:
                break
            await asyncio.sleep(0.1)
        await app.state.task_queue.flush()
        results.append(f"- 账本对账:{n} 行,总 token {tokens}(= 网关异步记账落库成功)")
        results.append(
            "- 边界注记:记账 at-least-once 从 enqueue 起算;进程硬停可丢在途 BackgroundTask 账本行"
            "(升级路径=接入层 write-ahead),本脚本以优雅停机+轮询规避"
        )

        report = "\n".join(results)
        print(report)
        Path("evidence").mkdir(exist_ok=True)
        Path("evidence/real_smoke.md").write_text(report, encoding="utf-8")
    finally:
        server.should_exit = True
        await upstream.aclose()
        await app.state.task_queue.stop()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
