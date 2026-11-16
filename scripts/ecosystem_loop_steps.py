"""生态闭环演示 · 场景编排与采证(由 ecosystem_loop_demo.sh 在四进程就绪后调用)。

为什么场景层用 python 而不是 bash+curl:Git Bash 向原生 exe 传参走活动代码页,
中文 JSON 体在 curl -d 参数里会被改码(FastAPI 报 "error parsing the body");
httpx 全程 Unicode,采证文件恒 UTF-8。

场景:S0 seed(租户 askbi + 专用 vk)→ S1 askbi 两问(JSON 链路)→ S2 直连网关 SSE
→ S3 白名单 403 → S4 租户预算 402(闸 + askbi 诚实降级)→ S5 采证(usage/metrics/账行)。
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import pathlib
import sqlite3
import time

import httpx
import redis.asyncio as ar

MODEL = "demo-chat"
ADMIN_TOKEN = "eco-demo-admin-token"


class Steps:
    def __init__(self, gw: str, ab: str, redis_url: str, gw_db: str, raw_path: str,
                 tenant_id: str, vk_raw_file: str, key_prefix: str):
        self.gw, self.ab, self.redis_url, self.gw_db = gw, ab, redis_url, gw_db
        # 采证文件是全程追加的日志流,跨整个生命周期持有而非单语句打开 —— noqa SIM115
        self.raw = open(raw_path, "a", encoding="utf-8")  # noqa: SIM115
        self.tenant_id = tenant_id
        self.raw_key = pathlib.Path(vk_raw_file).read_text(encoding="utf-8").strip()
        self.key_prefix = key_prefix
        self.failures: list[str] = []

    def step(self, title: str) -> None:
        self.raw.write(f"\n\n===== {title} =====\n")

    def note(self, line: str = "") -> None:
        self.raw.write(line + "\n")
        print(line, flush=True)

    def _final_events(self, sse_body: str) -> list[dict]:
        out = []
        for line in sse_body.splitlines():
            if line.startswith("data: ") and '"type": "final"' in line:
                out.append(json.loads(line[len("data: "):]))
        return out

    def usage(self, label: str) -> dict:
        with httpx.Client(timeout=30) as c:
            r = c.get(f"{self.gw}/admin/tenants/{self.tenant_id}/usage",
                      headers={"X-Admin-Token": ADMIN_TOKEN})
        data = r.json()
        self.note("")
        self.note(f"### gateway /admin/tenants/…/usage · {label}")
        self.note(json.dumps(data, ensure_ascii=False))
        return data

    # ---- S1 ----
    def askbi_ask(self, question: str, label: str) -> None:
        t0 = time.perf_counter()
        with httpx.Client(timeout=180) as c, c.stream(
            "POST", f"{self.ab}/api/ask", json={"question": question}
        ) as resp:
            body = "".join(chunk.decode("utf-8") for chunk in resp.iter_raw())
        wall_ms = int((time.perf_counter() - t0) * 1000)
        finals = self._final_events(body)
        self.note("")
        self.note(f"### {label}(q={question},墙钟 {wall_ms}ms,SSE 帧数={body.count('data: ')})")
        if not finals:
            self.note("!! 无 final 事件 —— 原始输出前 2000 字:")
            self.note(body[:2000])
            self.failures.append(label)
            return
        self.note("data: " + json.dumps(finals[-1], ensure_ascii=False))

    # ---- S2 ----
    def sse_direct(self) -> None:
        self.step("S2 场景二:直连网关 SSE(stream=true,流式形态补证——askbi 客户端为非流式,见口径声明)")
        payload = {"model": MODEL, "stream": True, "stream_options": {"include_usage": True},
                   "messages": [{"role": "user", "content": "用一句话介绍你自己"}]}
        with httpx.Client(timeout=60) as c, c.stream(
            "POST", f"{self.gw}/v1/chat/completions", json=payload,
            headers={"Authorization": f"Bearer {self.raw_key}"},
        ) as resp:
            frames = list(resp.iter_lines())
        self.note("")
        self.note(f"### S2 SSE 帧(共 {len(frames)} 行,截前 8 + 末 3)")
        for line in frames[:8]:
            self.note(line)
        self.note("……")
        for line in frames[-3:]:
            self.note(line)

    # ---- S3 ----
    def whitelist_403(self) -> None:
        self.step("S3 场景三:模型白名单闸(model 不在 vk 白名单 → 403 model_not_allowed)")
        with httpx.Client(timeout=30) as c:
            r = c.post(f"{self.gw}/v1/chat/completions",
                       json={"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]},
                       headers={"Authorization": f"Bearer {self.raw_key}"})
        self.note("")
        self.note(f"### S3 白名单 403 信封(status={r.status_code})")
        self.note(r.text)

    # ---- S4 ----
    def budget_402(self) -> None:
        self.step("S4 场景四:租户预算兜底闸(把租户预算设为=已花费 → 下一次 LLM 调用 402)")
        spent = self.usage("S4 设闸前")["total_tokens"]
        with httpx.Client(timeout=30) as c:
            r = c.post(f"{self.gw}/admin/tenants/{self.tenant_id}/budget",
                       json={"budget_tokens": spent}, headers={"X-Admin-Token": ADMIN_TOKEN})
        self.note(f"已花费 total_tokens={spent} → 租户预算设为 {spent}(判定为 >=,下一次调用必 402): {r.text}")
        # 预算读数在 vk 鉴权缓存里最长 60s 后生效(auth 缓存 TTL)——演示口径下 DEL
        # 该 key 的缓存,让新预算立即可见;租户预算计数器(budget:tenant:)不受影响。
        async def _del():
            r2 = ar.from_url(self.redis_url, decode_responses=True, protocol=2)
            n = await r2.delete("vk:" + hashlib.sha256(self.raw_key.encode()).hexdigest())
            await r2.aclose()
            return n

        self.note(f"auth 缓存 DEL(vk:sha256(raw)) → 删 {asyncio.run(_del())} 键,新预算立即生效")

        with httpx.Client(timeout=30) as c:
            r = c.post(f"{self.gw}/v1/chat/completions",
                       json={"model": MODEL, "messages": [{"role": "user", "content": "hi"}]},
                       headers={"Authorization": f"Bearer {self.raw_key}"})
        self.note("")
        self.note(f"### S4a 直连探测:租户预算信封(status={r.status_code})")
        self.note(r.text)
        if r.status_code != 402:
            self.failures.append("S4a 未 402")
        self.note("")
        self.note("### S4b askbi 行为:LLM 调用 402 → LLMError 不重试 → 节点诚实降级(澄清放行/SQL 拒答)")
        self.askbi_ask("上月华南销售额是多少", "S4b 402 后(租户预算已耗尽)")

    # ---- S5 ----
    def collect(self) -> None:
        self.step("S5 采证:usage 聚合 / metrics / 账本行样本")
        self.usage("终态")
        with httpx.Client(timeout=30) as c:
            metrics = c.get(f"{self.gw}/metrics").text
        self.note("")
        self.note("### gateway /metrics(agentgate_ 相关行)")
        n = 0
        for line in metrics.splitlines():
            if line.startswith("agentgate_") and n < 40:
                self.note(line)
                n += 1
        self.note("")
        self.note(f"(共 {sum(1 for l in metrics.splitlines() if l.startswith('agentgate_'))} 行 agentgate_ 指标,截 40)")
        self.note("")
        self.note("### 账本行样本(usage_records,SQLite 直接读)")
        con = sqlite3.connect(self.gw_db)
        cols = ("request_id,tenant_id,model,prompt_tokens,completion_tokens,"
                "total_tokens,ttft_ms,latency_ms,status,channel,cost")
        self.note(" | ".join(c.upper() for c in cols.split(",")))
        for row in con.execute(f"SELECT {cols} FROM usage_records ORDER BY id"):
            self.note(" | ".join("" if v is None else str(v) for v in row))
        count = con.execute("SELECT COUNT(*) FROM usage_records").fetchone()[0]
        self.note(f"-- 共 {count} 行")
        con.close()

    def done(self) -> list[str]:
        self.raw.close()
        return self.failures


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gw", default="http://127.0.0.1:8100")
    parser.add_argument("--ab", default="http://127.0.0.1:8200")
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6399/0")
    parser.add_argument("--gw-db", required=True)
    parser.add_argument("--raw-out", required=True)
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument("--vk-raw-file", required=True)
    parser.add_argument("--key-prefix", default="")
    args = parser.parse_args()

    s = Steps(args.gw, args.ab, args.redis_url, args.gw_db, args.raw_out,
              args.tenant_id, args.vk_raw_file, args.key_prefix)
    s.step("S1 场景一:askbi 两问(JSON 形态链路;轻量链路每问 2 次 LLM 调用:澄清→SQL)")
    s.askbi_ask("上月各渠道销售额是多少", "S1-Q1")
    s.usage("S1-Q1 后")
    s.askbi_ask("最近三个月销售额趋势如何", "S1-Q2")
    s.usage("S1-Q2 后")
    s.sse_direct()
    s.whitelist_403()
    s.budget_402()
    s.collect()
    fails = s.done()
    if fails:
        print(f"FAILURES: {fails}")
        return 1
    print("ALL SCENARIOS OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
