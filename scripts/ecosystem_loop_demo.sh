#!/usr/bin/env bash
# 生态闭环演示:askbi(多 Agent ChatBI 的 LLM 客户端)→ agentgate(网关/鉴权/计费)→ mock OpenAI 兼容上游
#
# 口径声明(与 evidence/ecosystem_loop_20261116.md 一致):
#   - mock only 零现金:LLM 上游是本仓 scripts/ecosystem_mock_upstream.py,无任何真实计费调用;
#   - 同机单进程演示口径,非生产部署(单 worker、SQLite、本机 Redis);
#   - 纯 config 演示:askbi 仓零代码改动,只经环境变量切换 provider(实读 .env.example/
#     app/llm/factory.py 确认的 LLM_PROVIDER/OPENAI_BASE_URL/OPENAI_API_KEY/OPENAI_MODEL)。
#
# 进程编排(本脚本):Redis → mock upstream → agentgate(alembic 建表)→ seed/场景/采证
#   (scripts/ecosystem_loop_steps.py,HTTP 层用 httpx 规避 Git Bash 中文参数改码)→ 清点清理。
# 产物: evidence/ecosystem_loop_raw_20261116.txt(原始采证;curated 版 ecosystem_loop_20261116.md)

set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ASKBI_ROOT="$ROOT/../askbi"
AG_PY="$ROOT/.venv/Scripts/python.exe"
AB_PY="$ASKBI_ROOT/.venv/Scripts/python.exe"
REDIS_BIN="${REDIS_SERVER_BIN:-$ROOT/../.tooling/agentgate-redis/redis-server.exe}"
[ -x "$REDIS_BIN" ] || REDIS_BIN="$(command -v redis-server || true)"
[ -n "${REDIS_BIN:-}" ] || { echo "FATAL: redis-server not found (set REDIS_SERVER_BIN)"; exit 1; }

GW_PORT="${GW_PORT:-8100}"; MOCK_PORT="${MOCK_PORT:-8101}"; AB_PORT="${AB_PORT:-8200}"; REDIS_PORT="${REDIS_PORT:-6399}"
GW="http://127.0.0.1:$GW_PORT"; MOCK="http://127.0.0.1:$MOCK_PORT"; AB="http://127.0.0.1:$AB_PORT"
MODEL="demo-chat"

RUN="$ROOT/.eco_demo_runtime"
mkdir -p "$RUN"
RAW="$RUN/raw_evidence.md"
EVID="$ROOT/evidence/ecosystem_loop_raw_20261116.txt"
GW_DB="$(cygpath -m "$RUN/eco_gw.db" 2>/dev/null || echo "$RUN/eco_gw.db")"

PIDS=()
note() { echo "$*" | tee -a "$RAW"; }
snap() { "$@" >>"$RAW" 2>&1; }

cleanup() {
  echo; echo "===== 清理:演示进程清点 =====" | tee -a "$RAW"
  for pid in "${PIDS[@]:-}"; do
    [ -n "$pid" ] && kill "$pid" 2>/dev/null && echo "killed pid=$pid" | tee -a "$RAW"
  done
  "$AG_PY" - "$REDIS_PORT" <<'PY' 2>/dev/null || true
import sys, redis
try:
    redis.Redis(host="127.0.0.1", port=int(sys.argv[1]), protocol=2, socket_connect_timeout=2).shutdown(nosave=True)
    print("redis shutdown: sent")
except Exception as e:
    print("redis shutdown skipped:", e)
PY
  sleep 2
  for port in "$GW_PORT" "$MOCK_PORT" "$AB_PORT" "$REDIS_PORT"; do
    state="$("$AG_PY" - "$port" <<'PY'
import socket, sys
s = socket.socket(); s.settimeout(0.5)
try:
    s.connect(("127.0.0.1", int(sys.argv[1]))); print("OPEN")
except Exception:
    print("CLOSED")
finally:
    s.close()
PY
)"
    if [ "$state" = "OPEN" ]; then
      sleep 2   # 给 Windows 句柄释放留时间后复查一次
      state="$("$AG_PY" - "$port" <<'PY'
import socket, sys
s = socket.socket(); s.settimeout(0.5)
try:
    s.connect(("127.0.0.1", int(sys.argv[1]))); print("OPEN")
except Exception:
    print("CLOSED")
finally:
    s.close()
PY
)"
    fi
    [ "$state" = "OPEN" ] && echo "PORT $port 仍开放(清理失败,需人工检查!)" | tee -a "$RAW" \
      || echo "PORT $port 已关闭" | tee -a "$RAW"
  done
  cp "$RAW" "$EVID" && echo "原始采证已更新: $EVID"
  rm -rf "$RUN"
  echo "runtime dir 已删除: $RUN"
}
trap cleanup EXIT INT TERM

wait_http() { # url label timeout_s
  local deadline=$(( $(date +%s) + $3 ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 "$1" || true)"
    [ "$code" = "200" ] && { echo "[$2] up: $1"; return 0; }
    sleep 0.5
  done
  echo "FATAL: $2 not up in $3s: $1"; exit 1
}

: >"$RAW"
note "# 生态闭环演示 · 原始采证(脚本自动生成)"
note "- 时间: $(date '+%Y-%m-%d %H:%M:%S')"
note "- 口径: mock only 零现金;同机单进程;演示=链路与计费验证,回答质量无意义"
note "- 拓扑: askbi(:$AB_PORT, LLM_PROVIDER=openai_compatible) → agentgate(:$GW_PORT, SQLite+Redis+记账) → mock upstream(:$MOCK_PORT)"
note "- 关键配置: PROFILE=full / AGENTGATE_MODELS=[\"$MODEL\"] / vk model_whitelist=[\"$MODEL\"] / tenant name=askbi"

echo "===== [1/6] 起本地 Redis(:$REDIS_PORT,无持久化) ====="
"$REDIS_BIN" --port "$REDIS_PORT" --save '' --appendonly no & PIDS+=("$!")
"$AG_PY" -c "
import asyncio, redis.asyncio as ar
async def ping():
    r = ar.from_url('redis://127.0.0.1:$REDIS_PORT/0', decode_responses=True, protocol=2)
    print('redis ping:', await r.ping())
    await r.aclose()
asyncio.run(ping())
"

echo "===== [2/6] 起 mock OpenAI 兼容上游(:$MOCK_PORT,JSON+SSE 双形态) ====="
"$AG_PY" "$ROOT/scripts/ecosystem_mock_upstream.py" --port "$MOCK_PORT" & PIDS+=("$!")
wait_http "$MOCK/health" "mock" 20

echo "===== [3/6] 起 agentgate(:$GW_PORT,REDIS+记账 on,默认渠道=mock upstream) ====="
export AGENTGATE_ENVIRONMENT=dev
export AGENTGATE_HOST=127.0.0.1 AGENTGATE_PORT=$GW_PORT
export AGENTGATE_DB_URL="sqlite+aiosqlite:///$GW_DB"
export AGENTGATE_REDIS_URL="redis://127.0.0.1:$REDIS_PORT/0"
export AGENTGATE_UPSTREAM_NAME="mock-openai"
export AGENTGATE_UPSTREAM_BASE_URL="$MOCK/v1"
export AGENTGATE_UPSTREAM_API_KEY="eco-mock-key"
export AGENTGATE_MODELS="[\"$MODEL\"]"
export AGENTGATE_AUTH_ENABLED=true
export AGENTGATE_ADMIN_TOKEN="eco-demo-admin-token"
export AGENTGATE_LOG_LEVEL=WARNING
( cd "$ROOT" && "$AG_PY" -m alembic upgrade head ) 2>&1 | tail -1
( cd "$ROOT" && exec "$AG_PY" -m uvicorn app.api.main:app --host 127.0.0.1 --port "$GW_PORT" --log-level warning ) & PIDS+=("$!")
wait_http "$GW/health" "agentgate" 30

echo "===== [4/6] seed:租户 askbi + 专用 vk(admin API,纯 ASCII 载荷) + 起 askbi ====="
TENANT_ID="$(curl -s -X POST "$GW/admin/tenants" -H "X-Admin-Token: eco-demo-admin-token" -H 'Content-Type: application/json' \
  -d '{"name":"askbi (eco-loop demo)"}' | "$AG_PY" -c "import sys,json;print(json.load(sys.stdin)['id'])")"
KEY_JSON="$(curl -s -X POST "$GW/admin/tenants/$TENANT_ID/keys" -H "X-Admin-Token: eco-demo-admin-token" -H 'Content-Type: application/json' \
  -d "{\"name\":\"eco-demo\",\"model_whitelist\":[\"$MODEL\"],\"rpm_limit\":120,\"tpm_limit\":200000}")"
RAW_KEY="$(echo "$KEY_JSON" | "$AG_PY" -c "import sys,json;print(json.load(sys.stdin)['key'])")"
KEY_PREFIX="$(echo "$KEY_JSON" | "$AG_PY" -c "import sys,json;print(json.load(sys.stdin)['prefix'])")"
printf '%s' "$TENANT_ID" > "$RUN/tenant.id"
printf '%s' "$RAW_KEY" > "$RUN/vk.raw"
note ""
note "===== S0 seed:租户 askbi + 专用 vk(白名单=[$MODEL],经 admin API) ====="
note "tenant_id=$TENANT_ID (name=askbi)"
note "vk 签发: prefix=$KEY_PREFIX (raw 不落证据,仅运行目录暂存,退出即删)"
note "vk 关键字段: name=eco-demo, model_whitelist=[\"$MODEL\"], rpm_limit=120, tpm_limit=200000, key/tenant 预算暂不设(S4 再设租户预算)"
[ -n "$TENANT_ID" ] && [ -n "$RAW_KEY" ] || { echo "FATAL: seed failed"; exit 1; }

( cd "$ASKBI_ROOT" && \
  PROFILE=full \
  LLM_PROVIDER=openai_compatible \
  OPENAI_BASE_URL="$GW/v1" \
  OPENAI_API_KEY="$(cat "$RUN/vk.raw")" \
  OPENAI_MODEL="$MODEL" \
  exec "$AB_PY" -m uvicorn app.api.main:app --host 127.0.0.1 --port "$AB_PORT" --log-level warning ) & PIDS+=("$!")
wait_http "$AB/api/health" "askbi" 90
snap curl -s "$AB/api/health"

echo "===== [5/6] 场景 + 采证(steps.py:S1 askbi 两问→S2 SSE→S3 403→S4 预算 402→S5 采证) ====="
"$AG_PY" "$ROOT/scripts/ecosystem_loop_steps.py" \
  --gw "$GW" --ab "$AB" --redis-url "redis://127.0.0.1:$REDIS_PORT/0" \
  --gw-db "$GW_DB" --raw-out "$RAW" --tenant-id "$TENANT_ID" --vk-raw-file "$RUN/vk.raw" \
  --key-prefix "$KEY_PREFIX"
STEPS_RC=$?

echo "===== [6/6] 收尾(trap 清理见下方清点) ====="
[ "$STEPS_RC" = "0" ] && echo "场景结果: ALL SCENARIOS OK" || echo "场景结果: 存在失败场景(见原始采证,如实留档)"
exit $STEPS_RC
