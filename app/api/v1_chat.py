"""/v1 OpenAI 兼容路由:M2 完整链路 鉴权 → 限流 → 预算 → 渠道调度(故障转移)→ 异步记账。

请求路径纪律(呼应 LiteLLM "no database write sits in the request path"):
- 请求路径只做:鉴权读(Redis→DB 回源)、限流 EVAL、配额预检 GET —— 全部 O(1) 读;
- 记账(Redis INCRBY + PG 落库)通过 BackgroundTask 在响应完成后执行;
- 渠道健康状态(EMA 延迟/冷却)存 Redis,多副本共享。
"""

import asyncio
import json
import time
from collections.abc import AsyncIterator
from contextlib import nullcontext

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from sqlalchemy import select
from starlette.background import BackgroundTask

from app.auth import keys as auth_keys
from app.auth.keys import AuthResult, KeyInactive, KeyNotFound
from app.cache import response as response_cache
from app.cache import semantic as semantic_cache
from app.db.models import Tenant
from app.errors import GatewayError
from app.log import current_request_id, get_logger
from app.observability import metrics
from app.quota import service as quota_service
from app.ratelimit.admit import AdmitResult, admit
from app.router.failover import ChannelRouter
from app.schemas.openai import ChatCompletionRequest, GatewayErrorBody
from app.upstream.client import READ_ERRORS, UPSTREAM_TRANSPORT_ERRORS
from app.upstream.sse import UsageAccumulator, relay_stream

router = APIRouter(prefix="/v1")
logger = get_logger("v1")

# 追踪上下文向下游传播;x-mock-case 是测试故障注入钩子(仅 mock 上游识别,生产上游忽略)
_FORWARD_HEADERS = ("traceparent", "x-request-id", "x-mock-case")


def _gateway_error(status: int, message: str, err_type: str, code: str) -> JSONResponse:
    body = GatewayErrorBody.model_validate(
        {"error": {"message": message, "type": err_type, "code": code, "request_id": current_request_id()}}
    )
    return JSONResponse(status_code=status, content=body.model_dump())


def r_tracer(state):
    """路由层 tracer(未启用 OTel 时为 None,零开销路径)。"""
    return getattr(state, "tracer", None)


def _retry_after_s(resp: httpx.Response) -> int:
    try:
        return max(1, int(resp.headers.get("retry-after", "60")))
    except ValueError:
        return 60


def _classify_upstream_exc(exc: Exception) -> tuple[int, str, str]:
    """传输异常 → (状态码, message, code)。真超时=504;对端中断/连不上=502。"""
    if isinstance(exc, (httpx.ReadTimeout, httpx.WriteTimeout)):
        return 504, "upstream timeout", "upstream_timeout"
    return 502, "upstream unreachable", "upstream_connect_failed"


# ---------- 渠道调度(故障转移状态机,师承 LiteLLM routing) ----------


async def _dispatch_non_stream(
    state, payload: dict, headers: dict,
    trace_ctx=None, captured: dict | None = None,
) -> tuple[str | None, httpx.Response]:
    """按候选顺序尝试;返回 (channel_name|None, response)。

    全渠道失败时返回最后一条上游错误响应(透传语义);连接类异常向上抛。
    """
    r: ChannelRouter = state.channel_router
    last_resp: httpx.Response | None = None
    last_exc: Exception | None = None
    model = payload.get("model", "")
    for name in await r.candidates(model):
        client = r.client(name)
        t0 = time.perf_counter()
        tracer = r_tracer(state)
        span_ctx = (
            tracer.start_as_current_span(
                "gateway.upstream",
                attributes={
                    "gateway.channel": name,
                    "gateway.model": model,
                    "gateway.request_id": current_request_id(),
                },
                context=trace_ctx,
            )
            if tracer else nullcontext()
        )
        try:
            with span_ctx as span:
                resp = await client.chat_completion(
                    payload, extra_headers=_with_traceparent(headers, captured, tracer)
                )
                if span:
                    span.set_attribute("http.status_code", resp.status_code)
        except UPSTREAM_TRANSPORT_ERRORS as exc:
            await r.mark_failure(name)
            last_exc = exc
            continue
        latency = (time.perf_counter() - t0) * 1000
        if resp.status_code == 429:
            await r.mark_rate_limited(name, _retry_after_s(resp))
            last_resp = resp
            continue
        if resp.status_code >= 500:
            await r.mark_failure(name)
            last_resp = resp
            continue
        await r.mark_success(name, latency)
        return name, resp
    if last_resp is not None:
        return None, last_resp
    if last_exc is not None:
        raise last_exc
    # 空候选 = 无渠道支持该模型/全部冷却:明确 503,而不是 raise None(TypeError)
    raise GatewayError(
        503, f"no healthy upstream channel for model '{model}'", "bad_gateway",
        "no_channel_available", retry_after_s=5,
    )


async def _dispatch_stream(
    state, payload: dict, headers: dict,
    trace_ctx=None, captured: dict | None = None,
) -> tuple[str | None, httpx.Response, bool]:
    """返回 (channel|None, resp, opened)。opened=False 时 resp 已读完(合成/错误响应)。"""
    r: ChannelRouter = state.channel_router
    last_synthetic: httpx.Response | None = None
    last_exc: Exception | None = None
    model = payload.get("model", "")
    for name in await r.candidates(model):
        client = r.client(name)
        t0 = time.perf_counter()
        tracer = r_tracer(state)
        span_ctx = (
            tracer.start_as_current_span(
                "gateway.upstream_stream",
                attributes={
                    "gateway.channel": name,
                    "gateway.model": model,
                    "gateway.request_id": current_request_id(),
                },
                context=trace_ctx,
            )
            if tracer else nullcontext()
        )
        try:
            with span_ctx as span:
                resp = await client.open_stream(
                    payload, extra_headers=_with_traceparent(headers, captured, tracer)
                )
                if span:
                    span.set_attribute("http.status_code", resp.status_code)
        except UPSTREAM_TRANSPORT_ERRORS as exc:
            await r.mark_failure(name)
            last_exc = exc
            continue
        if resp.status_code == 429:
            await r.mark_rate_limited(name, _retry_after_s(resp))
            await resp.aclose()
            continue
        if resp.status_code >= 500:
            await r.mark_failure(name)
            await resp.aclose()
            continue
        if "text/event-stream" not in resp.headers.get("content-type", ""):
            # 非流式错误响应(如 400):读完转合成响应,保持透传语义
            body = await resp.aread()
            await resp.aclose()
            synth = httpx.Response(
                resp.status_code, content=body, headers=dict(resp.headers)
            )
            if resp.status_code >= 500:
                await r.mark_failure(name)
                last_synthetic = synth
                continue
            await r.mark_success(name, (time.perf_counter() - t0) * 1000)
            return name, synth, False
        await r.mark_success(name, (time.perf_counter() - t0) * 1000)
        return name, resp, True
    if last_synthetic is not None:
        return None, last_synthetic, False
    if last_exc is not None:
        raise last_exc
    raise GatewayError(
        503, f"no healthy upstream channel for model '{model}'", "bad_gateway",
        "no_channel_available", retry_after_s=5,
    )


# ---------- 准入与记账 ----------


def _bearer(raw: str | None) -> str:
    if not raw or not raw.startswith("Bearer "):
        raise GatewayError(401, "missing bearer key", "invalid_request_error", "missing_api_key")
    return raw[len("Bearer "):].strip()


def _last_user_question(req: ChatCompletionRequest) -> str:
    return next(
        (
            m.content for m in reversed(req.messages)
            if m.role == "user" and isinstance(m.content, str)
        ),
        "",
    )


async def _authenticate(request: Request, raw_key: str) -> AuthResult:
    state = request.app.state
    try:
        return await auth_keys.lookup(state.redis, state.session_factory, raw_key)
    except KeyNotFound:
        raise GatewayError(401, "invalid api key", "authentication_error", "key_not_found")
    except KeyInactive:
        raise GatewayError(401, "api key revoked", "authentication_error", "key_inactive")


async def _admit(request: Request, auth: AuthResult, model: str) -> AdmitResult:
    """准入链(单次 Lua 往返):模型白名单 → RPM 令牌桶(硬上界)→ TPM/预算预检(计量制)
    → 租户预算兜底 → 每 key 并发上限。

    Redis 故障 = fail-closed 503(与鉴权/缓存的降级语义相反):准入承载计费硬上界,
    跳过意味着限流与预算全部失效 —— 宁可拒绝也不裸放。
    返回 AdmitResult 供限流响应头使用。
    """
    state = request.app.state
    if auth.model_whitelist and model not in auth.model_whitelist:
        raise GatewayError(
            403, f"model '{model}' not allowed for this key", "invalid_request_error", "model_not_allowed"
        )
    from redis.exceptions import RedisError

    try:
        r = await admit(
            state.redis, key_id=auth.key_id, capacity=auth.rpm_limit, amount=1,
            tenant_id=auth.tenant_id if auth.tenant_budget_tokens is not None else None,
        )
    except RedisError as exc:
        logger.warning("admission_fail_closed", error=str(exc))
        raise GatewayError(
            503, "admission temporarily unavailable", "api_error",
            "admission_unavailable", retry_after_s=5,
        )
    if not r.rpm_allowed:
        raise GatewayError(
            429, "rpm limit exceeded", "rate_limit_error", "rpm_exceeded",
            retry_after_s=max(1, r.retry_after_ms // 1000),
        )
    if r.tpm_window >= auth.tpm_limit:
        raise GatewayError(429, "tpm limit exceeded", "rate_limit_error", "tpm_exceeded", retry_after_s=60)
    if auth.budget_tokens is not None and r.budget_spent >= auth.budget_tokens:
        raise GatewayError(402, "budget exhausted", "insufficient_quota", "budget_exceeded")
    if auth.tenant_budget_tokens is not None and r.tenant_budget_spent >= auth.tenant_budget_tokens:
        # 租户级兜底:换新 key 也绕不过(计数器跟租户走,不跟 key)
        raise GatewayError(
            402, "tenant budget exhausted", "insufficient_quota", "tenant_budget_exceeded",
        )
    if auth.concurrency_limit is not None:
        from app.ratelimit import concurrency

        ok = await concurrency.acquire(state.redis, auth.key_id, auth.concurrency_limit)
        if not ok:
            raise GatewayError(
                429, "concurrency limit exceeded for this key", "rate_limit_error",
                "concurrency_exceeded", retry_after_s=1,
            )
    return r


def _with_traceparent(headers: dict, captured: dict | None, tracer=None) -> dict:
    """W3C 注入(P4-2):追踪启用且调用方没带 traceparent 时,从当前 span 生成
    一条给上游(跨服务 trace 连续);调用方带了则原样透传并回记(响应头回传)。

    门控用 app 自己的 tracer(而非模块全局):FastAPI 0.162+ 原生 telemetry 会在
    全局 provider 存在时自动开 span —— 本 app 关闭追踪时不能被别的进程态污染。
    """
    if tracer is None:
        return headers
    if "traceparent" in headers:
        if captured is not None:
            captured["traceparent"] = headers["traceparent"]
        return headers
    from opentelemetry.propagate import inject

    carrier: dict = {}
    inject(carrier)  # 当前 span 上下文 → 新 span id,同一 trace
    tp = carrier.get("traceparent")
    if tp and captured is not None:
        captured["traceparent"] = tp
    return {**headers, "traceparent": tp} if tp else headers


def _rate_limit_headers(auth: AuthResult | None, r: AdmitResult | None) -> dict:
    """标准限流响应头(有鉴权且准入已执行时附带;匿名/纯代理模式不带)。"""
    if auth is None or r is None:
        return {}
    reset_s = max(1, (r.retry_after_ms + 999) // 1000) if not r.rpm_allowed else max(
        1, int(60 / max(auth.rpm_limit, 1))
    )
    return {
        "x-ratelimit-limit-requests": str(auth.rpm_limit),
        "x-ratelimit-remaining-requests": str(r.rpm_remaining),
        "x-ratelimit-reset-requests": str(reset_s),
    }


def _record(
    auth: AuthResult | None, model: str, request_id: str,
    status: int, usage: dict | None, ttft_ms: float | None, latency_ms: float,
    channel: str = "",
) -> dict:
    u = usage or {}
    return {
        "request_id": request_id,
        # 纯代理模式没有 virtual key:key_id 为空,账本仍可写(PG 外键不再误伤)。
        "key_id": auth.key_id if auth else None,
        "tenant_id": auth.tenant_id if auth else "anonymous",
        "model": model,
        "prompt_tokens": int(u.get("prompt_tokens", 0)),
        "completion_tokens": int(u.get("completion_tokens", 0)),
        "total_tokens": int(u.get("total_tokens", 0)),
        "ttft_ms": ttft_ms,
        "latency_ms": int(latency_ms),
        "status": status,
        "channel": channel,
    }


def _idempotency_key(kind: str, auth: AuthResult | None, request_id: str) -> str:
    """账本/回调的幂等键必须带租户域。

    request_id 允许客户端经 X-Request-Id 传入 —— 如果幂等键只有 request_id,
    任何租户都能用一个重复 ID 让其他租户的账本被 UNIQUE 吞掉(计费 DoS)。
    """
    tenant = auth.tenant_id if auth else "anonymous"
    return f"{kind}:{tenant}:{request_id}"


def _record_metrics(
    auth: AuthResult | None,
    model: str,
    channel: str,
    status: int,
    usage: dict | None,
    *,
    latency_ms: float | None = None,
    ttft_ms: float | None = None,
) -> None:
    """指标只在响应真实结束后记一次(流式必须等 relay 结束,不能提前)。"""
    tenant_label = auth.tenant_id if auth else "anonymous"
    metrics.REQUESTS.labels(
        tenant=tenant_label, channel=channel or "exhausted", status_class=metrics.status_class(status)
    ).inc()
    if latency_ms is not None:
        metrics.REQUEST_LATENCY.labels(channel=channel or "exhausted").observe(latency_ms)
    if ttft_ms is not None:
        metrics.TTFT.labels(channel=channel or "exhausted").observe(ttft_ms)
    if usage:
        metrics.TOKENS.labels(tenant=tenant_label, model=model, kind="prompt").inc(
            usage.get("prompt_tokens", 0)
        )
        metrics.TOKENS.labels(tenant=tenant_label, model=model, kind="completion").inc(
            usage.get("completion_tokens", 0)
        )


async def _enqueue_error(state, auth, model: str, request_id: str, status: int) -> None:
    """错误路径的账本行:也走 Streams(错误低频,允许路径内小开销换全链一致)。"""
    from app.queue.streams import TaskQueue

    tq: TaskQueue = state.task_queue
    try:
        async with state.session_factory() as db:
            await tq.enqueue(
                db, "usage_write",
                _record(auth, model, request_id, status, None, None, 0),
                idempotency_key=_idempotency_key("usage", auth, request_id),
            )
    except Exception as exc:
        logger.warning("error_record_enque_failed", error=str(exc))
    finally:
        # 错误路径没有 BackgroundTask 记账:并发槽在这里释放(成功路径由 _account 释放)
        if auth is not None and auth.concurrency_limit is not None:
            from app.ratelimit import concurrency

            await concurrency.release(state.redis, auth.key_id)


async def _account(request: Request, auth: AuthResult | None, record_or_factory) -> None:
    """响应完成后的记账(BackgroundTask):账本先持久化,再更新 Redis 快路径计数。

    顺序很重要:账本是事实来源,Redis 是可重建的缓存。先 enqueue 意味着即使
    记账时 Redis 抖动,账本仍在;预算计数可由 rebuild_from_db 修复。
    record_or_factory:流式路径传可调用对象(usage 流结束才可知)。
    同时是并发槽的释放点(响应真正结束;流式在流末尾,这正是"在途"的语义)。
    """
    state = request.app.state
    record = record_or_factory() if callable(record_or_factory) else record_or_factory
    from app.queue.streams import TaskQueue

    tq: TaskQueue = state.task_queue
    try:
        async with state.session_factory() as db:
            await tq.enqueue(
                db, "usage_write", record,
                idempotency_key=_idempotency_key("usage", auth, record["request_id"]),
            )

            # webhook 回调(幂等键=租户+request_id):请求完成事件推送给订阅方
            if auth is not None and record["status"] == 200 and state.settings.auth_enabled:
                tenant = (
                    await db.execute(select(Tenant).where(Tenant.id == auth.tenant_id))
                ).scalar_one_or_none()
                if tenant and tenant.webhook_url:
                    await tq.enqueue(
                        db,
                        "webhook_callback",
                        {
                            "webhook_url": tenant.webhook_url,
                            "webhook_secret": tenant.webhook_secret,
                            "request_id": record["request_id"],
                            "model": record["model"],
                            "total_tokens": record["total_tokens"],
                        },
                        idempotency_key=_idempotency_key("webhook", auth, record["request_id"]),
                    )
    except Exception as exc:
        logger.exception("usage_enqueue_failed", error=str(exc))

    # 有真实 token 消耗就计量(流式中途断开也已产生的 token 不应漏计)
    if auth is not None and record["total_tokens"] > 0:
        try:
            await quota_service.account_tokens(
                state.redis, auth.key_id, record["total_tokens"], tenant_id=auth.tenant_id
            )
        except Exception as exc:
            logger.warning("quota_account_failed", error=str(exc))

    try:
        metrics.USAGE_QUEUE_DEPTH.set(await tq.backlog())
    except Exception as exc:
        logger.warning("queue_depth_metric_failed", error=str(exc))

    # 并发槽释放(响应真正结束;DECR 下限 0,重复释放安全)
    if auth is not None and auth.concurrency_limit is not None:
        from app.ratelimit import concurrency

        await concurrency.release(state.redis, auth.key_id)


@router.post("/chat/completions")
async def chat_completions(req: ChatCompletionRequest, request: Request) -> Response:
    state = request.app.state
    request_t0 = time.perf_counter()
    payload = req.upstream_payload()
    request_id = current_request_id()

    # ---- 准入(auth_enabled=False 为纯代理模式,仅测试/内网边缘鉴权场景)----
    auth: AuthResult | None = None
    r_admit = None
    if state.settings.auth_enabled:
        auth = await _authenticate(request, _bearer(request.headers.get("authorization")))
        r_admit = await _admit(request, auth, req.model)
    rl_headers = _rate_limit_headers(auth, r_admit)

    # 计费模式下的流式请求强制 include_usage:否则客户端不传该参数时,
    # 上游不返回 usage,网关只能记 0 token(真实成本漏计)。请求体被增强,
    # 响应仍是字节级透传;客户端会多收到一个 usage-only chunk(OpenAI 规范允许)。
    if req.stream and state.settings.auth_enabled and state.settings.force_stream_usage:
        stream_options = dict(payload.get("stream_options") or {})
        stream_options["include_usage"] = True
        payload["stream_options"] = stream_options

    forward_headers = {k: v for k, v in request.headers.items() if k in _FORWARD_HEADERS}

    # ---- Idempotency-Key(P2):重试安全,仅非流式(流式响应体不可重放,照常执行并留痕)----
    idem_sk: str | None = None
    raw_idem = (request.headers.get("idempotency-key") or "").strip()
    if raw_idem:
        if auth is None:
            logger.warning("idempotency_requires_auth", request_id=request_id)
        elif req.stream:
            logger.warning("idempotency_skipped_for_stream", request_id=request_id)
        else:
            from app import idempotency

            idem_sk = idempotency.storage_key(auth.tenant_id, auth.key_id, raw_idem)

    # ---- W3C Trace Context(P4-2):client traceparent 提取为父上下文;响应头回传 ----
    from app.observability import tracing as otel_tracing

    trace_ctx = (
        otel_tracing.incoming_context(request.headers) if r_tracer(state) else None
    )
    captured: dict = {"traceparent": None}

    # ---- 流式:先调度渠道并开流(连接类异常发生在状态码提交前,可安全映射)----
    if req.stream:
        try:
            channel, resp, opened = await _dispatch_stream(
                state, payload, forward_headers, trace_ctx, captured
            )
        except UPSTREAM_TRANSPORT_ERRORS as exc:
            status, message, code = _classify_upstream_exc(exc)
            logger.warning("upstream_transport_error", error=str(exc), status=status)
            await _enqueue_error(state, auth, req.model, request_id, status)
            return _gateway_error(status, message, "bad_gateway", code)
        except GatewayError as exc:
            await _enqueue_error(state, auth, req.model, request_id, exc.status)
            raise

        if not opened:
            # 渠道遍历后的合成响应(非流式错误透传 或 全渠道耗尽):也要进账本,
            # 否则审计里会缺掉一整类流式失败请求。
            latency_ms = (time.perf_counter() - request_t0) * 1000
            record = _record(
                auth, req.model, request_id, resp.status_code, None, None, latency_ms,
                channel=channel or "exhausted",
            )
            _record_metrics(
                auth, req.model, channel or "exhausted", resp.status_code, None,
                latency_ms=latency_ms if resp.status_code == 200 else None,
            )
            return Response(
                content=resp.content,
                status_code=resp.status_code,
                media_type="application/json",
                headers={"x-upstream-channel": channel or "exhausted"},
                background=BackgroundTask(_account, request, auth, record),
            )

        holder: dict = {"usage": None, "ttft_ms": None, "status": 200, "latency_ms": 0}

        async def relay() -> AsyncIterator[bytes]:
            acc = UsageAccumulator()

            def on_first(ttft_ms: float) -> None:
                holder["ttft_ms"] = round(ttft_ms, 2)
                state.ttft_samples.append(round(ttft_ms, 2))
                logger.info("ttft_ms", value=round(ttft_ms, 2), channel=channel)

            try:
                async for chunk in relay_stream(
                    resp.aiter_raw(), on_first_event=on_first, start_time=request_t0
                ):
                    acc.feed(chunk)  # 旁观解析 usage,不影响透传字节
                    yield chunk
            except asyncio.CancelledError:
                # 客户端断开:上游可能已产生 token,账本标 499,不伪装成 2xx。
                holder["status"] = 499
                raise
            except READ_ERRORS as exc:
                holder["status"] = 502
                logger.warning("upstream_read_error_midstream", error=str(exc))
                yield b'event: error\ndata: {"message": "upstream read timeout mid-stream"}\n\n'
            finally:
                await resp.aclose()
                holder["usage"] = acc.usage
                holder["latency_ms"] = int((time.perf_counter() - request_t0) * 1000)
                # 指标必须在流真正结束后记:此前在构造 StreamResponse 时记,
                # TTFT/usage 永远还是 None/0,线上看板会展示假数据。
                _record_metrics(
                    auth,
                    req.model,
                    channel or "exhausted",
                    holder["status"],
                    holder["usage"],
                    ttft_ms=holder["ttft_ms"],
                )
                logger.info(
                    "stream_done",
                    duration_ms=holder["latency_ms"],
                    status=holder["status"],
                )

        def make_record() -> dict:
            return _record(
                auth, req.model, request_id, holder["status"],
                holder["usage"], holder["ttft_ms"], holder["latency_ms"],
                channel=channel or "",
            )

        return StreamingResponse(
            relay(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
                "x-upstream-channel": channel or "exhausted",
                **rl_headers,
                **({"traceparent": captured["traceparent"]} if captured.get("traceparent") else {}),
            },
            background=BackgroundTask(_account, request, auth, make_record),
        )
    # 注意:传 make_record 本身(可调用),后台任务执行时才求值 —— 流式 usage 在流结束才可知

    # ---- 非流式 ----
    # Idempotency-Key 优先于一切缓存:重放是客户端重试语义的最强承诺。
    if idem_sk is not None:
        from app import idempotency

        stored = await idempotency.lookup(state.redis, idem_sk)
        if stored is not None:
            logger.info("idempotent_replay", request_id=request_id)
            # 回放不重计费:账本记 channel=replay、token=0(上游零成本,预算/TPM 不动)
            record = _record(auth, req.model, request_id, stored["status"], None, None, 0, channel="replay")
            _record_metrics(auth, req.model, "replay", stored["status"], None)
            return Response(
                content=json.dumps(stored["body"], ensure_ascii=False).encode(),
                status_code=stored["status"],
                media_type="application/json",
                headers={"x-idempotent-replay": "true", "x-upstream-channel": "replay"},
                background=BackgroundTask(_account, request, auth, record),
            )
        if not await idempotency.acquire(state.redis, idem_sk):
            raise GatewayError(
                409, "request with this Idempotency-Key is still in flight",
                "invalid_request_error", "idempotency_key_conflict", retry_after_s=2,
            )
    try:
        return await _non_stream_core(
            req, request, state, payload, request_id, request_t0, auth, idem_sk,
            forward_headers, rl_headers, trace_ctx, captured,
        )
    finally:
        if idem_sk is not None:
            from app import idempotency

            await idempotency.release(state.redis, idem_sk)


async def _semantic_search(state, auth: AuthResult, model: str, ctx: str, question: str):
    """语义缓存检索后端分发(P4-4):redis 暴力余弦 | pgvector KNN。"""
    s = state.settings
    if s.semantic_backend == "pgvector":
        from app.cache import semantic_pg

        return await semantic_pg.search_pg(
            state.engine, state.embedder, auth.tenant_id, model, ctx, question,
            s.semantic_cache_threshold, ttl=s.semantic_cache_ttl,
            dim=s.semantic_pg_vector_dim,
        )
    return await semantic_cache.search(
        state.redis, state.embedder, auth.tenant_id, model, ctx, question,
        s.semantic_cache_threshold,
    )


async def _semantic_store(state, auth: AuthResult, model: str, ctx: str, question: str, body_json: dict) -> None:
    s = state.settings
    if s.semantic_backend == "pgvector":
        from app.cache import semantic_pg

        await semantic_pg.store_pg(
            state.engine, state.embedder, auth.tenant_id, model, ctx, question,
            body_json, s.semantic_cache_ttl, dim=s.semantic_pg_vector_dim,
        )
        return
    await semantic_cache.store(
        state.redis, state.embedder, auth.tenant_id, model, ctx, question,
        body_json, s.semantic_cache_ttl,
    )


async def _non_stream_core(
    req: ChatCompletionRequest, request: Request, state, payload: dict,
    request_id: str, request_t0: float, auth: AuthResult | None,
    idem_sk: str | None, forward_headers: dict, rl_headers: dict | None = None,
    trace_ctx=None, captured: dict | None = None,
) -> Response:
    """非流式主路径(从 chat_completions 拆出以承载 Idempotency-Key 的 try/finally)。"""
    from app import idempotency

    # 响应缓存(默认关;只缓确定性请求,租户隔离键,语义见 app/cache/response.py)
    rkey = None
    exact_hit = False
    if auth is not None and response_cache.cache_enabled(state.settings, req):
        rkey = response_cache.cache_key(auth.tenant_id, req.model, payload)
        cached = await response_cache.get(state.redis, rkey)
        if cached is not None:
            exact_hit = True
            metrics.CACHE_HITS.inc()
            logger.info("cache_hit", request_id=request_id)
            latency_ms = (time.perf_counter() - request_t0) * 1000
            record = _record(
                auth, req.model, request_id, 200, cached.get("usage"), None, latency_ms,
                channel="cache",
            )
            _record_metrics(
                auth, req.model, "cache", 200, cached.get("usage"), latency_ms=latency_ms
            )
            return Response(
                content=json.dumps(cached, ensure_ascii=False).encode(),
                status_code=200,
                media_type="application/json",
                headers={"x-cache": "HIT", "x-upstream-channel": "cache"},
                background=BackgroundTask(_account, request, auth, record),
            )
        metrics.CACHE_MISSES.inc()

    # 单飞锁(P2 hardening):同 prompt 并发 miss 只放一个去上游,其余短轮询等填。
    # 等到 → 直接回缓存;等不到(1s 预算耗尽)→ 自己执行,不陪葬持锁者。
    sf_acquired = False
    if rkey is not None:
        if await response_cache.try_acquire(state.redis, rkey):
            sf_acquired = True
        else:
            filled = await response_cache.wait_for_fill(state.redis, rkey)
            if filled is not None:
                latency_ms = (time.perf_counter() - request_t0) * 1000
                record = _record(
                    auth, req.model, request_id, 200, filled.get("usage"), None,
                    latency_ms, channel="cache",
                )
                _record_metrics(auth, req.model, "cache", 200, filled.get("usage"),
                                latency_ms=latency_ms)
                return Response(
                    content=json.dumps(filled, ensure_ascii=False).encode(),
                    status_code=200,
                    media_type="application/json",
                    headers={"x-cache": "HIT", "x-upstream-channel": "cache",
                             **(rl_headers or {})},
                    background=BackgroundTask(_account, request, auth, record),
                )

    # 语义缓存(精确未命中时):问题向量召回,阈值以上直接复用答案。
    # context_digest 绑定 system/历史消息与采样参数,防止不同 system prompt 串答案。
    semantic_ctx = None
    question = _last_user_question(req)
    if auth is not None and semantic_cache.semantic_enabled(state.settings, req):
        semantic_ctx = semantic_cache.context_digest(req.model, payload)
    if auth is not None and not exact_hit and semantic_ctx is not None and question:
        hit = await _semantic_search(state, auth, req.model, semantic_ctx, question)
        if hit is not None:
            if sf_acquired:
                await response_cache.release(state.redis, rkey)
                sf_acquired = False
            metrics.CACHE_HITS.inc()
            logger.info("semantic_cache_hit", request_id=request_id, score=hit["score"])
            latency_ms = (time.perf_counter() - request_t0) * 1000
            record = _record(
                auth, req.model, request_id, 200,
                hit["answer"].get("usage"), None, latency_ms, channel="cache-semantic",
            )
            _record_metrics(
                auth, req.model, "cache-semantic", 200,
                hit["answer"].get("usage"), latency_ms=latency_ms,
            )
            return Response(
                content=json.dumps(hit["answer"], ensure_ascii=False).encode(),
                status_code=200,
                media_type="application/json",
                headers={
                    "x-cache": "SEMANTIC_HIT",
                    "x-cache-score": str(hit["score"]),
                    "x-upstream-channel": "cache-semantic",
                },
                background=BackgroundTask(_account, request, auth, record),
            )
        metrics.CACHE_MISSES.inc()

    try:
        channel, resp = await _dispatch_non_stream(
            state, payload, forward_headers, trace_ctx, captured
        )
    except UPSTREAM_TRANSPORT_ERRORS as exc:
        if sf_acquired:
            await response_cache.release(state.redis, rkey)
        status, message, code = _classify_upstream_exc(exc)
        logger.warning("upstream_transport_error", error=str(exc), status=status)
        await _enqueue_error(state, auth, req.model, request_id, status)
        return _gateway_error(status, message, "bad_gateway", code)
    except GatewayError as exc:
        if sf_acquired:
            await response_cache.release(state.redis, rkey)
        await _enqueue_error(state, auth, req.model, request_id, exc.status)
        raise

    latency_ms = (time.perf_counter() - request_t0) * 1000
    usage = None
    body_json = None
    if resp.status_code == 200:
        try:
            body_json = resp.json()
            usage = body_json.get("usage")
        except ValueError:
            usage = None
        if rkey is not None and body_json is not None:
            await response_cache.put(state.redis, rkey, body_json, state.settings.response_cache_ttl)
            if sf_acquired:
                await response_cache.release(state.redis, rkey)
                sf_acquired = False
        # 语义缓存入库(精确与语义同源;问题取最后一条 user 消息)
        if (
            auth is not None
            and semantic_ctx is not None
            and question
            and body_json is not None
        ):
            await _semantic_store(state, auth, req.model, semantic_ctx, question, body_json)
    record = _record(
        auth, req.model, request_id, resp.status_code, usage, None, latency_ms,
        channel=channel or "",
    )
    _record_metrics(
        auth, req.model, channel or "exhausted", resp.status_code, usage,
        latency_ms=latency_ms if resp.status_code == 200 else None,
    )
    if sf_acquired:
        await response_cache.release(state.redis, rkey)  # 非 200 未填缓存:兜底释放
    extra_headers = {"x-upstream-channel": channel or "exhausted", **(rl_headers or {})}
    if captured and captured.get("traceparent"):
        extra_headers["traceparent"] = captured["traceparent"]
    if rkey is not None:
        extra_headers["x-cache"] = "MISS"
    if idem_sk is not None and 200 <= resp.status_code < 300 and body_json is not None:
        # 成功响应才进入重放库存(错误响应视为瞬态,重试重新执行);
        # 释放锁由外层 finally 兜底,这里存完即释放,窗口最小化。
        await idempotency.store(
            state.redis, idem_sk, resp.status_code, body_json,
            state.settings.idempotency_ttl_seconds,
        )
        await idempotency.release(state.redis, idem_sk)
    return Response(
        content=resp.content,
        status_code=resp.status_code,
        media_type="application/json",
        headers=extra_headers,
        background=BackgroundTask(_account, request, auth, record),
    )


@router.get("/models")
async def list_models(request: Request) -> dict:
    settings = request.app.state.settings
    return {
        "object": "list",
        "data": [{"id": m, "object": "model", "owned_by": settings.upstream_name} for m in settings.models],
    }
