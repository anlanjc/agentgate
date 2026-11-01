"""/v1 OpenAI 兼容路由:M2 完整链路 鉴权 → 限流 → 预算 → 渠道调度(故障转移)→ 异步记账。

请求路径纪律(呼应 LiteLLM "no database write sits in the request path"):
- 请求路径只做:鉴权读(Redis→DB 回源)、限流 EVAL、配额预检 GET —— 全部 O(1) 读;
- 记账(Redis INCRBY + PG 落库)通过 BackgroundTask 在响应完成后执行;
- 渠道健康状态(EMA 延迟/冷却)存 Redis,多副本共享。
"""

import time
from collections.abc import AsyncIterator

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from sqlalchemy import select
from starlette.background import BackgroundTask

from app.auth import keys as auth_keys
from app.auth.keys import AuthResult, KeyInactive, KeyNotFound
from app.db.models import Tenant
from app.errors import GatewayError
from app.log import current_request_id, get_logger
from app.observability import metrics
from app.quota import service as quota_service
from app.ratelimit.admit import admit
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


def _retry_after_s(resp: httpx.Response) -> int:
    try:
        return max(1, int(resp.headers.get("retry-after", "60")))
    except ValueError:
        return 60


def _classify_upstream_exc(exc: Exception) -> tuple[int, str, str]:
    """传输异常 → (状态码, message, code)。真超时=504;对端中断/连不上=502。"""
    import httpx as _hx

    if isinstance(exc, _hx.ReadTimeout) or isinstance(exc, _hx.WriteTimeout):
        return 504, "upstream timeout", "upstream_timeout"
    return 502, "upstream unreachable", "upstream_connect_failed"


# ---------- 渠道调度(故障转移状态机,师承 LiteLLM routing) ----------


async def _dispatch_non_stream(
    state, payload: dict, headers: dict
) -> tuple[str | None, httpx.Response]:
    """按候选顺序尝试;返回 (channel_name|None, response)。

    全渠道失败时返回最后一条上游错误响应(透传语义);连接类异常向上抛。
    """
    r: ChannelRouter = state.channel_router
    last_resp: httpx.Response | None = None
    last_exc: Exception | None = None
    for name in await r.candidates():
        client = r.client(name)
        t0 = time.perf_counter()
        try:
            resp = await client.chat_completion(payload, extra_headers=headers)
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
    # 空候选 = 全部渠道冷却中:明确 503,而不是 raise None(TypeError)
    raise GatewayError(
        503, "all upstream channels cooling down", "bad_gateway",
        "no_channel_available", retry_after_s=5,
    )


async def _dispatch_stream(
    state, payload: dict, headers: dict
) -> tuple[str | None, httpx.Response, bool]:
    """返回 (channel|None, resp, opened)。opened=False 时 resp 已读完(合成/错误响应)。"""
    r: ChannelRouter = state.channel_router
    last_synthetic: httpx.Response | None = None
    last_exc: Exception | None = None
    for name in await r.candidates():
        client = r.client(name)
        t0 = time.perf_counter()
        try:
            resp = await client.open_stream(payload, extra_headers=headers)
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
        503, "all upstream channels cooling down", "bad_gateway",
        "no_channel_available", retry_after_s=5,
    )


# ---------- 准入与记账 ----------


def _bearer(raw: str | None) -> str:
    if not raw or not raw.startswith("Bearer "):
        raise GatewayError(401, "missing bearer key", "invalid_request_error", "missing_api_key")
    return raw[len("Bearer "):].strip()


async def _authenticate(request: Request, raw_key: str) -> AuthResult:
    state = request.app.state
    try:
        return await auth_keys.lookup(state.redis, state.session_factory, raw_key)
    except KeyNotFound:
        raise GatewayError(401, "invalid api key", "authentication_error", "key_not_found")
    except KeyInactive:
        raise GatewayError(401, "api key revoked", "authentication_error", "key_inactive")


async def _admit(request: Request, auth: AuthResult, model: str) -> None:
    """准入三关(单次 Lua 往返):模型白名单 → RPM 令牌桶(硬上界)→ TPM 窗口/预算预检(计量制)。"""
    state = request.app.state
    if auth.model_whitelist and model not in auth.model_whitelist:
        raise GatewayError(
            403, f"model '{model}' not allowed for this key", "invalid_request_error", "model_not_allowed"
        )
    r = await admit(state.redis, key_id=auth.key_id, capacity=auth.rpm_limit, amount=1)
    if not r.rpm_allowed:
        raise GatewayError(
            429, "rpm limit exceeded", "rate_limit_error", "rpm_exceeded",
            retry_after_s=max(1, r.retry_after_ms // 1000),
        )
    if r.tpm_window >= auth.tpm_limit:
        raise GatewayError(429, "tpm limit exceeded", "rate_limit_error", "tpm_exceeded", retry_after_s=60)
    if auth.budget_tokens is not None and r.budget_spent >= auth.budget_tokens:
        raise GatewayError(402, "budget exhausted", "insufficient_quota", "budget_exceeded")


def _record(
    auth: AuthResult | None, model: str, request_id: str,
    status: int, usage: dict | None, ttft_ms: float | None, latency_ms: float,
    channel: str = "",
) -> dict:
    u = usage or {}
    return {
        "request_id": request_id,
        "key_id": auth.key_id if auth else "anonymous",
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


async def _account(request: Request, auth: AuthResult | None, record_or_factory) -> None:
    """响应完成后的记账(BackgroundTask):预算/TPM 计数(Redis 原子)+ 账本落库(单写者队列)。

    record_or_factory:流式路径传可调用对象(usage 在流结束后才可知,后台执行时才求值);
    非流式路径直接传 dict。webhook 回调任务在此上下文入队(允许 DB 访问)。
    """
    state = request.app.state
    record = record_or_factory() if callable(record_or_factory) else record_or_factory
    if auth is not None and record["status"] == 200 and record["total_tokens"] > 0:
        await quota_service.account_tokens(state.redis, auth.key_id, record["total_tokens"])
    state.writer.submit(record)
    metrics.USAGE_QUEUE_DEPTH.set(state.writer._queue.qsize())

    # webhook 回调(幂等键=request_id):请求完成事件推送给订阅方
    if auth is not None and record["status"] == 200 and state.settings.auth_enabled:
        from app.queue.streams import TaskQueue

        tq: TaskQueue = state.task_queue
        async with state.session_factory() as db:
            tenant = (
                await db.execute(select(Tenant).where(Tenant.id == auth.tenant_id))
            ).scalar_one_or_none()
            if tenant and tenant.webhook_url:
                await tq.enqueue(
                    db,
                    "webhook_callback",
                    {
                        "webhook_url": tenant.webhook_url,
                        "request_id": record["request_id"],
                        "model": record["model"],
                        "total_tokens": record["total_tokens"],
                    },
                    idempotency_key=f"webhook:{record['request_id']}",
                )


@router.post("/chat/completions")
async def chat_completions(req: ChatCompletionRequest, request: Request) -> Response:
    state = request.app.state
    payload = req.upstream_payload()
    request_id = current_request_id()

    # ---- 准入(auth_enabled=False 为纯代理模式,仅测试/内网边缘鉴权场景)----
    auth: AuthResult | None = None
    if state.settings.auth_enabled:
        auth = await _authenticate(request, _bearer(request.headers.get("authorization")))
        await _admit(request, auth, req.model)

    forward_headers = {k: v for k, v in request.headers.items() if k in _FORWARD_HEADERS}

    # ---- 流式:先调度渠道并开流(连接类异常发生在状态码提交前,可安全映射)----
    if req.stream:
        try:
            channel, resp, opened = await _dispatch_stream(state, payload, forward_headers)
        except UPSTREAM_TRANSPORT_ERRORS as exc:
            status, message, code = _classify_upstream_exc(exc)
            logger.warning("upstream_transport_error", error=str(exc), status=status)
            state.writer.submit(_record(auth, req.model, request_id, status, None, None, 0))
            return _gateway_error(status, message, "bad_gateway", code)

        if not opened:
            # 渠道遍历后的合成响应(非流式错误透传 或 全渠道耗尽)
            return Response(
                content=resp.content,
                status_code=resp.status_code,
                media_type="application/json",
                headers={"x-upstream-channel": channel or "exhausted"},
            )

        holder: dict = {"usage": None, "ttft_ms": None}

        async def relay() -> AsyncIterator[bytes]:
            t0 = time.perf_counter()
            acc = UsageAccumulator()

            def on_first(ttft_ms: float) -> None:
                holder["ttft_ms"] = round(ttft_ms, 2)
                state.ttft_samples.append(round(ttft_ms, 2))
                logger.info("ttft_ms", value=round(ttft_ms, 2), channel=channel)

            try:
                async for chunk in relay_stream(resp.aiter_raw(), on_first_event=on_first):
                    acc.feed(chunk)  # 旁观解析 usage,不影响透传字节
                    yield chunk
            except READ_ERRORS as exc:
                logger.warning("upstream_read_error_midstream", error=str(exc))
                yield b'event: error\ndata: {"message": "upstream read timeout mid-stream"}\n\n'
            finally:
                await resp.aclose()
                holder["usage"] = acc.usage
                logger.info(
                    "stream_done", duration_ms=round((time.perf_counter() - t0) * 1000, 2)
                )

        def make_record() -> dict:
            return _record(
                auth, req.model, request_id, 200, holder["usage"], holder["ttft_ms"], 0,
                channel=channel or "",
            )

        tenant_label = auth.tenant_id if auth else "anonymous"
        if holder["ttft_ms"] is not None:
            metrics.TTFT.labels(channel=channel or "exhausted").observe(holder["ttft_ms"])
        metrics.REQUESTS.labels(
            tenant=tenant_label, channel=channel or "exhausted", status_class="2xx"
        ).inc()
        metrics.TOKENS.labels(tenant=tenant_label, model=req.model, kind="prompt").inc(
            (holder["usage"] or {}).get("prompt_tokens", 0)
        )
        metrics.TOKENS.labels(tenant=tenant_label, model=req.model, kind="completion").inc(
            (holder["usage"] or {}).get("completion_tokens", 0)
        )

        return StreamingResponse(
            relay(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
                "x-upstream-channel": channel or "exhausted",
            },
            background=BackgroundTask(_account, request, auth, make_record),
        )
    # 注意:传 make_record 本身(可调用),后台任务执行时才求值 —— 流式 usage 在流结束才可知

    # ---- 非流式 ----
    t0 = time.perf_counter()
    try:
        channel, resp = await _dispatch_non_stream(state, payload, forward_headers)
    except UPSTREAM_TRANSPORT_ERRORS as exc:
        status, message, code = _classify_upstream_exc(exc)
        logger.warning("upstream_transport_error", error=str(exc), status=status)
        state.writer.submit(_record(auth, req.model, request_id, status, None, None, 0))
        return _gateway_error(status, message, "bad_gateway", code)

    latency_ms = (time.perf_counter() - t0) * 1000
    usage = None
    if resp.status_code == 200:
        try:
            usage = resp.json().get("usage")
        except ValueError:
            usage = None
    record = _record(
        auth, req.model, request_id, resp.status_code, usage, None, latency_ms,
        channel=channel or "",
    )
    tenant_label = auth.tenant_id if auth else "anonymous"
    if resp.status_code == 200:
        metrics.REQUEST_LATENCY.labels(channel=channel or "exhausted").observe(latency_ms)
    metrics.REQUESTS.labels(
        tenant=tenant_label,
        channel=channel or "exhausted",
        status_class=metrics.status_class(resp.status_code),
    ).inc()
    if usage:
        metrics.TOKENS.labels(tenant=tenant_label, model=req.model, kind="prompt").inc(
            usage.get("prompt_tokens", 0)
        )
        metrics.TOKENS.labels(tenant=tenant_label, model=req.model, kind="completion").inc(
            usage.get("completion_tokens", 0)
        )
    return Response(
        content=resp.content,
        status_code=resp.status_code,
        media_type="application/json",
        headers={"x-upstream-channel": channel or "exhausted"},
        background=BackgroundTask(_account, request, auth, record),
    )


@router.get("/models")
async def list_models(request: Request) -> dict:
    settings = request.app.state.settings
    return {
        "object": "list",
        "data": [{"id": m, "object": "model", "owned_by": settings.upstream_name} for m in settings.models],
    }
