"""上游渠道 HTTP 客户端(httpx AsyncClient 封装)。

设计要点:
- trust_env 默认 False:该机曾配置注册表系统代理,LLM 上游必须直连(PC 会话实测教训);
- 流式与非流式共用一个连接池(生产口径:连接池大小是压测调优变量之一);
- 连接类异常不在此层吞掉,向上抛给路由层统一映射 502 —— M2 故障转移状态机需要看到原始异常类型。
"""

import httpx

from app.log import get_logger

logger = get_logger("upstream")

# 故障分类(演练实证:kill -9 上游进程 → 连接重置 → httpx.ReadError/RemoteProtocolError,漏接会成为未处理 500):
#   UPSTREAM_TRANSPORT_ERRORS:调度层统一捕获 → failover 到下一渠道
#   状态码映射(_classify):Connect*/Pool/ReadError/WriteError/RemoteProtocolError → 502(上游死了)
#                            ReadTimeout/WriteTimeout → 504(超时)
CONNECT_ERRORS = (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout, httpx.ReadError, httpx.WriteError)
READ_ERRORS = (httpx.ReadTimeout, httpx.WriteTimeout, httpx.RemoteProtocolError)
UPSTREAM_TRANSPORT_ERRORS = CONNECT_ERRORS + READ_ERRORS


def _build_headers(api_key: str) -> dict:
    """本地/内网渠道可以没有 api_key —— 空 key 时不发 Authorization(httpx 拒绝空 Bearer 值)。"""
    headers = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    return headers


class UpstreamClient:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        *,
        connect_timeout: float = 5.0,
        read_timeout: float = 300.0,
        trust_env: bool = False,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._client = client or httpx.AsyncClient(
            base_url=base_url,
            headers=_build_headers(api_key),
            trust_env=trust_env,
            timeout=httpx.Timeout(connect=connect_timeout, read=read_timeout, write=30.0, pool=5.0),
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
        )

    async def chat_completion(self, payload: dict, *, extra_headers: dict | None = None) -> httpx.Response:
        """非流式:返回完整 Response(状态码透传由路由层决定)。

        extra_headers:向下游传播的追踪上下文头(traceparent / x-request-id)。
        """
        return await self._client.post(
            "/chat/completions", json=payload, headers=extra_headers or None
        )

    async def open_stream(self, payload: dict, *, extra_headers: dict | None = None) -> httpx.Response:
        """流式:打开响应流(stream=True),字节迭代与关闭交由调用方。

        注意:调用方必须在 finally 中 `await resp.aclose()`。
        """
        req = self._client.build_request(
            "POST", "/chat/completions", json=payload, headers=extra_headers or None
        )
        return await self._client.send(req, stream=True)

    async def aclose(self) -> None:
        await self._client.aclose()
