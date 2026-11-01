"""SSE(Server-Sent Events)辅助:流式转发中的最小解析。

只做两件事,不做完整 SSE 解析器:
1. 探测首个完整 `data:` 行 —— TTFT(首 token 延迟)的锚点;
2. 从字节流中提取 usage 行(上游开启 stream_options.include_usage 时)。

为什么按字节缓冲:aiter_raw 的分块边界与 SSE 行边界无关(可能一条事件跨两个 TCP 分段),
逐字节转发必须保留原始字节(零改动透传),解析只旁观不参与。
"""

from collections.abc import AsyncIterator

DONE_MARKER = b"data: [DONE]"


class FirstDataEventDetector:
    """跨块缓冲探测首个 `data:` 行。fire-once,命中后零开销。"""

    def __init__(self, max_buffer: int = 65536) -> None:
        self._buf = b""
        self._max_buffer = max_buffer
        self.fired = False

    def feed(self, chunk: bytes) -> bool:
        """喂入一段原始字节;若本次喂入使首条完整 data: 行出现,返回 True(仅一次)。"""
        if self.fired:
            return False
        self._buf += chunk
        while True:
            nl = self._buf.find(b"\n")
            if nl < 0:
                if len(self._buf) > self._max_buffer:  # 防御:无换行的异常流
                    self._buf = b""
                return False
            line, self._buf = self._buf[:nl], self._buf[nl + 1 :]
            if line.strip().startswith(b"data:"):
                self.fired = True
                return True


class SSELineParser:
    """旁观解析完整 SSE data: 行,产出 JSON payload(仅用于提取 usage 等元数据)。

    纪律:解析器与转发器解耦 —— 转发仍走原始字节(零改动),解析只旁观副本。
    """

    def __init__(self, max_buffer: int = 262_144) -> None:
        self._buf = b""
        self._max_buffer = max_buffer

    def feed(self, chunk: bytes) -> list[dict]:
        """喂入原始字节,返回本次凑齐的完整 data: JSON payload 列表。"""
        payloads: list[dict] = []
        self._buf += chunk
        while True:
            nl = self._buf.find(b"\n")
            if nl < 0:
                if len(self._buf) > self._max_buffer:
                    self._buf = b""
                break
            line, self._buf = self._buf[:nl], self._buf[nl + 1 :]
            stripped = line.strip()
            if not stripped.startswith(b"data:"):
                continue
            body = stripped[len(b"data:"):].strip()
            if not body or body == b"[DONE]":
                continue
            try:
                import json

                payloads.append(json.loads(body))
            except (ValueError, UnicodeDecodeError):
                continue
        return payloads


class UsageAccumulator:
    """从 SSE 流中提取最终 usage(上游 include_usage 语义:最后一个带 usage 的事件为准)。"""

    def __init__(self) -> None:
        self._parser = SSELineParser()
        self.usage: dict | None = None

    def feed(self, chunk: bytes) -> None:
        for payload in self._parser.feed(chunk):
            usage = payload.get("usage")
            if usage:
                self.usage = usage


async def relay_stream(
    source: AsyncIterator[bytes],
    on_first_event=None,
    on_chunk=None,
) -> AsyncIterator[bytes]:
    """旁观式转发:原样 yield 字节,仅在首个 data: 行时回调 on_first_event(ttft_ms)。

    解析器不修改任何字节 —— 透传纪律。
    """
    detector = FirstDataEventDetector()
    import time

    t0 = time.perf_counter()
    async for chunk in source:
        if detector.feed(chunk) and on_first_event:
            on_first_event((time.perf_counter() - t0) * 1000)
        if on_chunk:
            on_chunk(len(chunk))
        yield chunk

