"""Prometheus 指标(对标 APISIX AI 可观测 + 腾讯面经原题"业务层怎么定义成功")。

指标设计原则:分维度到排障有用的粒度(租户/渠道/状态类),不细分到 request_id(基数爆炸)。
TTFT 只对流式请求有意义 —— 面试讲点:网关的可观测性必须回答"慢在上游还是网关",
TTFT(首 token)与总延迟分开记,差值即网关+传输开销。
"""

from prometheus_client import Counter, Gauge, Histogram

REQUESTS = Counter(
    "agentgate_requests_total",
    "网关处理的补全请求数",
    ["tenant", "channel", "status_class"],  # status_class: 2xx/4xx/5xx
)

TOKENS = Counter(
    "agentgate_tokens_total",
    "token 消耗(计费口径)",
    ["tenant", "model", "kind"],  # kind: prompt / completion
)

REQUEST_LATENCY = Histogram(
    "agentgate_request_latency_ms",
    "非流式请求总延迟(毫秒)",
    ["channel"],
    buckets=(5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000),
)

TTFT = Histogram(
    "agentgate_ttft_ms",
    "流式首 token 延迟(毫秒)",
    ["channel"],
    buckets=(10, 25, 50, 100, 200, 400, 800, 1600, 3200, 6400),
)

USAGE_QUEUE_DEPTH = Gauge(
    "agentgate_usage_queue_depth",
    "任务队列真实积压(未投递 + PEL;ACK 后 XDEL,应接近 0)",
)

TASKS_DEAD = Counter(
    "agentgate_tasks_dead_total",
    "进入死信流的任务数(正常应为 0;>0 需查 DLQ 或 webhook)",
)

CACHE_HITS = Counter(
    "agentgate_cache_hits_total",
    "精确响应缓存命中数(仅 temperature≤0 非流式无工具请求参与缓存)",
)

CACHE_MISSES = Counter(
    "agentgate_cache_misses_total",
    "精确响应缓存未命中数",
)

CHANNEL_COOLING = Gauge(
    "agentgate_channel_cooling",
    "渠道是否冷却中(1=冷却,0=健康)",
    ["channel"],
)


def status_class(status: int) -> str:
    if 200 <= status < 300:
        return "2xx"
    if 400 <= status < 500:
        return "4xx"
    return "5xx"


def render_metrics() -> bytes:
    """GATE-6(评审 4.0):/metrics 渲染 —— 按部署形态选 registry。

    多进程部署(uvicorn --workers N,PROMETHEUS_MULTIPROC_DIR 已设):各 worker
    的指标落在共享目录的 MMAP 文件,经 MultiProcessCollector 合并输出 —— 修复前
    直出进程内默认 registry,workers 共享监听 socket 下每次抓取只落到其中一个
    worker(轮流取半份 + 虚假 counter reset),HighErrorRate/ThroughputDrop/
    StreamingTTFTP99High 告警系统性漏报/误抖。
    单进程/测试/开发口径(env 未设):回退进程内默认 registry,行为与修复前一致。
    """
    import os

    from prometheus_client import CollectorRegistry, generate_latest, multiprocess

    if os.environ.get("PROMETHEUS_MULTIPROC_DIR") or os.environ.get("prometheus_multiproc_dir"):
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)
        return generate_latest(registry)
    from prometheus_client import REGISTRY

    return generate_latest(REGISTRY)
