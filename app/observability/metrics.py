"""Prometheus 指标(对标 APISIX AI 可观测 + 腾讯面经原题"业务层怎么定义成功")。

指标设计原则:分维度到排障有用的粒度(租户/渠道/状态类),不细分到 request_id(基数爆炸)。
TTFT 只对流式请求有意义 —— 面试讲点:网关的可观测性必须回答"慢在上游还是网关",
TTFT(首 token)与总延迟分开记,差值即网关+传输开销。
"""

import os

from prometheus_client import Counter, Gauge, Histogram

from app.log import get_logger

logger = get_logger("metrics")

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


# 混沌 round 3(2026-11-18 [1B4-AG] S1 实测):SIGKILL worker 的 MMAP 残留语义 ——
# prometheus_client 0.26.0 的 mark_process_dead 只清 live 系 gauge 文件
# (_LIVE_GAUGE_MULTIPROCESS_MODES),而本应用两个 Gauge 都用缺省 all 模式
# (gauge_all_{pid}.db,合并输出带 pid 标签):worker 被 kill -9(无优雅停运)时,
# 其 gauge_all 文件被每次抓取永久并入 —— USAGE_QUEUE_DEPTH{pid=死}/
# CHANNEL_COOLING{channel=...,pid=死} 冻结在死前值,sum() 类告警永不恢复。
# counter/histogram 残留按上游设计保留(累计口径连续,删了制造虚假 reset)。
# GATE-6 的优雅停运只覆盖正常退出;Dockerfile CMD 启动前 rm -rf 只覆盖容器整体
# 重启;k8s 下 master 进程内重启 worker 两者都够不到 —— 残留存活期=容器生命周期。
# 本 janitor 在 worker 启动时自动收尸:属主 pid 已死亡且文件足够老(防 pid 复用)
# 的 gauge 文件(所有模式)回收;counter/histogram 一律不碰。
GAUGE_FILE_DEAD_AGE_SECONDS = 60.0


def _pid_alive(pid: int) -> bool:
    """跨平台进程存活探测(零依赖,不用 psutil)。"""
    try:
        if os.name == "nt":
            import ctypes

            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            kernel32 = ctypes.windll.kernel32
            handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, int(pid))
            if handle:
                kernel32.CloseHandle(handle)
                return True
            return kernel32.GetLastError() == 5  # ERROR_ACCESS_DENIED:存在但无权查询
        os.kill(int(pid), 0)  # POSIX:0 信号 = 存在性探测
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except (OSError, ValueError):
        return False


def cleanup_dead_process_files(max_age_s: float = GAUGE_FILE_DEAD_AGE_SECONDS) -> int:
    """SIGKILL 残留回收(多进程模式):删掉属主 pid 已死亡且文件足够老的 gauge MMAP 文件。

    - 只动 gauge(所有模式含缺省 all):counter/histogram 是累计口径,死进程残留
      按上游设计保留(删除会在 Prometheus 侧制造虚假 counter reset);
    - age 守卫:文件 mtime 距今不足 max_age_s 的跳过 —— 刚崩溃的 worker 由下一次
      worker 启动/重启再收,同时防"死 pid 被新进程复用"误删活文件;
    - 单进程/测试口径(env 未设)no-op 返回 0;并发收尸幂等(文件没了就跳过)。
    返回删除的文件数。
    """
    import glob
    import re
    import time

    directory = os.environ.get("PROMETHEUS_MULTIPROC_DIR") or os.environ.get(
        "prometheus_multiproc_dir"
    )
    if not directory:
        return 0
    pattern = re.compile(r"^gauge_[a-z]+_(\d+)\.db$")
    removed = 0
    now = time.time()
    for f in glob.glob(os.path.join(directory, "gauge_*.db")):
        m = pattern.match(os.path.basename(f))
        if not m:
            continue
        pid = int(m.group(1))
        if pid == os.getpid():
            continue  # 自己的文件永远不收
        try:
            if now - os.path.getmtime(f) < max_age_s:
                continue  # 太新:pid 可能复用,留给下轮
        except OSError:
            continue
        if _pid_alive(pid):
            continue
        try:
            os.remove(f)
            removed += 1
        except OSError:
            pass  # 并发窗口内被别的 worker 收走
    if removed:
        logger.info("multiproc_dead_gauge_files_purged", count=removed)
    return removed


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
