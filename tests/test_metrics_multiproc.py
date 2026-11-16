"""GATE-6(评审 4.0)回归:多进程指标聚合 —— /metrics 不再轮流取半份。

根因:Dockerfile CMD `--workers 2`(README 还建议再加 workers)共享监听 socket,
而 /metrics 直出 prometheus_client 进程内默认 registry —— 每次抓取连接只落到其中
一个 worker:agentgate_requests_total/ttft_ms 等按抓取轮流取半份并伴随虚假 counter
reset,HighErrorRate/ThroughputDrop/StreamingTTFTP99High 告警在交付的生产拓扑里
系统性漏报/误抖。修复:PROMETHEUS_MULTIPROC_DIR 多进程模式(MMAP 文件)+
MultiProcessCollector 合并抓取 + lifespan 停运 mark_process_dead 清死进程文件 +
Dockerfile CMD 启动前清残留目录(SIGKILL 遗留兜底)。
"""

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[1]


def _child_script() -> str:
    """模拟一个 worker 子进程:env 已由父进程注入,递增指标后退出(MMAP 文件留存)。"""
    return textwrap.dedent("""
        import sys
        sys.path.insert(0, sys.argv[3])
        from app.observability import metrics
        metrics.REQUESTS.labels(tenant="mp-t", channel="mp-c", status_class="2xx").inc(float(sys.argv[1]))
        metrics.TTFT.labels(channel="mp-c").observe(float(sys.argv[2]))
    """)


@pytest.fixture
def mp_dir(tmp_path, monkeypatch):
    d = tmp_path / "mp"
    d.mkdir()
    monkeypatch.setenv("PROMETHEUS_MULTIPROC_DIR", str(d))
    return d


def test_multiproc_merge_across_worker_processes(mp_dir, tmp_path):
    """两个子进程(模拟两个 worker)各自递增指标 → 合并抓取必须给出总和。

    修复前:每抓取只落到共享 socket 上被接受的那个 worker —— requests_total
    轮流取半份(3 或 5,永不 8)且伴随虚假 counter reset。"""
    script = tmp_path / "child_worker.py"
    script.write_text(_child_script(), encoding="utf-8")
    for amount, ttft in ((3, 120.0), (5, 220.0)):
        subprocess.run(
            [sys.executable, str(script), str(amount), str(ttft), str(_REPO)],
            check=True, capture_output=True, cwd=str(_REPO),
        )

    from app.observability.metrics import render_metrics

    body = render_metrics().decode()
    line = next(
        l for l in body.splitlines()
        if l.startswith("agentgate_requests_total{") and '"mp-c"' in l
    )
    # 合并口径 = 两个"worker"之和(单进程默认 registry 口径只会是 3 或 5)
    assert float(line.rsplit(" ", 1)[1]) == 8.0
    # TTFT 直方图跨进程合并:120ms 落 le=200 计数 1,220ms 落 le=400 → 累计计数 2
    bucket200 = next(
        l for l in body.splitlines()
        if l.startswith("agentgate_ttft_ms_bucket{") and '"mp-c"' in l and 'le="200.0"' in l
    )
    assert float(bucket200.rsplit(" ", 1)[1]) == 1.0
    bucket400 = next(
        l for l in body.splitlines()
        if l.startswith("agentgate_ttft_ms_bucket{") and '"mp-c"' in l and 'le="400.0"' in l
    )
    assert float(bucket400.rsplit(" ", 1)[1]) == 2.0


def test_metrics_endpoint_falls_back_to_default_registry_without_env(monkeypatch):
    """env 未设(单进程/测试/开发口径):/metrics 回退进程内默认 registry,
    行为与修复前一致 —— 多进程模式只在显式部署时启用。"""
    monkeypatch.delenv("PROMETHEUS_MULTIPROC_DIR", raising=False)
    monkeypatch.delenv("prometheus_multiproc_dir", raising=False)

    from app.observability.metrics import render_metrics

    body = render_metrics().decode()
    # 无标签 Gauge 恒有样本:单进程口径必须出现(而非空 merged 输出)
    assert "agentgate_usage_queue_depth" in body
    # 子进程 MMAP 口径的 mp-c 指标不在进程内默认 registry 里
    assert '"mp-c"' not in body


async def test_lifespan_shutdown_marks_process_dead(mp_dir, monkeypatch, tmp_path):
    """优雅停运清本进程 MMAP 文件 —— 不清则死进程计数器永久并入抓取(虚假高值)。

    经 lifespan_context 驱动 startup+shutdown,断言 mark_process_dead 被调用;
    SIGKILL 等异常退出的残留由 Dockerfile CMD 启动前 rm -rf 兜底(静态回归见
    test_deploy_config.py::test_dockerfile_multiproc_metrics_env)。"""
    from prometheus_client import multiprocess

    called: list[int] = []
    monkeypatch.setattr(multiprocess, "mark_process_dead", lambda pid: called.append(pid))

    from app.api.main import create_app
    from app.db.session import init_db
    from tests.conftest import _base_settings

    app = create_app(settings=_base_settings(tmp_path))
    await init_db(app.state.engine)  # startup 的 load_channels 查 channels 表
    try:
        async with app.router.lifespan_context(app):
            pass  # startup → yield → shutdown
    finally:
        await app.state.engine.dispose()
    assert os.getpid() in called, "lifespan shutdown 未调用 mark_process_dead:死进程 MMAP 文件将永久并入抓取"
