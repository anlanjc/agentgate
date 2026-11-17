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


def _chaos_child_src() -> str:
    """混沌 round 3 S1 子进程:写 counter+gauge 后直接退出(不 mark_process_dead,
    等价 SIGKILL 残留 —— uvicorn worker 被强杀时走不到 lifespan 收尾)。"""
    import textwrap

    return textwrap.dedent("""
        import sys
        sys.path.insert(0, sys.argv[1])
        from app.observability import metrics
        metrics.REQUESTS.labels(tenant="s1", channel="dead-ch", status_class="2xx").inc(3.0)
        metrics.CHANNEL_COOLING.labels(channel="dead-ch").set(1.0)
    """)


def test_killed_worker_gauge_residue_purged_on_next_worker_start(mp_dir, tmp_path):
    """混沌 round 3(2026-11-18)S1 回归:SIGKILL worker 的 point-in-time gauge 残留。

    实测语义(prometheus_client 0.26.0):mark_process_dead 只清 gauge 文件;
    counter/histogram 残留按上游设计保留(累计连续)。真缺陷 = 死 worker 的
    gauge_live 冻结值被永久并入每次抓取(USAGE_QUEUE_DEPTH/CHANNEL_COOLING
    永不恢复)——GATE-6 的优雅停运/Dockerfile rm -rf 都覆盖不到"master 进程内
    重启 worker"的 k8s 场景。修复 = 新 worker 启动时 cleanup_dead_process_files()
    收尸(死 pid + mtime 老龄守卫)。"""
    import glob
    import os as _os
    import subprocess
    import time as _time

    from app.observability.metrics import cleanup_dead_process_files, render_metrics

    script = tmp_path / "chaos_worker.py"
    script.write_text(_chaos_child_src(), encoding="utf-8")
    for _ in range(2):
        subprocess.run([sys.executable, str(script), str(_REPO)], check=True, capture_output=True)
        # 子进程已退出:其 pid 在文件名里,但进程表里已无 —— 残留成立
    files = glob.glob(_os.path.join(str(mp_dir), "*.db"))
    dead_pids = {int(f.rsplit("_", 1)[1].split(".")[0]) for f in files}
    assert dead_pids, "子进程未留下 MMAP 文件(装置失效)"
    assert len(glob.glob(_os.path.join(str(mp_dir), "gauge_*.db"))) == len(dead_pids)

    # 修复前语义:counter 累计连续(8.0)+ 死进程 gauge 冻结值仍在抓取里(幻影,
    # 带 pid 标签 —— 0.26.0 gauge 缺省 all 模式,mark_process_dead 的 live 系
    # glob 覆盖不到它)
    body = render_metrics().decode()
    counter_line = next(
        l for l in body.splitlines()
        if l.startswith("agentgate_requests_total{") and '"dead-ch"' in l
    )
    assert float(counter_line.rsplit(" ", 1)[1]) == 6.0  # 2 个 worker × 3.0
    assert 'agentgate_channel_cooling{channel="dead-ch"' in body  # 死 worker 的冻结 gauge

    # 老龄化(越过 60s 守卫)+ 收尸:全部死 pid 的 gauge 文件应清零
    for f in glob.glob(_os.path.join(str(mp_dir), "gauge_*.db")):
        past = _time.time() - 120
        _os.utime(f, (past, past))
    removed = cleanup_dead_process_files()
    assert removed == len(dead_pids), f"收尸数量不符: removed={removed}"

    # gauge 文件清零(dead pid 的),counter 文件保留(累计口径按上游设计)
    remaining_gauges = glob.glob(_os.path.join(str(mp_dir), "gauge_*.db"))
    assert not remaining_gauges, f"死进程 gauge 文件未收尸: {remaining_gauges}"
    assert glob.glob(_os.path.join(str(mp_dir), "counter_*.db")), "counter 文件不应被收尸(累计连续)"
    body = render_metrics().decode()
    assert 'agentgate_channel_cooling{channel="dead-ch"' not in body  # 幻影消失
    counter_line = next(
        l for l in body.splitlines()
        if l.startswith("agentgate_requests_total{") and '"dead-ch"' in l
    )
    assert float(counter_line.rsplit(" ", 1)[1]) == 6.0  # 2 个 worker × 3.0  # counter 连续性不受收尸影响

    # age 守卫:新文件(mtime=now)即使属主已死也不清(防 pid 复用误删)
    fresh = _os.path.join(str(mp_dir), f"gauge_all_{dead_pids.pop()}.db")
    if not _os.path.exists(fresh):
        open(fresh, "wb").close()  # 空文件即可:收尸只看文件名/mtime/pid 存活,不读内容
    _os.utime(fresh, None)  # mtime=now:60s 守卫内
    removed2 = cleanup_dead_process_files()
    assert _os.path.exists(fresh), "60s 守卫内的新文件被误删(pid 复用防护失效)"
    assert removed2 == 0, f"守卫内的文件不应计入删除: removed={removed2}"


def test_lifespan_startup_calls_dead_file_janitor():
    """混沌 round 3:worker 启动路径必须挂收尸调用(静态回归,GATE-1 风格)。"""
    from pathlib import Path

    src = (Path(__file__).resolve().parents[1] / "app" / "api" / "main.py").read_text(
        encoding="utf-8"
    )
    assert "cleanup_dead_process_files" in src, "lifespan startup 未挂 SIGKILL 残留收尸"
