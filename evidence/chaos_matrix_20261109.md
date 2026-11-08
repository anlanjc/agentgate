# 混沌矩阵 e2e（2026-11-09，S9 Wave2）：redis 故障域 × 流式记账

**结论：6/6 PASS**（两轮真实缺陷发现并当日修复，本批当时口径 123+2skip / ruff 全绿；现口径随测试增长,唯一出处见 EVIDENCE「终态」节）

| 场景 | 注入 | 期望 | 实测 | 结果 |
|---|---|---|---|---|
| C1 正常请求 | 无 | 200 + 账本落行 | 200, rows=1 | ✅ |
| C1b 账本对账 | 无 | request_id 恰 1 行 | rows=1 | ✅ |
| C2 redis 断连(非流式) | terminate redis | 503 admission_unavailable(fail-closed) | 503 + 统一信封 | ✅ |
| C3 redis 重启恢复 | 新空实例 | 200 恢复 | 200 | ✅(修复后) |
| C4 GW-5 流式客户端中途断开 | read 2 chunks 后 abort | 账本恰 1 行(不漏不重) | rows=1 | ✅(修复后) |
| C5 redis 断连(流式) | terminate redis | 503(准入先行) | 503 | ✅ |

## 混沌抓到的两个真实缺陷（当日修复）

1. **NOGROUP 死循环**（commit 1676a7d）：redis 重启(数据丢失)后 consumer group 随之消失，
   worker 的 `_run` 循环把 NOGROUP 当普通异常无限重试、永不重建 → 账本消费停摆至应用重启
   （C4 rows=0 实锤，日志 NOGROUP × N）。修复=`_run` 异常分支识别 NOGROUP → `ensure_group()`
   自愈重建（id="0" 补消费孤儿条目）；回归=test_worker_self_heals_after_redis_data_loss。
2. **NOSCRIPT 判定失配**（commit 82f3dbd）：redis 重启后脚本缓存清空，EVALSHA 报
   "No matching script. Please use EVAL."——redis-py 的 message **不含字面 NOSCRIPT**（那是错误码），
   `eval_cached` 的回退判定永远不命中 → 恢复后准入/配额/failover fail-closed 停摆
   （run5 恢复窗 admission_fail_closed ×15）。修复=判定扩为 NOSCRIPT ∥ No matching script；
   回归=script_flush 后回退重载测试。

## 测试装置自身教训（同样入册）

- 僵尸服务两次污染实验：8000 有已死演示会话的 askbi 常驻服务（health 由它答、chat 405）；
  6379 有 11-06 会话的 301MB 僵尸 redis + 脚本收尾未清理的孤儿实例（terminate 的是自己的
  进程，应答的是僵尸——"断连注入"从未真正发生）。**混沌实验第一课：先清场，注入前验证
  "目标真的断了"**（本例以 fail-closed 是否触发为断言恰好把装置缺陷暴露出来）。
- 进程内 uvicorn 关闭时 lifespan CancelledError 为良性噪声，不影响结果判定。

## 复现

```
.venv/Scripts/python.exe <chaos_gw.py>   # 需先清空 6379/8000 端口占用
.venv/Scripts/python.exe -m pytest tests/test_m2_streams.py tests/test_m1_ratelimit.py -q
```
