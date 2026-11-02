"""成本模型(P4):模型单价 × token = 金额,账本可对账、账单可导出。

价格口径:元 / 每百万 token(input/output 分列),经 `AGENTGATE_MODEL_PRICES`
(JSON 字符串)注入,例如:
    {"qwen-flash": {"input": 0.3, "output": 0.6}}
未登记的模型 cost 记 NULL —— 不猜价,宁可空也不给假数字(与 mock/real 分列同纪律)。

单价表的维护边界:启动时读 env(改价格需重启/重部署);运行时热更价目表属于
管理面功能,当前规模不做(诚实边界)。
"""

import json

from app.config import get_settings
from app.log import get_logger

logger = get_logger("billing")

# 价目表快照:app 工厂启动时从 env 加载一次(改价需重启,见模块头);
# 测试/热路径直接 set_price_table 注入。consumer 进程无 app.state,经此共享。
_PRICE_TABLE: dict | None = None


def reload_prices() -> None:
    global _PRICE_TABLE
    raw = get_settings().model_prices
    if not raw:
        _PRICE_TABLE = {}
        return
    try:
        parsed = json.loads(raw)
        _PRICE_TABLE = parsed if isinstance(parsed, dict) else {}
    except ValueError:
        logger.error("model_prices_malformed", raw=raw[:200])
        _PRICE_TABLE = {}


def set_price_table(prices: dict | None) -> None:
    """测试/运维注入(覆盖 env 快照);None = 回落到 env 重载。"""
    global _PRICE_TABLE
    _PRICE_TABLE = prices if prices is not None else None
    if prices is None:
        reload_prices()


def _table() -> dict:
    if _PRICE_TABLE is None:
        reload_prices()
    return _PRICE_TABLE or {}


def compute_cost(model: str, prompt_tokens: int, completion_tokens: int) -> float | None:
    """账本 cost 字段的唯一计算点;未登记价格返回 None(不猜)。"""
    price = _table().get(model)
    if price is None:
        return None
    try:
        return round(
            prompt_tokens / 1e6 * float(price.get("input", 0))
            + completion_tokens / 1e6 * float(price.get("output", 0)),
            8,
        )
    except (TypeError, ValueError):
        logger.error("model_price_malformed", model=model)
        return None
