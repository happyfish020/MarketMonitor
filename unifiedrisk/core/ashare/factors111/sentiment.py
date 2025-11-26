"""A 股市场情绪因子（涨跌停结构 / 炸板率等）实战版骨架."""

from __future__ import annotations

from typing import Dict, Any, Tuple


def compute_sentiment_risk(raw: Dict[str, Any] | None = None) -> Tuple[float, Dict[str, float], str]:
    """计算情绪相关风险。

    预期 raw schema:

    raw["ashare"]["sentiment"] = {
        "limit_up": int,               # 涨停家数
        "limit_down": int,             # 跌停家数
        "open_limit_up": int,          # 开板数（炸板总数）
        "open_limit_up_success": int,  # 最终封板成功数
        "high_leaders": {
            "max_continuous_limit": int,
            "leading_sectors": list[str],
        },
    }
    """
    if not raw or "ashare" not in raw or "sentiment" not in raw["ashare"]:
        return 0.0, {}, "情绪：无数据，默认中性。"

    s = raw["ashare"]["sentiment"]

    lu = int(s.get("limit_up", 0))
    ld = int(s.get("limit_down", 0))
    open_lu = int(s.get("open_limit_up", 0))
    success_lu = int(s.get("open_limit_up_success", 0))

    high = s.get("high_leaders", {}) or {}
    max_cont = int(high.get("max_continuous_limit", 0))

    # === 1) 涨跌停结构 ===
    # 简化经验规则：
    # - 涨停多且跌停少 → 正
    # - 跌停多且涨停少 → 负
    net_up = lu - ld

    if lu >= 80 and ld <= 10 and net_up >= 60:
        lu_score = 2.0
    elif lu >= 50 and net_up >= 30:
        lu_score = 1.5
    elif lu >= 30 and net_up >= 10:
        lu_score = 1.0
    elif ld >= 50 and net_up <= -30:
        lu_score = -2.0
    elif ld >= 30 and net_up <= -10:
        lu_score = -1.5
    elif ld >= 20 and net_up < 0:
        lu_score = -1.0
    else:
        lu_score = 0.0

    # === 2) 炸板率 ===
    # 炸板率 = 开板数 / (涨停 + 1e-6)，开板多则情绪不稳
    if lu > 0:
        fail_lu = max(open_lu - success_lu, 0)
        fail_ratio = fail_lu / max(lu, 1)
    else:
        fail_lu = 0
        fail_ratio = 0.0

    if fail_ratio >= 0.7 and lu > 20:
        zb_score = -2.0
    elif fail_ratio >= 0.5 and lu > 20:
        zb_score = -1.5
    elif fail_ratio >= 0.3 and lu > 10:
        zb_score = -1.0
    elif fail_ratio <= 0.15 and lu > 20:
        zb_score = 0.5   # 炸板率低，情绪较稳
    else:
        zb_score = 0.0

    # === 3) 高标高度 ===
    # 高度越高，说明情绪越亢奋，但也要防止情绪过热。
    if max_cont >= 7:
        high_score = -1.0   # 过热，易见顶
    elif max_cont >= 5:
        high_score = 0.5
    elif max_cont >= 3:
        high_score = 0.2
    else:
        high_score = 0.0

    # === 聚合 ===
    total = lu_score * 0.6 + zb_score * 0.25 + high_score * 0.15
    total = max(-3.0, min(3.0, total))

    comment = (
        f"情绪：涨停 {lu} 家，跌停 {ld} 家，炸板率约 {fail_ratio * 100:.1f}%，"
        f"最高连板高度 {max_cont} 板。"
    )

    detail: Dict[str, float] = {
        "lu_score": lu_score,
        "zb_score": zb_score,
        "high_score": high_score,
        "limit_up": float(lu),
        "limit_down": float(ld),
        "fail_ratio": float(fail_ratio),
        "max_continuous_limit": float(max_cont),
    }
    return total, detail, comment
