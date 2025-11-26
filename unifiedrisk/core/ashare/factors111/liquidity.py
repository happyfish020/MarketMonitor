"""A 股流动性因子（成交额 / 换手 / 两融等综合）实战骨架."""

from __future__ import annotations

from typing import Dict, Any, Tuple


def compute_liquidity_risk(raw: Dict[str, Any] | None = None) -> Tuple[float, Dict[str, float], str]:
    """计算流动性相关风险。

    预期 raw schema:

    raw["ashare"]["liquidity"] = {
        "total_turnover": float,   # 全市场成交额（亿）
        "turnover_ratio": float,   # 总换手率（%）
        "northbound_etf_proxy": float,  # 北向代理强弱（如 510300/159919 成交/资金变化）
    }
    """
    if not raw or "ashare" not in raw or "liquidity" not in raw["ashare"]:
        return 0.0, {}, "流动性：无数据，默认中性。"

    liq = raw["ashare"]["liquidity"]

    total_turnover = float(liq.get("total_turnover", 0.0))
    turnover_ratio = float(liq.get("turnover_ratio", 0.0))
    nb_proxy = float(liq.get("northbound_etf_proxy", 0.0))

    # === 1) 成交额规模 ===
    # 示例阈值：你可根据近一年中位数动态调整
    VERY_HIGH = 12000.0
    HIGH = 9000.0
    LOW = 6000.0

    if total_turnover >= VERY_HIGH:
        scale_score = 1.5   # 极度活跃，注意后续是否冲顶
    elif total_turnover >= HIGH:
        scale_score = 1.0   # 活跃
    elif total_turnover <= LOW:
        scale_score = -1.0  # 明显缩量
    else:
        scale_score = 0.0

    # === 2) 换手率 ===
    if turnover_ratio >= 3.0:
        tr_score = 1.0
    elif turnover_ratio >= 2.0:
        tr_score = 0.5
    elif turnover_ratio <= 1.0:
        tr_score = -0.5
    else:
        tr_score = 0.0

    # === 3) 北向代理活跃度 ===
    # 这里假设 nb_proxy 已经是一个大致在 [-3, 3] 的强弱指标（如 GlobalRisk 部分输出），
    # 若你后续希望直接复用，可将其直接加权使用。
    nb_score = max(-3.0, min(3.0, nb_proxy))

    # === 聚合 ===
    total = (
        scale_score * 0.5 +
        tr_score * 0.3 +
        nb_score * 0.2
    )
    total = max(-3.0, min(3.0, total))

    comment = (
        f"流动性：全市场成交约 {total_turnover:.0f} 亿，总换手率 {turnover_ratio:.2f}%，"
        f"北向代理强弱指标 {nb_proxy:.2f}。"
    )

    detail: Dict[str, float] = {
        "scale_score": scale_score,
        "tr_score": tr_score,
        "nb_score": nb_score,
        "total_turnover": total_turnover,
        "turnover_ratio": turnover_ratio,
        "northbound_etf_proxy": nb_proxy,
    }
    return total, detail, comment
