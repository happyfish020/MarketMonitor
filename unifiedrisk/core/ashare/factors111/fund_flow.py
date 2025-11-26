"""资金流向因子（北向替代 / 主力 / 两融）实战骨架."""

from __future__ import annotations

from typing import Dict, Any, Tuple


def compute_fund_flow_risk(raw: Dict[str, Any] | None = None) -> Tuple[float, Dict[str, float], str]:
    """计算 A 股资金流向相关风险。

    预期 raw schema:

    raw["ashare"]["fund_flow"] = {
        "northbound_proxy": {
            "etf_510300_flow": float,   # 510300 ETF 资金流入（亿）
            "etf_159919_flow": float,   # 159919 ETF 资金流入（亿）
            "trend_3d": float,          # 近 3 日累积流入（亿）
            "trend_5d": float,          # 近 5 日累积流入（亿）
        },
        "main_fund": {
            "inflow": float,            # 主力资金净流入（亿）
        },
        "margin": {
            "change": float,            # 两融余额变化（%）
        }
    }
    """
    if not raw or "ashare" not in raw or "fund_flow" not in raw["ashare"]:
        return 0.0, {}, "资金流向：无数据，默认中性。"

    ff = raw["ashare"]["fund_flow"]

    nb = ff.get("northbound_proxy", {})
    main = ff.get("main_fund", {})
    margin = ff.get("margin", {})

    etf_510300_flow = float(nb.get("etf_510300_flow", 0.0))
    etf_159919_flow = float(nb.get("etf_159919_flow", 0.0))
    trend_3d = float(nb.get("trend_3d", 0.0))
    trend_5d = float(nb.get("trend_5d", 0.0))

    main_inflow = float(main.get("inflow", 0.0))
    margin_chg = float(margin.get("change", 0.0))

    # === 1) 北向替代因子打分 ===
    north_daily = etf_510300_flow + etf_159919_flow

    if north_daily >= 30.0:
        nb_score_today = 2.0
    elif north_daily >= 10.0:
        nb_score_today = 1.0
    elif north_daily <= -30.0:
        nb_score_today = -2.0
    elif north_daily <= -10.0:
        nb_score_today = -1.0
    else:
        nb_score_today = 0.0

    trend_sum = trend_3d + trend_5d

    if trend_sum >= 80.0:
        nb_score_trend = 2.0
    elif trend_sum >= 30.0:
        nb_score_trend = 1.0
    elif trend_sum <= -80.0:
        nb_score_trend = -2.0
    elif trend_sum <= -30.0:
        nb_score_trend = -1.0
    else:
        nb_score_trend = 0.0

    nb_score = 0.6 * nb_score_today + 0.4 * nb_score_trend

    # === 2) 主力资金打分 ===
    if main_inflow >= 50.0:
        main_score = 2.0
    elif main_inflow >= 20.0:
        main_score = 1.0
    elif main_inflow <= -50.0:
        main_score = -2.0
    elif main_inflow <= -20.0:
        main_score = -1.0
    else:
        main_score = 0.0

    # === 3) 两融余额打分 ===
    if margin_chg >= 2.0:
        margin_score = 1.0
    elif margin_chg >= 0.5:
        margin_score = 0.5
    elif margin_chg <= -2.0:
        margin_score = -1.0
    elif margin_chg <= -0.5:
        margin_score = -0.5
    else:
        margin_score = 0.0

    # === 总分聚合 ===
    total = (
        nb_score * 0.5 +
        main_score * 0.3 +
        margin_score * 0.2
    )

    # 限制在 [-3, 3]
    total = max(-3.0, min(3.0, total))

    comment = (
        f"资金流向：北向代理当日 {north_daily:.1f} 亿，3+5日合计 {trend_sum:.1f} 亿，"
        f"主力净流 {main_inflow:.1f} 亿，两融余额变化 {margin_chg:.2f}%。"
    )

    detail: Dict[str, float] = {
        "north_daily": north_daily,
        "trend_3d": trend_3d,
        "trend_5d": trend_5d,
        "nb_score": nb_score,
        "main_inflow": main_inflow,
        "main_score": main_score,
        "margin_chg": margin_chg,
        "margin_score": margin_score,
    }
    return total, detail, comment
