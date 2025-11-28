from __future__ import annotations

from typing import Dict, Any


def score_us_daily(us_eq: Dict[str, Any], treasury: Dict[str, Any]) -> Dict[str, Any]:
    """
    us_eq 结构示例（由 AshareDataFetcher 提供）：
    {
        "spy": {"changePct": 0.95},
        "nasdaq": {"changePct": 1.20},
        "vix": {"price": 18.5, "changePct": -3.2},
        ...
    }
    treasury 示例：
    {
        "yield_curve_diff": 43.7,   # 10Y - 2Y or 10Y - 5Y (bps)
        ...
    }
    """
    spy = (us_eq.get("spy") or {}).get("changePct")
    nas = (us_eq.get("nasdaq") or {}).get("changePct")
    vix = (us_eq.get("vix") or {}).get("price")
    ycurve = treasury.get("yield_curve_diff")

    score = 50.0
    reasons = []

    # 1) 指数涨跌
    if spy is not None:
        if spy < -2.0:
            score -= 10
            reasons.append(f"SPY 大跌 {spy:.2f}%")
        elif spy < 0.0:
            score -= 3
            reasons.append(f"SPY 小幅回调 {spy:.2f}%")
        elif spy > 2.0:
            score += 6
            reasons.append(f"SPY 大涨 {spy:.2f}%")
        elif spy > 0.5:
            score += 3
            reasons.append(f"SPY 小幅上涨 {spy:.2f}%")

    if nas is not None:
        if nas < -3.0:
            score -= 8
            reasons.append(f"纳指科技股大跌 {nas:.2f}%")
        elif nas < -1.0:
            score -= 4
            reasons.append(f"纳指科技股回调 {nas:.2f}%")
        elif nas > 3.0:
            score += 5
            reasons.append(f"纳指科技股大涨 {nas:.2f}%")
        elif nas > 1.0:
            score += 2
            reasons.append(f"纳指科技股小涨 {nas:.2f}%")

    # 2) VIX
    if vix is not None:
        if vix > 25:
            score -= 12
            reasons.append(f"VIX={vix:.1f} 显著偏高，恐慌较重")
        elif vix > 20:
            score -= 6
            reasons.append(f"VIX={vix:.1f} 略偏高，风险偏好下降")
        elif vix < 15:
            score += 4
            reasons.append(f"VIX={vix:.1f} 偏低，风险偏好良好")

    # 3) 美债收益率曲线
    if ycurve is not None:
        if ycurve < 0:
            score -= 6
            reasons.append(f"美债收益率曲线倒挂（{ycurve:.1f} bp）")
        elif ycurve < 50:
            score -= 2
            reasons.append(f"美债收益率曲线偏平（{ycurve:.1f} bp）")

    score = max(0.0, min(100.0, score))

    if score >= 70:
        level = "美股环境偏友好"
    elif score >= 50:
        level = "美股环境中性"
    elif score >= 30:
        level = "美股环境偏紧"
    else:
        level = "美股环境高风险"

    desc = "；".join(reasons) if reasons else "指标中性，无明显极端信号"
    return {
        "score": score,
        "level": level,
        "desc": desc,
        "spy_pct": spy,
        "nasdaq_pct": nas,
        "vix": vix,
        "ycurve_bps": ycurve,
    }
