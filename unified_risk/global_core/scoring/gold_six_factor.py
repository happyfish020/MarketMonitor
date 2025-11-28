# global_risk/scoring/gold_six_factor.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any, Optional


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


@dataclass
class GoldSixView:
    real_rate_view: str
    usd_view: str
    vix_view: str
    fed_view: str
    geo_view: str
    inflation_view: str
    conclusion: str


def build_gold_six_view(raw_macro: Dict[str, Any]) -> GoldSixView:
    """
    基于 6 个维度对黄金中短期环境做文字解读：
      1. 实际利率（目前用名义 10Y 粗略代替）
      2. 美元走势
      3. VIX 风险偏好
      4. 美联储预期（用收益率曲线简化）
      5. 地缘风险（目前占位描述）
      6. 通胀预期（目前占位描述）
    """
    ten_y = _safe_float(raw_macro.get("treasury_10y"))
    yc_bps = _safe_float(raw_macro.get("ycurve_bps"))
    vix = _safe_float(raw_macro.get("vix_last"))
    dxy = _safe_float(raw_macro.get("dxy_change") or raw_macro.get("dxy_pct"))

    # 1) “实际利率”视角（粗略）
    if ten_y is None:
        real_rate_view = "实际利率环境未知（未接入 10Y 利率数据）。"
    elif ten_y > 4:
        real_rate_view = "10 年期利率偏高，对黄金构成一定利空。"
    elif ten_y < 2:
        real_rate_view = "10 年期利率偏低，对黄金偏利多。"
    else:
        real_rate_view = "10 年期利率在中性区间，对黄金影响温和。"

    # 2) 美元走势
    if dxy is None:
        usd_view = "美元走势未知。"
    elif dxy > 0.5:
        usd_view = "美元近期走强，对金价有压制。"
    elif dxy < -0.5:
        usd_view = "美元回落，对金价形成支撑。"
    else:
        usd_view = "美元波动不大，对金价影响有限。"

    # 3) VIX 风险偏好
    if vix is None:
        vix_view = "VIX 水平未知。"
    elif vix >= 25:
        vix_view = "VIX 偏高，避险需求较强，对黄金偏利多。"
    elif vix <= 15:
        vix_view = "VIX 偏低，风险偏好较强，对黄金略偏压力。"
    else:
        vix_view = "VIX 处于中性区间，黄金主要由利率与美元主导。"

    # 4) 美联储预期（收益率曲线）
    if yc_bps is None:
        fed_view = "收益率曲线未知，难以判断美联储预期。"
    elif yc_bps < 0:
        fed_view = "收益率曲线倒挂，市场对经济前景偏忧，长期对黄金构成支撑。"
    elif yc_bps > 80:
        fed_view = "收益率曲线较陡，反映对经济复苏与加息预期，对黄金中性略偏空。"
    else:
        fed_view = "收益率曲线平坦，对黄金影响中性。"

    # 5) 地缘风险（目前为占位描述）
    geo_view = "当前未接入地缘政治事件监控，可在未来版本接入新闻事件与地缘风险指数。"

    # 6) 通胀预期（占位）
    inflation_view = "当前未接入通胀预期数据，可后续接入 breakeven / CPI 预期。"

    # 综合结论
    conclusion = "综合 6 因子，目前黄金中期环境整体中性，可作为组合防守与对冲工具。"
    if ten_y and ten_y < 2 and (vix and vix >= 20):
        conclusion = "利率偏低 + 波动偏高，黄金中期环境偏利多。"

    return GoldSixView(
        real_rate_view=real_rate_view,
        usd_view=usd_view,
        vix_view=vix_view,
        fed_view=fed_view,
        geo_view=geo_view,
        inflation_view=inflation_view,
        conclusion=conclusion,
    )
