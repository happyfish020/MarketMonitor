# global_risk/scoring/commodity_scoring.py
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
class CommodityView:
    gold_view: str
    gold_comment: str
    oil_view: str
    oil_comment: str
    copper_view: str
    copper_comment: str
    dxy_comment: str
    overall_comment: str


def build_commodity_view(raw_macro: Dict[str, Any]) -> CommodityView:
    """
    仅用于文本描述：
      - gold_pct / gold_change
      - oil_pct  / oil_change
      - copper_pct / copper_change
      - dxy_pct / dxy_change
    """
    g_chg = _safe_float(
        raw_macro.get("gold_pct")
        or raw_macro.get("gold_change")
    )
    o_chg = _safe_float(
        raw_macro.get("oil_pct")
        or raw_macro.get("oil_change")
    )
    c_chg = _safe_float(
        raw_macro.get("copper_pct")
        or raw_macro.get("copper_change")
    )
    dxy = _safe_float(
        raw_macro.get("dxy_change")
        or raw_macro.get("dxy_pct")
    )

    # 黄金
    if g_chg is None:
        gold_view = "未知"
        gold_comment = "未接入黄金变动数据。"
    elif g_chg > 1:
        gold_view = "大涨"
        gold_comment = "避险情绪明显升温，或对成长股估值有压制。"
    elif g_chg < -1:
        gold_view = "大跌"
        gold_comment = "避险需求回落，风险偏好阶段性抬升。"
    else:
        gold_view = "震荡"
        gold_comment = "黄金小幅波动，对风险偏好影响有限。"

    # 原油
    if o_chg is None:
        oil_view = "未知"
        oil_comment = "未接入原油变动数据。"
    elif o_chg > 2:
        oil_view = "大涨"
        oil_comment = "原油大幅走强，可能推升通胀与成本压力。"
    elif o_chg < -2:
        oil_view = "大跌"
        oil_comment = "原油大幅走弱，或反映需求担忧。"
    else:
        oil_view = "震荡"
        oil_comment = "油价震荡，对大盘仅有边际影响。"

    # 铜
    if c_chg is None:
        copper_view = "未知"
        copper_comment = "未接入铜价变动数据。"
    elif c_chg > 1.5:
        copper_view = "走强"
        copper_comment = "铜价走强，制造业与周期预期偏乐观。"
    elif c_chg < -1.5:
        copper_view = "走弱"
        copper_comment = "铜价走弱，经济预期偏谨慎。"
    else:
        copper_view = "震荡"
        copper_comment = "铜价震荡。"

    # 美元指数
    if dxy is None:
        dxy_comment = "美元指数变动未知。"
    elif dxy > 0.5:
        dxy_comment = "美元明显走强，对风险资产略有压制。"
    elif dxy < -0.5:
        dxy_comment = "美元回落，对新兴市场与大宗品略有支撑。"
    else:
        dxy_comment = "美元指数波动有限。"

    # 综合结论
    overall_comment = "综合大宗商品与美元，当前对 A 股整体影响中性偏温和。"
    if g_chg and g_chg > 1 and dxy and dxy > 0.5:
        overall_comment = "黄金与美元同步走强，避险情绪抬升，对权益市场略偏压力。"

    return CommodityView(
        gold_view=gold_view,
        gold_comment=gold_comment,
        oil_view=oil_view,
        oil_comment=oil_comment,
        copper_view=copper_view,
        copper_comment=copper_comment,
        dxy_comment=dxy_comment,
        overall_comment=overall_comment,
    )
