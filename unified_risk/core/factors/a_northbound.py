from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any
import os

from unified_risk.common.logging_utils import log_info


@dataclass
class NorthboundResult:
    score: float
    direction: str
    description: str
    raw: Dict[str, Any]


def _calc_strength(mode: str, etf_flow_yi: float, hs300_pct: float, a50_night_pct: float) -> float:
    """根据 ABCDE 模式组合 ETF 北向代理 / 沪深300 / A50 夜盘，输出等效“强度值”（亿元）。"""
    m = mode.upper()
    if m == "A":
        return etf_flow_yi
    if m == "B":
        return etf_flow_yi + hs300_pct * 10.0
    if m == "C":
        return etf_flow_yi + hs300_pct * 10.0 + a50_night_pct * 15.0
    if m == "D":
        return etf_flow_yi * 0.8 + hs300_pct * 15.0 + a50_night_pct * 20.0
    if m == "E":
        return etf_flow_yi * 0.5 + hs300_pct * 20.0 + a50_night_pct * 20.0
    return etf_flow_yi + hs300_pct * 10.0 + a50_night_pct * 15.0


def _strength_to_score(strength: float) -> float:
    s = max(-100.0, min(100.0, strength))
    score = 10.0 + s / 10.0
    return max(0.0, min(20.0, score))


def _score_to_direction(score: float) -> str:
    if score >= 16:
        return "北向极强流入 / 明显支撑"
    if score >= 13:
        return "北向偏强流入"
    if score >= 9:
        return "北向中性"
    if score >= 6:
        return "北向偏弱流出"
    return "北向明显流出 / 施压"


def compute_a_northbound(snapshot: Dict[str, Any]) -> NorthboundResult:
    mode = os.getenv("NPS_MODE", "C").upper()
    if mode not in {"A", "B", "C", "D", "E"}:
        mode = "C"

    nb = (snapshot.get("northbound_proxy") or {}) if snapshot else {}
    index = (snapshot.get("index") or {}) if snapshot else {}
    hs300 = index.get("hs300") or {}
    a50 = (snapshot.get("a50_night") or {}) if snapshot else {}

    etf_flow_yi = float(nb.get("proxy_etf_flow_yi") or 0.0)
    hs300_pct = float(hs300.get("changePct") or 0.0)
    a50_ret = a50.get("ret")
    a50_night_pct = float(a50_ret * 100.0) if isinstance(a50_ret, (int, float)) else 0.0

    strength = _calc_strength(mode, etf_flow_yi, hs300_pct, a50_night_pct)
    score = _strength_to_score(strength)
    direction = _score_to_direction(score)

    parts = [
        f"ETF 宽基代理净流入 {etf_flow_yi:.1f} 亿元",
        f"沪深300 日涨跌 {hs300_pct:.2f}%",
        f"A50 夜盘变动 {a50_night_pct:.2f}%",
    ]
    desc = f"模式 {mode}：" + "；".join(parts) + f"；综合北向强度 {strength:.1f}（得分 {score:.1f}/20，{direction}）。"

    log_info(
        f"[NPS] mode={mode}, score={score:.1f}, dir={direction}, "
        f"strength={strength:.1f}, etf={etf_flow_yi:.1f}, hs300={hs300_pct:.2f}, a50={a50_night_pct:.2f}"
    )

    raw = {
        "mode": mode,
        "strength": strength,
        "etf_flow_yi": etf_flow_yi,
        "hs300_pct": hs300_pct,
        "a50_night_pct": a50_night_pct,
    }

    return NorthboundResult(score=score, direction=direction, description=desc, raw=raw)
