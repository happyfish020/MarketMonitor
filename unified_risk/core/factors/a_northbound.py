from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any
import os
from pathlib import Path
import pandas as pd

from unified_risk.common.logging_utils import log_info


@dataclass
class NorthboundResult:
    score: float
    direction: str
    description: str
    raw: Dict[str, Any]


# -------------------------------
#   基础强度计算（你原有逻辑）
# -------------------------------

def _calc_strength(mode: str, etf_flow_yi: float, hs300_pct: float, a50_night_pct: float) -> float:
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


# -------------------------------
#   新增：NPS 趋势 (T-3/T-2/T-1)
# -------------------------------

NPS_FILE = Path("data/ashare/nps_history.csv")


def _save_nps_history(date_str: str, strength: float):
    """保存每日 NPS 强度到 CSV"""
    NPS_FILE.parent.mkdir(parents=True, exist_ok=True)
    write_header = not NPS_FILE.exists()
    with NPS_FILE.open("a", encoding="utf-8") as f:
        if write_header:
            f.write("date,nps\n")
        f.write(f"{date_str},{strength}\n")


def _compute_nps_trend() -> float:
    """计算 MA3 - MA1"""
    if not NPS_FILE.exists():
        return 0.0

    try:
        df = pd.read_csv(NPS_FILE)
        if len(df) < 3:
            return 0.0
        ma3 = df["nps"].tail(3).mean()
        ma1 = df["nps"].iloc[-1]
        return ma3 - ma1
    except Exception:
        return 0.0


# -------------------------------
#   主函数：构建 NorthboundResult
# -------------------------------

def compute_a_northbound(snapshot: Dict[str, Any]) -> NorthboundResult:
    # === 1）基础数据 ===
    mode = os.getenv("NPS_MODE", "C").upper()
    if mode not in {"A", "B", "C", "D", "E"}:
        mode = "C"

    nb = (snapshot.get("northbound_proxy") or {})
    index = (snapshot.get("index") or {})
    hs300 = index.get("hs300") or {}
    a50 = (snapshot.get("a50_night") or {})

    etf_flow_yi = float(nb.get("proxy_etf_flow_yi") or 0.0)
    hs300_pct = float(hs300.get("changePct") or 0.0)

    a50_ret = a50.get("ret")
    a50_night_pct = float(a50_ret * 100.0) if isinstance(a50_ret, (int, float)) else 0.0

    # === 2）基础强度 ===
    strength_base = _calc_strength(mode, etf_flow_yi, hs300_pct, a50_night_pct)

    # === 3）保存 NPS 到历史 ===
    trade_date = snapshot.get("trade_date") or ""
    if trade_date:
        _save_nps_history(str(trade_date), strength_base)

    # === 4）趋势 MA3-MA1 ===
    trend = _compute_nps_trend()

    # 你原本的 score 是按 strength 基础逻辑来的
    score_base = _strength_to_score(strength_base)

    # === 5）加入趋势增强（温和修正） ===
    # 趋势权重可调：0.3
    score = score_base + trend * 0.3
    score = max(0.0, min(20.0, score))

    direction = _score_to_direction(score)

    desc = (
        f"模式 {mode}：ETF 宽基代理净流入 {etf_flow_yi:.1f} 亿元；"
        f"沪深300 日涨跌 {hs300_pct:.2f}%；"
        f"A50 夜盘变动 {a50_night_pct:.2f}%；"
        f"趋势 MA3-MA1={trend:.2f}；"
        f"综合北向强度 {strength_base:.1f}（得分 {score:.1f}/20，{direction}）。"
    )

    log_info(
        f"[NPS] mode={mode}, score={score:.1f}, trend={trend:.2f}, "
        f"strength={strength_base:.1f}, etf={etf_flow_yi:.1f}, "
        f"hs300={hs300_pct:.2f}, a50={a50_night_pct:.2f}"
    )

    raw = {
        "mode": mode,
        "strength_base": strength_base,
        "trend": trend,
        "strength_final": strength_base + trend,
        "etf_flow_yi": etf_flow_yi,
        "hs300_pct": hs300_pct,
        "a50_night_pct": a50_night_pct,
    }

    return NorthboundResult(
        score=score, direction=direction, description=desc, raw=raw
    )
