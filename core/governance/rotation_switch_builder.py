# -*- coding: utf-8 -*-
"""UnifiedRisk V12 · Rotation Enable Switch (Frozen)

目标：
- 给出一个明确结论：今日是否适合启用“板块轮动策略”
  - mode: ON / OFF / PARTIAL
  - reasons: 结构化理由（可落库/可回放）

设计原则：
- Governance veto 优先（Gate/Execution/DRS）
- 缺失关键字段时，不“编造事实”，降级为 PARTIAL 或 OFF
- 输出仅用于解释与策略开关，不直接改写 Gate/Execution/DRS
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


def _get(d: Any, path: List[str]) -> Any:
    cur = d
    for k in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def _as_float(v: Any) -> Optional[float]:
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    try:
        if isinstance(v, str) and v.strip():
            return float(v.strip())
    except Exception:
        return None
    return None


def _ratio_to_pct(v: Any) -> Optional[float]:
    """Accept ratio in [0,1] or percent in [0,100] and return percent."""
    x = _as_float(v)
    if x is None:
        return None
    if 0.0 <= x <= 1.0:
        return x * 100.0
    return x


def _pick_gate(slots: Dict[str, Any]) -> Optional[str]:
    for p in (
        ["governance", "gate", "final_gate"],
        ["governance", "gate", "raw_gate"],
        ["gate"],
    ):
        v = _get(slots, p)
        if isinstance(v, str) and v.strip():
            return v.strip().upper()
    return None


def _pick_execution(slots: Dict[str, Any]) -> Optional[str]:
    for p in (
        ["governance", "execution", "band"],
        ["execution_summary", "band"],
        ["execution"],
    ):
        v = _get(slots, p)
        if isinstance(v, str) and v.strip():
            return v.strip().upper()
    return None


def _pick_drs(slots: Dict[str, Any]) -> Optional[str]:
    for p in (
        ["governance", "drs", "signal"],
        ["drs", "signal"],
        ["drs"],
    ):
        v = _get(slots, p)
        if isinstance(v, str) and v.strip():
            return v.strip().upper()
    return None


def _pick_adv_ratio_pct(slots: Dict[str, Any]) -> Optional[float]:
    # Prefer strict breadth snapshot if present
    v = _get(slots, ["market_overview", "breadth", "adv_ratio_pct"])
    if v is None:
        v = _get(slots, ["market_overview", "breadth", "adv_ratio"])
    if v is None:
        v = _get(slots, ["market_overview", "breadth", "adv_ratio"])
    if v is not None:
        return _ratio_to_pct(v)

    # Fallback: check factors wrapper
    factors = slots.get("factors") if isinstance(slots.get("factors"), dict) else {}
    for k in ("unified_emotion", "participation", "breadth"):
        fr = factors.get(k)
        det = fr.get("details") if isinstance(fr, dict) else None
        if isinstance(det, dict):
            for kk in ("adv_ratio_pct", "adv_ratio", "adv_ratio_percent"):
                if kk in det:
                    return _ratio_to_pct(det.get(kk))
    return None


def _pick_limit_down(slots: Dict[str, Any]) -> Optional[int]:
    for p in (
        ["market_overview", "breadth", "limit_down"],
        ["market_overview", "breadth", "down_limit"],
        ["market_overview", "breadth", "limitDown"],
    ):
        v = _get(slots, p)
        if isinstance(v, int):
            return v
        fv = _as_float(v)
        if fv is not None:
            return int(fv)

    factors = slots.get("factors") if isinstance(slots.get("factors"), dict) else {}
    for k in ("unified_emotion", "participation", "breadth"):
        fr = factors.get(k)
        det = fr.get("details") if isinstance(fr, dict) else None
        if isinstance(det, dict):
            for kk in ("limit_down", "down_limit", "limitDown"):
                if kk in det:
                    vv = det.get(kk)
                    if isinstance(vv, int):
                        return vv
                    fv = _as_float(vv)
                    if fv is not None:
                        return int(fv)
    return None


def _pick_broken_limit_rate_pct(slots: Dict[str, Any]) -> Optional[float]:
    # Some sources store as percent, some as ratio
    factors = slots.get("factors") if isinstance(slots.get("factors"), dict) else {}
    for k in ("unified_emotion", "participation"):
        fr = factors.get(k)
        det = fr.get("details") if isinstance(fr, dict) else None
        if isinstance(det, dict):
            for kk in (
                "broken_limit_rate_pct",
                "broken_limit_rate",
                "broken_limit_pct",
                "broken_rate",
            ):
                if kk in det:
                    return _ratio_to_pct(det.get(kk))
    return None


def _pick_top20_turnover_ratio_pct(slots: Dict[str, Any]) -> Optional[float]:
    # Strict semantics: liquidity_quality.details.top20_ratio (0~1)
    factors = slots.get("factors") if isinstance(slots.get("factors"), dict) else {}
    fr = factors.get("liquidity_quality")
    det = fr.get("details") if isinstance(fr, dict) else None
    if isinstance(det, dict):
        v = det.get("top20_ratio")
        if v is not None:
            return _ratio_to_pct(v)

    # Compatibility: market_overview.top20_ratio may exist (already strict in v12 blocks)
    v = _get(slots, ["market_overview", "top20_ratio"])
    if v is not None:
        return _ratio_to_pct(v)
    return None


def _pick_turnover_vs_ma20(slots: Dict[str, Any]) -> Optional[float]:
    # Optional: if not present, we won't hard-fail.
    factors = slots.get("factors") if isinstance(slots.get("factors"), dict) else {}
    fr = factors.get("liquidity_quality")
    det = fr.get("details") if isinstance(fr, dict) else None
    if isinstance(det, dict):
        for kk in ("turnover_ratio_vs_ma20", "amount_ratio_vs_ma20", "turnover_vs_ma20"):
            if kk in det:
                return _as_float(det.get(kk))
    return None


def build_rotation_switch(*, slots: Dict[str, Any], cfg: Dict[str, Any], trade_date: Optional[str] = None) -> Dict[str, Any]:
    """Compute rotation switch decision from slots + config.

    Returns dict to be stored in slots["rotation_switch"].
    """
    cfg = cfg or {}
    th = cfg.get("thresholds", {}) if isinstance(cfg.get("thresholds"), dict) else {}
    gv = cfg.get("governance_veto", {}) if isinstance(cfg.get("governance_veto"), dict) else {}
    feas = cfg.get("feasibility", {}) if isinstance(cfg.get("feasibility"), dict) else {}

    out: Dict[str, Any] = {
        "asof": trade_date,
        "mode": "OFF",
        "confidence": 0.0,
        "verdict": "不适合板块轮动",
        "reasons": [],
        "gating": {
            "gate": _pick_gate(slots),
            "execution": _pick_execution(slots),
            "drs": _pick_drs(slots),
        },
        "constraints": cfg.get("constraints", {}) if isinstance(cfg.get("constraints"), dict) else {},
        "data_status": {"coverage": "FULL", "missing": []},
        "version": cfg.get("version"),
    }

    # --- Governance veto ---
    # 设计：如果命中多条 veto，必须全部展示在 reasons 中（便于审计/解释）。
    gate = out["gating"].get("gate")
    execution = out["gating"].get("execution")
    drs = out["gating"].get("drs")

    veto_hit = False
    if gate in (s.upper() for s in gv.get("gate_disallow", []) if isinstance(s, str)):
        out["reasons"].append({"code": "ROT_GOV_VETO_GATE", "level": "FAIL", "msg": f"Gate={gate}"})
        veto_hit = True
    if execution in (s.upper() for s in gv.get("execution_disallow", []) if isinstance(s, str)):
        out["reasons"].append({"code": "ROT_GOV_VETO_EXEC", "level": "FAIL", "msg": f"Execution={execution}"})
        veto_hit = True
    if drs in (s.upper() for s in gv.get("drs_disallow", []) if isinstance(s, str)):
        out["reasons"].append({"code": "ROT_GOV_VETO_DRS", "level": "FAIL", "msg": f"DRS={drs}"})
        veto_hit = True

    # Execution=D3 等非 veto 也应解释为“高摩擦”，避免误解
    if isinstance(execution, str) and execution.startswith("D") and not veto_hit:
        out["reasons"].append({"code": "ROT_EXEC_HIGH_FRICTION", "level": "WARN", "msg": f"Execution={execution}（高摩擦，轮动胜率下降）"})

    if veto_hit:
        out["mode"] = "OFF"
        out["verdict"] = "不适合板块轮动（制度否决）"
        # 置信度语义：对“开关结论”的确定性，而非对涨跌的预测
        out["confidence"] = 0.95
        return out

    # --- Metrics ---
    adv_pct = _pick_adv_ratio_pct(slots)
    broken_pct = _pick_broken_limit_rate_pct(slots)
    limit_down = _pick_limit_down(slots)
    top20_pct = _pick_top20_turnover_ratio_pct(slots)
    turnover_vs_ma20 = _pick_turnover_vs_ma20(slots)

    missing = out["data_status"]["missing"]
    if adv_pct is None:
        missing.append("adv_ratio_pct")
    if broken_pct is None:
        missing.append("broken_limit_rate_pct")
    if limit_down is None:
        missing.append("limit_down")
    if top20_pct is None:
        missing.append("top20_turnover_ratio_pct")
    if missing:
        out["data_status"]["coverage"] = "PARTIAL"

    # --- Evaluate core conditions ---
    ok_flags: List[bool] = []

    # Breadth
    b_th = th.get("breadth", {}) if isinstance(th.get("breadth"), dict) else {}
    adv_on = _as_float(b_th.get("adv_ratio_pct_on"))
    if adv_pct is not None and adv_on is not None:
        if adv_pct >= adv_on:
            out["reasons"].append({"code": "ROT_BREADTH_OK", "level": "INFO", "msg": f"广度改善：adv_ratio_pct={adv_pct:.2f}% ≥ {adv_on:.2f}%"})
            ok_flags.append(True)
        else:
            out["reasons"].append({"code": "ROT_BREADTH_FAIL", "level": "FAIL", "msg": f"广度不足：adv_ratio_pct={adv_pct:.2f}% < {adv_on:.2f}%"})
            ok_flags.append(False)
    else:
        out["reasons"].append({"code": "ROT_BREADTH_MISSING", "level": "WARN", "msg": "广度字段缺失：adv_ratio_pct"})

    # Payoff / failure rate proxy
    p_th = th.get("payoff", {}) if isinstance(th.get("payoff"), dict) else {}
    broken_max = _as_float(p_th.get("broken_limit_rate_max_pct"))
    if broken_pct is not None and broken_max is not None:
        if broken_pct <= broken_max:
            out["reasons"].append({"code": "ROT_PAYOFF_OK", "level": "INFO", "msg": f"炸板率可控：broken_limit_rate={broken_pct:.2f}% ≤ {broken_max:.2f}%"})
            ok_flags.append(True)
        else:
            out["reasons"].append({"code": "ROT_PAYOFF_FAIL", "level": "FAIL", "msg": f"炸板率偏高：broken_limit_rate={broken_pct:.2f}% > {broken_max:.2f}%"})
            ok_flags.append(False)
    else:
        out["reasons"].append({"code": "ROT_PAYOFF_MISSING", "level": "WARN", "msg": "炸板率字段缺失：broken_limit_rate"})

    # Down limit guard (risk expansion)
    ld_max = p_th.get("limit_down_max")
    if isinstance(ld_max, (int, float)) and limit_down is not None:
        if limit_down <= int(ld_max):
            out["reasons"].append({"code": "ROT_LIMIT_DOWN_OK", "level": "INFO", "msg": f"跌停不拥挤：limit_down={limit_down} ≤ {int(ld_max)}"})
            ok_flags.append(True)
        else:
            out["reasons"].append({"code": "ROT_LIMIT_DOWN_FAIL", "level": "FAIL", "msg": f"跌停扩散：limit_down={limit_down} > {int(ld_max)}"})
            ok_flags.append(False)
    else:
        out["reasons"].append({"code": "ROT_LIMIT_DOWN_MISSING", "level": "WARN", "msg": "跌停字段缺失：limit_down"})

    # Concentration
    c_th = th.get("concentration", {}) if isinstance(th.get("concentration"), dict) else {}
    top20_max = _as_float(c_th.get("top20_turnover_ratio_max_pct"))
    if top20_pct is not None and top20_max is not None:
        if top20_pct <= top20_max:
            out["reasons"].append({"code": "ROT_CONC_OK", "level": "INFO", "msg": f"集中度可控：top20_ratio={top20_pct:.1f}% ≤ {top20_max:.1f}%"})
            ok_flags.append(True)
        else:
            out["reasons"].append({"code": "ROT_CONC_WARN", "level": "WARN", "msg": f"集中度偏高：top20_ratio={top20_pct:.1f}% > {top20_max:.1f}%"})
            # not hard-fail; treat as soft negative
            ok_flags.append(False)
    else:
        out["reasons"].append({"code": "ROT_CONC_MISSING", "level": "WARN", "msg": "集中度字段缺失：top20_turnover_ratio"})

    # Liquidity (optional)
    l_th = th.get("liquidity", {}) if isinstance(th.get("liquidity"), dict) else {}
    tv_min = _as_float(l_th.get("turnover_ratio_vs_ma20_min"))
    if turnover_vs_ma20 is not None and tv_min is not None:
        if turnover_vs_ma20 >= tv_min:
            out["reasons"].append({"code": "ROT_LIQ_OK", "level": "INFO", "msg": f"成交不萎缩：turnover_vs_ma20={turnover_vs_ma20:.2f} ≥ {tv_min:.2f}"})
        else:
            out["reasons"].append({"code": "ROT_LIQ_WARN", "level": "WARN", "msg": f"偏缩量：turnover_vs_ma20={turnover_vs_ma20:.2f} < {tv_min:.2f}"})
    else:
        out["reasons"].append({"code": "ROT_LIQ_MISSING", "level": "INFO", "msg": "成交相对MA20字段缺失（非硬条件）"})

    # --- Decide ON/OFF baseline ---
    # Require at least 2 hard INFO-OK among breadth/payoff/limit_down, and no FAIL in those.
    hard_fail = any(r.get("level") == "FAIL" for r in out["reasons"] if str(r.get("code", "")).startswith("ROT_") and "GOV" not in str(r.get("code", "")))
    hard_ok = sum(1 for r in out["reasons"] if r.get("level") == "INFO" and str(r.get("code", "")).endswith("_OK"))
    if not hard_fail and hard_ok >= 2:
        out["mode"] = "ON"
        out["verdict"] = "适合板块轮动"
        out["confidence"] = 0.70 if out["data_status"]["coverage"] == "FULL" else 0.55
    else:
        out["mode"] = "OFF"
        out["verdict"] = "不适合板块轮动"
        out["confidence"] = 0.35 if out["data_status"]["coverage"] == "FULL" else 0.25

    # --- Fast rotation downgrade ---
    fast_if = feas.get("fast_rotation_if", {}) if isinstance(feas.get("fast_rotation_if"), dict) else {}
    br_min = _as_float(fast_if.get("broken_limit_rate_min_pct"))
    t20_min = _as_float(fast_if.get("top20_turnover_ratio_min_pct"))
    adv_max = _as_float(fast_if.get("adv_ratio_pct_max"))
    is_fast = False
    if broken_pct is not None and top20_pct is not None and br_min is not None and t20_min is not None:
        if broken_pct >= br_min and top20_pct >= t20_min:
            # optional: adv_pct not too strong (meaning: narrow & unstable)
            if adv_max is None or (adv_pct is not None and adv_pct <= adv_max):
                is_fast = True

    if is_fast and out["mode"] == "ON":
        out["mode"] = str(feas.get("mode_when_fast") or "PARTIAL").upper()
        out["verdict"] = "仅限低频 / 只做确认段（轮动过快）"
        out["confidence"] = min(out["confidence"], 0.60)
        out["reasons"].append({
            "code": "ROT_TOO_FAST_PARTIAL",
            "level": "WARN",
            "msg": f"轮动过快：broken={broken_pct:.2f}% top20={top20_pct:.1f}%（降级为 {out['mode']}）",
        })
    elif is_fast and out["mode"] == "OFF":
        out["reasons"].append({
            "code": "ROT_TOO_FAST",
            "level": "WARN",
            "msg": f"轮动过快：broken={broken_pct:.2f}% top20={top20_pct:.1f}%（不建议启用轮动）",
        })

    # If data is very partial, avoid a strong ON
    if out["mode"] == "ON" and out["data_status"]["coverage"] == "PARTIAL":
        out["mode"] = "PARTIAL"
        out["verdict"] = "数据不全：仅限低频 / 只做确认段"
        out["confidence"] = min(out["confidence"], 0.55)

    return out
