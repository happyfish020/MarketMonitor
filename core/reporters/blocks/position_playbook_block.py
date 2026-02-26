# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from typing import Any, Dict, List, Tuple


def _as_list(v: Any) -> List[str]:
    if isinstance(v, list):
        return [str(x) for x in v if str(x)]
    return []


def _to_float(v: Any) -> float | None:
    try:
        if v is None or isinstance(v, bool):
            return None
        return float(v)
    except Exception:
        return None


def _extract_trim_anchor(slots: Dict[str, Any]) -> Dict[str, Any]:
    """Best-effort MA anchor for quantified TRIM boundaries."""
    out: Dict[str, Any] = {}
    factors = slots.get("factors") if isinstance(slots, dict) else None
    if not isinstance(factors, dict):
        return out

    idx = factors.get("index_tech")
    if not isinstance(idx, dict):
        return out
    details = idx.get("details") if isinstance(idx.get("details"), dict) else {}

    raw = details.get("_raw_data")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            raw = None
    if not isinstance(raw, dict):
        return out

    for k in ("hs300", "sh", "zz500", "sz"):
        node = raw.get(k)
        if not isinstance(node, dict):
            continue
        c = _to_float(node.get("close"))
        m5 = _to_float(node.get("ma5"))
        m10 = _to_float(node.get("ma10"))
        if c is None or m5 is None or m10 is None or m5 == 0.0 or m10 == 0.0:
            continue
        out = {
            "index_key": k,
            "close": c,
            "ma5": m5,
            "ma10": m10,
            "dist_ma5_pct": (c / m5 - 1.0) * 100.0,
            "dist_ma10_pct": (c / m10 - 1.0) * 100.0,
        }
        break
    return out


def render_position_playbook(slots: Dict[str, Any]) -> Tuple[str, List[str], Dict[str, Any]]:
    aw = slots.get("attack_window") if isinstance(slots, dict) else None
    aw = aw if isinstance(aw, dict) else {}

    state = str(aw.get("state") or "OFF").upper()
    gate = str(aw.get("gate") or slots.get("governance", {}).get("gate", {}).get("final_gate") or "UNKNOWN").upper()

    execu = slots.get("execution_summary") if isinstance(slots, dict) else None
    execu = execu if isinstance(execu, dict) else {}
    band = str(execu.get("execution_band") or execu.get("band") or "NA").upper()

    allowed = _as_list(aw.get("allowed_actions"))
    forbidden = _as_list(aw.get("forbidden_actions"))

    if not allowed:
        if state == "VERIFY_ONLY":
            allowed = ["HOLD", "TRIM_ON_STRENGTH", "BASE_VERIFY_ADD(<=1lot, existing only)", "PULLBACK_VERIFY(<=1lot)"]
        elif state == "LIGHT_ON":
            allowed = ["HOLD", "TRIM_ON_STRENGTH", "ADD(<=1lot per symbol)", "PULLBACK_ADD(<=1lot)"]
        elif state == "ON":
            allowed = ["EXECUTE_PLAN_IN_BATCHES", "HOLD", "TRIM_ON_STRENGTH"]
        else:
            allowed = ["HOLD", "TRIM_ON_STRENGTH"]

    if not forbidden:
        if state in ("VERIFY_ONLY", "LIGHT_ON"):
            forbidden = ["CHASE_ADD", "SCALE_UP", "NEW_POSITION", "LEVERAGE", "OPTIONS"]
        elif state == "OFF":
            forbidden = ["ADD_RISK", "NEW_POSITION", "CHASE_ADD"]
        else:
            forbidden = []

    rollback = aw.get("rollback_triggers_hit")
    rollback_list = _as_list(aw.get("rollback_triggers"))

    title = "Position Playbook - Today"
    content: List[str] = []

    content.append(f"- State: **{state}** | Gate: **{gate}** | Execution: **{band}**")

    content.append("")
    content.append("**Today you CAN do**")
    for a in allowed:
        content.append(f"- {a}")

    content.append("")
    content.append("**Today you MUST NOT do**")
    if forbidden:
        for f in forbidden:
            content.append(f"- {f}")
    else:
        content.append("- (none)")

    content.append("")
    if state == "VERIFY_ONLY":
        content.append("**Risk Cap**: verification only - max **1 lot** on existing positions; no chasing.")
    elif state == "LIGHT_ON":
        content.append("**Risk Cap**: light offense - max **1 lot per symbol** today; portfolio new risk <= **20%** of planned exposure.")
    elif state == "OFF":
        content.append("**Risk Cap**: OFF - do not add new risk; defense / trim only.")

    trim_anchor = _extract_trim_anchor(slots)
    if gate in ("CAUTION", "D", "FREEZE") or state == "OFF":
        content.append("")
        content.append("**TRIM Triggers (Quantified)**")
        if trim_anchor:
            idx_key = trim_anchor.get("index_key")
            close = trim_anchor.get("close")
            ma5 = trim_anchor.get("ma5")
            ma10 = trim_anchor.get("ma10")
            d5 = trim_anchor.get("dist_ma5_pct")
            d10 = trim_anchor.get("dist_ma10_pct")
            content.append(
                f"- Anchor: {idx_key} close={close:.2f}, MA5={ma5:.2f}, MA10={ma10:.2f}, "
                f"dist_to_MA5={d5:+.2f}%, dist_to_MA10={d10:+.2f}%"
            )
            content.append("- Trigger-1: touch/near MA5 (>= -0.3%) -> allow TRIM up to 20%-33% of planned daily reduction.")
            content.append("- Trigger-2: touch/above MA10 (>= -0.3%) -> allow TRIM up to 33%-50% of planned daily reduction.")
            content.append("- Execution guard: batch orders, avoid chase, keep single-day gross turnover under internal limit.")
        else:
            content.append("- Anchor unavailable (missing MA5/MA10). Use conservative fallback: rebound-only small TRIM.")

    if rollback is True or (isinstance(rollback_list, list) and rollback_list):
        content.append("")
        content.append("**Rollback Alert**")
        if rollback_list:
            content.append("- Triggers hit: " + ", ".join(rollback_list))
        content.append("- Action: next day **revert** VERIFY/LIGHT adds (if any).")

    raw = {
        "state": state,
        "gate": gate,
        "execution_band": band,
        "allowed_actions": allowed,
        "forbidden_actions": forbidden,
        "rollback_triggers_hit": rollback,
        "rollback_triggers": rollback_list,
        "trim_trigger_anchor": trim_anchor,
    }
    return title, content, raw
