# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List, Tuple


def _as_list(v: Any) -> List[str]:
    if isinstance(v, list):
        return [str(x) for x in v if str(x)]
    return []


def render_position_playbook(slots: Dict[str, Any]) -> Tuple[str, List[str], Dict[str, Any]]:
    aw = slots.get("attack_window") if isinstance(slots, dict) else None
    aw = aw if isinstance(aw, dict) else {}

    state = str(aw.get("state") or "OFF").upper()
    gate = str(aw.get("gate") or slots.get("governance", {}).get("gate", {}).get("final_gate") or "UNKNOWN").upper()

    execu = slots.get("execution_summary") if isinstance(slots, dict) else None
    execu = execu if isinstance(execu, dict) else {}
    band = str(execu.get("execution_band") or "NA").upper()

    allowed = _as_list(aw.get("allowed_actions"))
    forbidden = _as_list(aw.get("forbidden_actions"))

    # Fallback defaults (read-only) when upstream does not provide explicit lists.
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

    title = "仓位行动指引（Position Playbook · Today）"
    content: List[str] = []

    content.append(f"- State: **{state}** | Gate: **{gate}** | Execution: **{band}**")

    content.append("")
    content.append("**✅ Today you CAN do**")
    for a in allowed:
        content.append(f"- {a}")

    content.append("")
    content.append("**⛔ Today you MUST NOT do**")
    if forbidden:
        for f in forbidden:
            content.append(f"- {f}")
    else:
        content.append("- (none)")

    # Risk caps (best-effort, read-only guidance)
    content.append("")
    if state == "VERIFY_ONLY":
        content.append("**Risk Cap**: verification only — max **1 lot** on existing positions; no chasing.")
    elif state == "LIGHT_ON":
        content.append("**Risk Cap**: light offense — max **1 lot per symbol** today; portfolio new risk ≤ **20%** of planned exposure.")
    elif state == "OFF":
        content.append("**Risk Cap**: OFF — do not add new risk; defense / trim only.")

    # Rollback alert
    if rollback is True or (isinstance(rollback_list, list) and rollback_list):
        content.append("")
        content.append("**⚠ Rollback Alert**")
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
    }
    return title, content, raw
