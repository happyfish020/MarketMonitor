# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - Report Block: Attack Window

UI-only rendering (optional). Most integrations use ReportEngine auto-block append.

Contract (AW_V1):
- Show BOTH market_top20_trade_ratio and proxy_top20_amount_ratio to avoid "口径混淆".
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional


def _fmt_pct(x: Any) -> str:
    try:
        if x is None:
            return "None"
        v = float(x)
        # assume ratio 0-1
        if v <= 1.5:
            return f"{v*100:.2f}%"
        return f"{v:.2f}%"
    except Exception:
        return str(x)


def render_attack_window_block(slots: Dict[str, Any]) -> str:
    """Render Attack Window for report.

    Display-layer only: do NOT change decision logic. For readability:
    - Keep: 主否决因子 + 结论 (尤其 state=OFF)
    - Move: full reasons/evidence/freshness into Audit section
    """
    aw = slots.get("attack_window")
    if not isinstance(aw, dict):
        return "## 进攻窗口（Attack Window）\n\n- ⚠ missing:attack_window\n"

    meta = aw.get("meta", {}) if isinstance(aw.get("meta"), dict) else {}
    # asof: prefer AW top-level, then meta, then context.trade_date
    ctx = slots.get("context", {}) if isinstance(slots.get("context"), dict) else {}
    asof = (aw.get("asof") or meta.get("asof") or aw.get("trade_date") or meta.get("trade_date") or ctx.get("trade_date") or ctx.get("trade_date_str") or "unknown")
    state = str(aw.get("attack_state") or aw.get("state") or "OFF")
    gate = str(aw.get("gate_state") or aw.get("gate") or "UNKNOWN")
    perm = str(aw.get("offense_permission") or aw.get("constraint_summary") or "FORBID")

    yes: List[str] = aw.get("reasons_yes", []) if isinstance(aw.get("reasons_yes"), list) else []
    no: List[str] = aw.get("reasons_no", []) if isinstance(aw.get("reasons_no"), list) else []
    evidence: Dict[str, Any] = aw.get("evidence", {}) if isinstance(aw.get("evidence"), dict) else {}
    freshness: Dict[str, Any] = aw.get("data_freshness", {}) if isinstance(aw.get("data_freshness"), dict) else {}

    # --------- 主否决因子（面向用户）---------
    veto: List[str] = []
    trend_state = evidence.get("trend_state")
    fr_level = evidence.get("failure_rate_level")
    lev = evidence.get("leverage_level")

    if str(state).upper() == "OFF":
        if trend_state in ("broken", "BROAD_DAMAGE", "BROKEN"):
            veto.append(f"A 结构硬否决：trend_in_force={trend_state}")
        if str(fr_level).upper() in ("HIGH", "RED"):
            veto.append(f"B 失败率硬否决：failure_rate={fr_level}")
        if str(lev).upper() in ("HIGH", "RED"):
            veto.append(f"D 约束硬否决：leverage={lev}")

        participation_fail = any(str(x).startswith("C_participation_require_any_fail") for x in no)
        if participation_fail:
            adv = evidence.get("adv_ratio")
            pct = evidence.get("pct_above_ma20")
            amt = evidence.get("amount_ma20_ratio")
            parts = []
            if adv is not None:
                parts.append(f"adv_ratio={_fmt_pct(adv)}")
            if pct is not None:
                parts.append(f"%>MA20={_fmt_pct(pct)}")
            if amt is not None:
                parts.append(f"amount/MA20={amt:.3f}" if isinstance(amt, (int, float)) else f"amount/MA20={amt}")
            s = " · ".join(parts) if parts else "参与度指标未达标"
            veto.append(f"C 参与度不足：{s}")

        if not veto:
            compact = [x for x in no if "missing" not in str(x).lower()]
            veto.extend([f"否决：{str(x)}" for x in compact[:2]])

    # --------- Evidence summary line (always helpful) ---------
    ev_summary: List[str] = []
    if trend_state is not None:
        ev_summary.append(f"trend_state={trend_state}")
    if "market_top20_trade_ratio" in evidence:
        ev_summary.append(f"market_top20={_fmt_pct(evidence.get('market_top20_trade_ratio'))}")
    if "proxy_top20_amount_ratio" in evidence:
        ev_summary.append(f"proxy_top20={_fmt_pct(evidence.get('proxy_top20_amount_ratio'))}")
    if fr_level is not None:
        ev_summary.append(f"failure_rate={fr_level}")
    if "north_proxy_level" in evidence:
        ev_summary.append(f"north_proxy={evidence.get('north_proxy_level')}")
    if lev is not None:
        ev_summary.append(f"leverage={lev}")

    # --------- Render ---------
    lines: List[str] = []
    lines.append("## 进攻窗口（Attack Window）\n")
    lines.append(f"- As of: **{asof}**")
    lines.append(f"- State: **{state}** · offense_permission=**{perm}** · Gate=**{gate}**")

    state_u = state.upper()

    if state_u == "OFF":
        lines.append("- **进攻窗口关闭（硬否决）**")
        for v in veto:
            lines.append(f"  - {v}")
        lines.append("- 结论：在当前制度与结构下，**不允许任何进攻行为**；仅按 Gate/Execution 允许的防守或降风险动作执行。")
    else:
        # VERIFY_ONLY / LIGHT_ON / ON
        decision_reasons = evidence.get("decision_reasons") if isinstance(evidence.get("decision_reasons"), list) else []
        constraint_summary = evidence.get("constraint_summary")
        allowed = evidence.get("allowed_actions") if isinstance(evidence.get("allowed_actions"), list) else []
        forbidden = evidence.get("forbidden_actions") if isinstance(evidence.get("forbidden_actions"), list) else []
        rb = evidence.get("rollback_triggers_hit") if isinstance(evidence.get("rollback_triggers_hit"), list) else []

        if decision_reasons:
            lines.append("- Decision Reasons:")
            for r in decision_reasons:
                lines.append(f"  - {r}")

        if constraint_summary:
            lines.append(f"- Constraints: {constraint_summary}")

        if allowed:
            lines.append("- Allowed: " + ", ".join([str(x) for x in allowed]))
        if forbidden:
            lines.append("- Forbidden: " + ", ".join([str(x) for x in forbidden]))

        if rb:
            lines.append("- Rollback Triggers Hit: " + ", ".join([str(x) for x in rb]))

        # If there are still any veto summaries, show them as constraints (non-blocking here)
        if veto:
            lines.append("- 约束摘要（非直接否决）：")
            for v in veto:
                lines.append(f"  - {v}")

    if ev_summary:
        lines.append("- 关键证据： " + " · ".join([str(x) for x in ev_summary]))

    # --------- Audit (debug / replay) ---------
    lines.append("\n### Audit（审计信息）")
    if yes:
        lines.append("- ✅ reasons_yes: " + ", ".join([str(x) for x in yes]))
    # Drop noisy 'A_structure_missing:*' and 'B_failure_rate_missing:*' from main list; keep in audit
    if no:
        # Avoid strong negative emoji for non-OFF states (prevents misread)
        prefix = "- reasons_no: " if state_u != "OFF" else "- ❌ reasons_no: "
        lines.append(prefix + ", ".join([str(x) for x in no]))

    # Full evidence (explicitly separate two top20 semantics)
    ev_lines: List[str] = []
    if "adv_ratio" in evidence:
        ev_lines.append(f"adv_ratio={_fmt_pct(evidence.get('adv_ratio'))}")
    if "pct_above_ma20" in evidence:
        ev_lines.append(f"pct_above_ma20={_fmt_pct(evidence.get('pct_above_ma20'))}")
    if "new_low_ratio_pct" in evidence:
        ev_lines.append(f"new_low_ratio_pct={_fmt_pct(evidence.get('new_low_ratio_pct'))}")
    if "amount_ma20_ratio" in evidence:
        ev_lines.append(f"amount_ma20_ratio={evidence.get('amount_ma20_ratio')}")
    if "market_top20_trade_ratio" in evidence:
        ev_lines.append(f"market_top20_trade_ratio={_fmt_pct(evidence.get('market_top20_trade_ratio'))}")
    if "proxy_top20_amount_ratio" in evidence:
        ev_lines.append(f"proxy_top20_amount_ratio={_fmt_pct(evidence.get('proxy_top20_amount_ratio'))}")
    if "rule_top20_ratio" in evidence:
        ev_lines.append(f"rule_top20_ratio={_fmt_pct(evidence.get('rule_top20_ratio'))}")
    if "failure_rate_improve_days" in evidence:
        # Interpret None as 0 for audit readability
        d = evidence.get("failure_rate_improve_days")
        ev_lines.append(f"failure_rate_improve_days={0 if d is None else d}")
    if "north_proxy_score" in evidence:
        ev_lines.append(f"north_proxy_score={evidence.get('north_proxy_score')}")
    if "options_level" in evidence:
        ev_lines.append(f"options_level={evidence.get('options_level')}")

    if "audit_notes" in evidence and isinstance(evidence.get("audit_notes"), list):
        notes = [str(x) for x in evidence.get("audit_notes") if x is not None]
        if notes:
            ev_lines.append("audit_notes=" + " | ".join(notes[:8]))

    if ev_lines:
        lines.append("- evidence: " + " · ".join([str(x) for x in ev_lines]))

    # Freshness: prefer global Data Freshness; avoid misleading local asof_ok=False
    # Keep only notes if present.
    if freshness:
        notes = freshness.get("notes", [])
        if isinstance(notes, list) and notes:
            lines.append("- freshness_notes: " + ", ".join([str(x) for x in notes]))

    return "\n".join(lines).rstrip() + "\n"
