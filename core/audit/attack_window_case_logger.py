# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
from collections import Counter, defaultdict
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

DEFAULT_REL_PATH = os.path.join("data", "audit", "attack_window_cases.jsonl")


def _ensure_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def _guess_root_dir() -> str:
    # Best-effort project root: try CWD, then this file's parent chain.
    try:
        return os.getcwd()
    except Exception:
        return "."


def _resolve_log_path() -> str:
    root = _guess_root_dir()
    path = os.path.join(root, DEFAULT_REL_PATH)
    _ensure_dir(path)
    return path


def _to_date_str(td: Any) -> str:
    if isinstance(td, str):
        return td
    if isinstance(td, (datetime, date)):
        return td.strftime("%Y-%m-%d")
    return str(td)


def _safe_get(d: Any, path: List[str]) -> Any:
    cur = d
    for k in path:
        if not isinstance(cur, dict):
            return None
        if k not in cur:
            return None
        cur = cur[k]
    return cur


def _extract_governance(des_payload: Optional[Dict[str, Any]], slots: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Returns: (gate_state, drs_level, frf_level)
    Prefer des_payload["governance"] because it is already normalized for reporting.
    """
    gate = drs = frf = None

    if isinstance(des_payload, dict):
        gov = des_payload.get("governance")
        if isinstance(gov, dict):
            gate = gov.get("gate") if isinstance(gov.get("gate"), str) else gate
            drs = gov.get("drs") if isinstance(gov.get("drs"), str) else drs
            frf = gov.get("frf") if isinstance(gov.get("frf"), str) else frf

    # Fallbacks from slots (best-effort, tolerate schema drift)
    if gate is None:
        gate = _safe_get(slots, ["gate", "state"]) or _safe_get(slots, ["governance", "gate"]) or _safe_get(slots, ["governance", "gate_state"])
    if drs is None:
        drs = _safe_get(slots, ["drs", "level"]) or _safe_get(slots, ["daily_risk_signal", "level"]) or _safe_get(slots, ["governance", "drs"])
    if frf is None:
        frf = _safe_get(slots, ["frf", "level"]) or _safe_get(slots, ["governance", "frf"])

    # Normalize to upper strings if possible
    def _norm(x: Any) -> Optional[str]:
        if x is None:
            return None
        if isinstance(x, str):
            return x.upper()
        return str(x).upper()

    return _norm(gate), _norm(drs), _norm(frf)


def _extract_attack_window(slots: Dict[str, Any], des_payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    aw = None
    if isinstance(des_payload, dict):
        aw = des_payload.get("attack_window")
    if not isinstance(aw, dict):
        aw = slots.get("attack_window")
    return aw if isinstance(aw, dict) else {}


def _first_not_none(*vals: Any) -> Any:
    for v in vals:
        if v is not None:
            return v
    return None


def _extract_execution_band(des_payload: Optional[Dict[str, Any]], slots: Dict[str, Any], aw: Dict[str, Any]) -> Optional[str]:
    """Best-effort extraction of execution band (A/N/D1/D2/D3)."""
    band = None
    if isinstance(des_payload, dict):
        # common layouts
        band = _safe_get(des_payload, ["execution", "band"]) or des_payload.get("execution_band")
        if band is None:
            gov = des_payload.get("governance")
            if isinstance(gov, dict):
                band = gov.get("execution_band") or gov.get("execution")
    band = _first_not_none(
        band,
        aw.get("execution_band"),
        aw.get("execution"),
        # common alternative locations
        _safe_get(slots, ["execution_summary", "band"]),
        _safe_get(slots, ["execution", "execution_band"]),
        _safe_get(slots, ["execution", "band"]),
        _safe_get(slots, ["governance", "execution_band"]),
    )
    if band is None:
        return None
    return str(band).upper()


def _extract_metrics_from_slots(slots: Dict[str, Any]) -> Dict[str, Any]:
    """Fallback metrics when not available in attack_window payload."""
    bp = slots.get("breadth_plus") if isinstance(slots.get("breadth_plus"), dict) else {}
    part = slots.get("participation") if isinstance(slots.get("participation"), dict) else {}
    # tolerate alternative naming
    return {
        "new_low_ratio_pct": _first_not_none(
            bp.get("new_low_ratio_pct"),
            bp.get("new_lows_ratio_pct"),
            bp.get("new_low_pct"),
            _safe_get(bp, ["new_lows", "ratio_pct"]),
        ),
        "amount_ratio": _first_not_none(
            part.get("amount_ratio"),
            part.get("turnover_ratio"),
            part.get("amt_ratio"),
            _safe_get(part, ["amount", "ratio"]),
        ),
        "adv_ratio": _first_not_none(bp.get("adv_ratio"), part.get("adv_ratio")),
        "pct_above_ma20": _first_not_none(bp.get("pct_above_ma20"), bp.get("pct_above_ma20_pct"), part.get("pct_above_ma20")),
    }


def _default_constraints_by_state(state: Optional[str]) -> Tuple[Optional[str], Optional[List[str]], Optional[List[str]]]:
    """Provide default constraint_summary and actions for better debuggability."""
    if not state:
        return None, None, None
    s = state.upper()
    if s == "OFF":
        return (
            "OFF: no new risk; hold/trim allowed",
            ["HOLD", "TRIM_ON_STRENGTH"],
            ["ADD_RISK", "CHASE_ADD", "SCALE_UP", "NEW_POSITION", "ROTATION_ATTACK", "LEVERAGE", "OPTIONS"],
        )
    if s == "VERIFY_ONLY":
        return (
            "VERIFY_ONLY: one-lot probe on existing positions; no chase; ready to revert",
            ["HOLD", "TRIM_ON_STRENGTH", "BASE_VERIFY_ADD", "PULLBACK_VERIFY"],
            ["CHASE_ADD", "SCALE_UP", "NEW_POSITION", "ROTATION_ATTACK", "LEVERAGE", "OPTIONS"],
        )
    if s == "LIGHT_ON":
        return (
            "LIGHT_ON: gradual add (≤1 lot per symbol today); no chase; respect risk cap & rollback",
            ["HOLD", "TRIM_ON_STRENGTH", "PULLBACK_ADD", "BASE_ETF_ADD"],
            ["CHASE_ADD", "SCALE_UP", "LEVERAGE", "OPTIONS"],
        )
    if s == "ON":
        return (
            "ON: execute planned entries in batches; still respect theme caps & stops",
            ["HOLD", "ADD_RISK", "PULLBACK_ADD"],
            ["LEVERAGE", "OPTIONS"],
        )
    return None, None, None


def _append_jsonl(path: str, obj: Dict[str, Any]) -> None:
    _ensure_dir(path)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")
def _weekly_dir(log_path: str) -> str:
    base = os.path.dirname(log_path) or "."
    d = os.path.join(base, "weekly")
    os.makedirs(d, exist_ok=True)
    return d


def _iso_week_key(td_str: str) -> str:
    try:
        d = datetime.strptime(td_str, "%Y-%m-%d").date()
        iso = d.isocalendar()
        return f"{iso.year}-W{iso.week:02d}"
    except Exception:
        return "UNKNOWN"


def _read_jsonl_records(path: str) -> List[Dict[str, Any]]:
    recs: List[Dict[str, Any]] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    recs.append(json.loads(line))
                except Exception:
                    # tolerate partial/corrupted line
                    continue
    except Exception:
        return []
    return recs


def _select_week_days(day_recs: List[Dict[str, Any]], week_key: str) -> List[Dict[str, Any]]:
    # dedupe by trade_date, keep the newest by ts
    by_td: Dict[str, Dict[str, Any]] = {}
    for r in day_recs:
        td = r.get("trade_date")
        if not isinstance(td, str):
            continue
        if _iso_week_key(td) != week_key:
            continue
        prev = by_td.get(td)
        if prev is None:
            by_td[td] = r
            continue
        # compare ts lexicographically (ISO strings comparable) if present
        ts_new = str(r.get("ts") or "")
        ts_old = str(prev.get("ts") or "")
        if ts_new >= ts_old:
            by_td[td] = r
    # sort by trade_date
    return [by_td[k] for k in sorted(by_td.keys())]


def _build_weekly_summary(week_key: str, days: List[Dict[str, Any]]) -> Dict[str, Any]:
    state_c = Counter()
    gate_c = Counter()
    drs_c = Counter()
    exec_c = Counter()
    trend_c = Counter()
    missing_c = Counter()
    reason_bundle_c = Counter()

    for d in days:
        state_c[str(d.get("attack_state") or "N/A")] += 1
        gate_c[str(d.get("gate_state") or "N/A")] += 1
        drs_c[str(d.get("drs_level") or "N/A")] += 1
        exec_c[str(d.get("execution_band") or "N/A")] += 1
        trend_c[str(d.get("trend_state") or "N/A")] += 1

        audit = d.get("audit_notes") or {}
        if isinstance(audit, dict):
            mf = audit.get("missing_fields") or []
            if isinstance(mf, list):
                for x in mf:
                    if isinstance(x, str) and x:
                        missing_c[x] += 1

        reasons = d.get("decision_reasons") or []
        if isinstance(reasons, list):
            bundle = " | ".join([x for x in reasons if isinstance(x, str)])
            if bundle:
                reason_bundle_c[bundle] += 1

    # lightweight day view
    day_view = []
    for d in days:
        audit = d.get("audit_notes") or {}
        mf = audit.get("missing_fields") if isinstance(audit, dict) else None
        day_view.append({
            "trade_date": d.get("trade_date"),
            "attack_state": d.get("attack_state"),
            "gate_state": d.get("gate_state"),
            "drs_level": d.get("drs_level"),
            "execution_band": d.get("execution_band"),
            "trend_state": d.get("trend_state"),
            "adv_ratio": d.get("adv_ratio"),
            "pct_above_ma20": d.get("pct_above_ma20"),
            "new_low_ratio_pct": d.get("new_low_ratio_pct"),
            "amount_ratio": d.get("amount_ratio"),
            "proxy_top20_amount_ratio": d.get("proxy_top20_amount_ratio"),
            "failure_rate_level": d.get("failure_rate_level"),
            "missing_fields": mf,
        })

    return {
        "week_key": week_key,
        "n_days": len(days),
        "days": day_view,
        "counts": {
            "attack_state": dict(state_c),
            "gate_state": dict(gate_c),
            "drs_level": dict(drs_c),
            "execution_band": dict(exec_c),
            "trend_state": dict(trend_c),
        },
        "data_quality": {
            "missing_fields_counts": dict(missing_c),
        },
        "top_reason_bundles": [
            {"bundle": k, "count": v} for k, v in reason_bundle_c.most_common(10)
        ],
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }


def _render_weekly_md(summary: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append(f"# Attack Window Weekly Summary — {summary.get('week_key')}")
    lines.append("")
    lines.append(f"- Days in summary: {summary.get('n_days')}")
    lines.append("")
    counts = summary.get("counts") or {}
    def _fmt_counts(title: str, m: Dict[str, Any]) -> None:
        lines.append(f"## {title}")
        for k, v in sorted(m.items(), key=lambda x: (-x[1], x[0])):
            lines.append(f"- {k}: {v}")
        lines.append("")
    _fmt_counts("Counts — Attack State", counts.get("attack_state") or {})
    _fmt_counts("Counts — Gate", counts.get("gate_state") or {})
    _fmt_counts("Counts — DRS", counts.get("drs_level") or {})
    _fmt_counts("Counts — Execution", counts.get("execution_band") or {})
    _fmt_counts("Counts — Trend", counts.get("trend_state") or {})

    dq = (summary.get("data_quality") or {}).get("missing_fields_counts") or {}
    lines.append("## Data Quality — Missing Fields")
    if dq:
        for k, v in sorted(dq.items(), key=lambda x: (-x[1], x[0])):
            lines.append(f"- {k}: {v}")
    else:
        lines.append("- (none)")
    lines.append("")

    lines.append("## Days")
    lines.append("| trade_date | state | gate | drs | exec | trend | adv_ratio | proxy_top20_amount_ratio | missing_fields |")
    lines.append("|---|---|---|---|---|---:|---:|---:|---|")
    for d in summary.get("days") or []:
        mf = d.get("missing_fields")
        mf_s = ",".join(mf) if isinstance(mf, list) else ""
        lines.append(
            f"| {d.get('trade_date')} | {d.get('attack_state')} | {d.get('gate_state')} | {d.get('drs_level')} | {d.get('execution_band')} | {d.get('trend_state')} | {d.get('adv_ratio')} | {d.get('proxy_top20_amount_ratio')} | {mf_s} |"
        )
    lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _update_weekly_outputs(log_path: str, td_str: str) -> None:
    week_key = _iso_week_key(td_str)
    if week_key == "UNKNOWN":
        return

    recs = _read_jsonl_records(log_path)
    day_recs = [r for r in recs if r.get("event") == "ATTACK_WINDOW_DAY"]
    days = _select_week_days(day_recs, week_key)
    summary = _build_weekly_summary(week_key, days)

    weekly_dir = _weekly_dir(log_path)
    json_path = os.path.join(weekly_dir, f"attack_window_weekly_{week_key}.json")
    md_path = os.path.join(weekly_dir, f"attack_window_weekly_{week_key}.md")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    with open(md_path, "w", encoding="utf-8") as f:
        f.write(_render_weekly_md(summary))



def maybe_log_attack_window_case(
    trade_date: Any,
    slots: Dict[str, Any],
    report_kind: str = "EOD",
    des_payload: Optional[Dict[str, Any]] = None,
    logger: Any = None,
    record_states: Optional[Iterable[str]] = None,
) -> None:
    """
    Always writes a DAY snapshot record so user can confirm activation.
    Writes an additional CASE snapshot when attack_window.state in record_states.
    Fail-safe: should never raise to caller.
    """
    if record_states is None:
        record_states = ("VERIFY_ONLY", "LIGHT_ON", "ON")

    td = _to_date_str(trade_date)
    path = _resolve_log_path()

    # 0) Active marker (written once per run / per day)
    _append_jsonl(path, {
        "event": "CASE_LOG_ACTIVE",
        "trade_date": td,
        "report_kind": report_kind,
        "path": path,
        "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    })

    aw = _extract_attack_window(slots, des_payload)
    state = aw.get("state") or aw.get("attack_state") or aw.get("status")
    if isinstance(state, str):
        state = state.upper().replace("-", "_")

    gate, drs, frf = _extract_governance(des_payload, slots)

    # Common evidence (best-effort)
    trend_state = (
        aw.get("trend_state")
        or aw.get("trend")
        # prefer already-normalized reporting payloads if present
        or (_safe_get(des_payload, ["trend_in_force", "state"]) if isinstance(des_payload, dict) else None)
        or (_safe_get(des_payload, ["trend_in_force", "trend_state"]) if isinstance(des_payload, dict) else None)
        or (_safe_get(des_payload, ["trend_in_force", "status"]) if isinstance(des_payload, dict) else None)
        or (_safe_get(des_payload, ["trend_state"]) if isinstance(des_payload, dict) else None)
        # fallbacks from slots
        or _safe_get(slots, ["trend_in_force", "state"])
        or _safe_get(slots, ["trend_in_force", "trend_state"])
        or _safe_get(slots, ["trend_in_force", "status"])
        or _safe_get(slots, ["trend_in_force", "result"])
    )
    execution = _extract_execution_band(des_payload, slots, aw)

    # evidence numbers may live under aw["evidence"] or aw["details"]
    evidence = aw.get("evidence") if isinstance(aw.get("evidence"), dict) else {}
    details = aw.get("details") if isinstance(aw.get("details"), dict) else {}
    def _pick_num(keys: List[str], slot_paths: Optional[List[List[str]]] = None) -> Any:
        for k in keys:
            if k in evidence:
                return evidence.get(k)
            if k in details:
                return details.get(k)
            v = aw.get(k)
            if v is not None:
                return v
        if slot_paths:
            for p in slot_paths:
                v = _safe_get(slots, p)
                if v is not None:
                    return v
        return None

    # Numbers: prefer AW/evidence, then fall back to known slot locations
    adv_ratio = _pick_num(
        ["adv_ratio", "adv_ratio_pct"],
        slot_paths=[
            ["participation", "adv_ratio"],
            ["participation", "adv_ratio_pct"],
            ["market_internals", "adv_ratio"],
        ],
    )
    pct_above_ma20 = _pick_num(
        ["pct_above_ma20", "pct_above_ma20_pct"],
        slot_paths=[
            ["breadth_plus", "pct_above_ma20"],
            ["breadth_plus", "pct_above_ma20_pct"],
        ],
    )
    new_low_ratio_pct = _first_not_none(
        _safe_get(des_payload, ["breadth_plus", "new_low_ratio_pct"]) if isinstance(des_payload, dict) else None,
        _safe_get(des_payload, ["breadth_plus", "new_lows_ratio_pct"]) if isinstance(des_payload, dict) else None,
        _safe_get(des_payload, ["breadth_plus", "new_low_ratio"]) if isinstance(des_payload, dict) else None,
        _pick_num(
            ["new_low_ratio_pct", "new_low_ratio", "new_lows_ratio_pct"],
            slot_paths=[
                ["breadth_plus", "new_low_ratio_pct"],
                ["breadth_plus", "new_low_ratio"],
                ["breadth_plus", "new_lows_ratio_pct"],
            ],
        ),
    )
    amount_ratio = _first_not_none(
        _safe_get(des_payload, ["amount", "amount_ratio"]) if isinstance(des_payload, dict) else None,
        _safe_get(des_payload, ["participation", "amount_ratio"]) if isinstance(des_payload, dict) else None,
        _safe_get(des_payload, ["participation", "amount_ratio_vs_ma20"]) if isinstance(des_payload, dict) else None,
        _pick_num(
            ["amount_ratio", "turnover_ratio"],
            slot_paths=[
                ["participation", "amount_ratio"],
                ["participation", "turnover_ratio"],
                ["participation", "amount_ratio_vs_ma20"],
            ],
        ),
    )
    proxy_top20_amount_ratio = _pick_num(
        ["proxy_top20_amount_ratio", "top20_amount_ratio"],
        slot_paths=[
            ["attack_window", "proxy_top20_amount_ratio"],
            ["attack_window", "top20_amount_ratio"],
            ["liquidity_quality", "top20_amount_ratio"],
        ],
    )

    allowed_actions = aw.get("allowed_actions")
    forbidden_actions = aw.get("forbidden_actions")
    constraint_summary = aw.get("constraint_summary")
    # Provide defaults so the daily log remains actionable even when AW doesn't populate lists.
    def_cs, def_allowed, def_forbidden = _default_constraints_by_state(state if isinstance(state, str) else None)
    if constraint_summary is None:
        constraint_summary = def_cs
    if allowed_actions is None:
        allowed_actions = def_allowed
    if forbidden_actions is None:
        forbidden_actions = def_forbidden

    raw_reasons = aw.get("decision_reasons") or aw.get("reasons_yes")
    # Always keep a minimal, stable decision context (Decision layer),
    # and push noisy D_* details into audit_notes.
    decision_reasons = [
        f"Gate:{gate or 'N/A'}",
        f"DRS:{drs or 'N/A'}",
        f"Trend:{(str(trend_state).upper() if trend_state is not None else 'N/A')}",
    ]

    day_rec = {
        "event": "ATTACK_WINDOW_DAY",
        "trade_date": td,
        "report_kind": report_kind,
        "attack_state": state,
        "prev_attack_state": aw.get("prev_state") or aw.get("prev_attack_state"),
        "gate_state": gate,
        "drs_level": drs,
        "execution_band": (str(execution).upper() if execution is not None else None),
        "trend_state": (str(trend_state).upper() if trend_state is not None else None),
        "adv_ratio": adv_ratio,
        "pct_above_ma20": pct_above_ma20,
        "new_low_ratio_pct": new_low_ratio_pct,
        "amount_ratio": amount_ratio,
        "proxy_top20_amount_ratio": proxy_top20_amount_ratio,
        "failure_rate_level": aw.get("failure_rate_level") or _pick_num(["failure_rate_level"]),
        "decision_reasons": decision_reasons,
        "constraint_summary": constraint_summary,
        "allowed_actions": allowed_actions,
        "forbidden_actions": forbidden_actions,
        "rollback_triggers_hit": aw.get("rollback_triggers_hit") or aw.get("rollback_hit"),
        "audit_notes": aw.get("audit_notes"),
        "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }

    # Normalize audit_notes to dict and attach missing_fields / reason_details.
    audit_notes = day_rec.get("audit_notes")
    if audit_notes is None:
        audit_notes = {}
    if not isinstance(audit_notes, dict):
        audit_notes = {"note": audit_notes}

    # If upstream provided only D_* reasons, keep them but don't let them pollute decision_reasons.
    if isinstance(raw_reasons, list) and raw_reasons:
        if all(isinstance(x, str) and x.startswith("D_") for x in raw_reasons):
            audit_notes.setdefault("reason_details", []).extend(raw_reasons)
        else:
            # mixed reasons: keep as details for forensic, decision stays minimal
            audit_notes.setdefault("reason_details", []).extend([x for x in raw_reasons if isinstance(x, str)])

    missing_fields: List[str] = []
    if day_rec.get("execution_band") is None:
        missing_fields.append("execution_band")
    if day_rec.get("trend_state") is None:
        missing_fields.append("trend_state")
    if day_rec.get("new_low_ratio_pct") is None:
        missing_fields.append("new_low_ratio_pct")
    if day_rec.get("amount_ratio") is None:
        missing_fields.append("amount_ratio")
    if missing_fields:
        audit_notes.setdefault("missing_fields", []).extend(missing_fields)

    day_rec["audit_notes"] = audit_notes

    _append_jsonl(path, day_rec)
    # Update weekly summary (dedup by trade_date, keep latest ts)
    try:
        _update_weekly_outputs(path, td)
    except Exception:
        pass

    # CASE snapshot only for selected states
    if not isinstance(state, str) or state not in set(record_states):
        if logger is not None:
            try:
                logger.debug("[CaseLog] day-only state=%s trade_date=%s path=%s", state or "N/A", td, path)
            except Exception:
                pass
        return

    case_rec = {
        "event": "ATTACK_WINDOW_CASE",
        "trade_date": td,
        "report_kind": report_kind,
        "attack_state": state,
        "gate_state": gate,
        "drs_level": drs,
        "frf": frf,
        "execution_band": day_rec.get("execution_band"),
        "trend_state": day_rec.get("trend_state"),
        "decision_reasons": day_rec.get("decision_reasons"),
        "constraint_summary": day_rec.get("constraint_summary"),
        "allowed_actions": day_rec.get("allowed_actions"),
        "forbidden_actions": day_rec.get("forbidden_actions"),
        "rollback_triggers_hit": day_rec.get("rollback_triggers_hit"),
        "audit_notes": day_rec.get("audit_notes"),
        "evidence": evidence or details or {},
        "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
    _append_jsonl(path, case_rec)

    if logger is not None:
        try:
            logger.info("[CaseLog] wrote state=%s trade_date=%s path=%s", state, td, path)
        except Exception:
            pass
