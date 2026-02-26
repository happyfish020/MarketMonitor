# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - Attack Window Evaluator (AW-V12-ATTACK-WINDOW-V1)

Frozen Engineering Contract:
- Read-only: does NOT modify Gate/DRS/Execution.
- Uses ONLY existing slots (no new data sources).
- Produces a stable slot payload: slots["attack_window"] with schema "AW_V1".
- Auditable: explicit reasons_yes/no and evidence snapshot.

V12 Contract Fixes (2026-02-05 batch):
- Resolve "口径问题": expose BOTH market_top20_trade_ratio (全市场口径) and proxy_top20_amount_ratio (拥挤代理口径).
- Fix north_proxy_pressure mapping: support details.pressure_level (e.g., NEUTRAL) and score-based fallback.
- Fix freshness false-negative: market_overview may not carry meta.asof; infer asof from structure/meta or slots.data_freshness.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import datetime

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None  # type: ignore


def _now_date_iso() -> str:
    return datetime.date.today().isoformat()


def _get(d: Dict[str, Any], path: str, default: Any = None) -> Any:
    cur: Any = d
    for key in path.split("."):
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def _to_float(v: Any) -> Optional[float]:
    try:
        if v is None or isinstance(v, bool):
            return None
        return float(v)
    except Exception:
        return None


def _level_norm(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, str):
        return v.strip().upper()
    return str(v).strip().upper()


def _map_color_to_level(v: Any) -> Optional[str]:
    """
    Some panels may output color words (GREEN/YELLOW/ORANGE/RED) instead of LOW/MED/HIGH.
    This helper normalizes them to LOW/MED/HIGH with conservative mapping.
    """
    s = _level_norm(v)
    if s is None:
        return None
    if s in ("LOW", "MED", "HIGH"):
        return s
    if s in ("GREEN",):
        return "LOW"
    if s in ("YELLOW",):
        return "MED"
    if s in ("ORANGE", "RED"):
        return "HIGH"
    if s in ("NEUTRAL",):
        return "MED"
    return s


def _extract_market_top20_trade_ratio(slots: Dict[str, Any]) -> Optional[float]:
    """
    全市场口径：Top20 成交占比（用于解释“集中度/拥挤”）
    Priority:
      1) slots.market_overview.top20_ratio / top20 / top20_trade_ratio
      2) slots.factors.liquidity_quality.details.top20_ratio
      3) watchlist_lead.lead_panels.F ... (best-effort)
    """
    # 1) market_overview
    for p in (
        "market_overview.top20_ratio",
        "market_overview.top20",
        "market_overview.market_top20_trade_ratio",
        "market_overview.top20_trade_ratio",
    ):
        v = _to_float(_get(slots, p))
        if v is not None:
            # tolerate 0-100
            return v / 100.0 if v > 1.5 else v

    # 2) liquidity_quality factor
    v = _to_float(_get(slots, "factors.liquidity_quality.details.top20_ratio"))
    if v is not None:
        return v / 100.0 if v > 1.5 else v

    # 3) watchlist_lead panel (best-effort)
    for p in (
        "factors.watchlist_lead.details.lead_panels.F.top20_ratio",
        "factors.watchlist_lead.details.lead_panels.f_liquidity_quality.top20_ratio",
        "factors.watchlist_lead.details.lead_panels.f_liquidity_quality.raw.top20_ratio",
    ):
        v = _to_float(_get(slots, p))
        if v is not None:
            return v / 100.0 if v > 1.5 else v
    return None


def _extract_proxy_top20_amount_ratio(slots: Dict[str, Any]) -> Optional[float]:
    """
    拥挤代理口径：top20_amount_ratio / proxy_top20pct
    Priority:
      1) slots.crowding_concentration.top20_amount_ratio
      2) slots.structure.crowding_concentration.top20_amount_ratio
      3) market_overview.top20_amount_ratio
    """
    for p in (
        "crowding_concentration.top20_amount_ratio",
        "crowding_concentration.evidence.top20_amount_ratio",
        "structure.crowding_concentration.top20_amount_ratio",
        "structure.crowding_concentration.evidence.top20_amount_ratio",
        "market_overview.top20_amount_ratio",
        "market_overview.top20_amount_ratio_pct",
    ):
        v = _to_float(_get(slots, p))
        if v is not None:
            return v / 100.0 if v > 1.5 else v
    return None


def _extract_north_level(slots: Dict[str, Any]) -> Tuple[Optional[str], Optional[float]]:
    """
    north_proxy_pressure comes from Factor.details (not necessarily .level).
    Prefer pressure_level; fallback to level; fallback score mapping.
    Returns (level, score).
    """
    # typical in details: pressure_level / pressure_score
    ds = _get(slots, "structure.north_proxy_pressure.evidence.data_status") or _get(slots, "north_proxy_pressure.data_status")
    if isinstance(ds, str) and ds.upper() in ("DATA_NOT_CONNECTED", "NA", "MISSING", "ERROR", "PARTIAL"):
        # Explicit NA: never return None, so downstream can treat it as unknown but consistent.
        sc0 = _to_float(_get(slots, "structure.north_proxy_pressure.evidence.pressure_score"))
        if sc0 is None:
            sc0 = _to_float(_get(slots, "north_proxy_pressure.pressure_score"))
        return "NA", sc0
    # prefer structure facts evidence (your V12 report uses this shape)
    lvl = _map_color_to_level(_get(slots, "structure.north_proxy_pressure.evidence.pressure_level"))
    score = _to_float(_get(slots, "structure.north_proxy_pressure.evidence.pressure_score"))
    if lvl is None:
        lvl = _map_color_to_level(_get(slots, "north_proxy_pressure.pressure_level"))
    if score is None:
        score = _to_float(_get(slots, "north_proxy_pressure.pressure_score"))

    if lvl in ("LOW", "MED", "HIGH"):
        return lvl, score

    # sometimes factor wrapper provides level
    lvl2 = _map_color_to_level(_get(slots, "north_proxy_pressure.level"))
    if lvl2 in ("LOW", "MED", "HIGH"):
        return lvl2, score

    # fallback score->level (conservative)
    if score is not None:
        # lower pressure_score = lower pressure, per factor semantics: pressure_score = 100 - quality
        if score >= 70:
            return "HIGH", score
        if score >= 35:
            return "MED", score
        return "LOW", score

    # allow strings like NEUTRAL to map
    lvl3 = _map_color_to_level(_get(slots, "north_proxy_pressure.pressure_level"))
    if lvl3 in ("MED",):
        return "MED", score

    # Never return None for level: keep explicit NA for consistency
    return "NA", score


def _extract_leverage_level(slots: Dict[str, Any]) -> Optional[str]:
    """
    Leverage constraint level. Prefer explicit LOW/MED/HIGH.
    If upstream provides only panel color, map with _map_color_to_level.
    """
    for p in (
        "leverage_constraints.level",
        "leverage_constraints.risk_level",
        "leverage_constraints.overall",
        "leverage_constraints.panel",
        "leverage_constraints.state",
        "leverage_constraints.color",
        # watchlist_lead panel G
        "factors.watchlist_lead.details.lead_panels.G.overall",
        "factors.watchlist_lead.details.lead_panels.G.level",
        "factors.watchlist_lead.details.lead_panels.G.state",
    ):
        lvl = _map_color_to_level(_get(slots, p))
        if lvl in ("LOW", "MED", "HIGH"):
            return lvl
    return None


def _extract_options_level(slots: Dict[str, Any]) -> Optional[str]:
    for p in (
        "options_risk.level",
        "options_risk.risk_level",
        "options_risk.overall",
        "options_risk.color",
        "options_risk.state",
        # watchlist_lead panel E
        "factors.watchlist_lead.details.lead_panels.E.overall",
        "factors.watchlist_lead.details.lead_panels.E.level",
        "factors.watchlist_lead.details.lead_panels.E.state",
    ):
        lvl = _map_color_to_level(_get(slots, p))
        if lvl in ("LOW", "MED", "HIGH"):
            return lvl
    return None


def _extract_failure_level(slots: Dict[str, Any]) -> Optional[str]:
    # Support both level and state strings
    lvl = _map_color_to_level(_get(slots, "failure_rate.level"))
    if lvl in ("LOW", "MED", "HIGH"):
        return lvl

    state = _level_norm(_get(slots, "failure_rate.state"))
    # common states in your report: watch / elevated_risk etc.
    if state in ("OK", "GREEN", "LOW"):
        return "LOW"
    if state in ("WATCH", "YELLOW", "MED"):
        return "MED"
    if state in ("ELEVATED_RISK", "ORANGE", "RED", "HIGH"):
        return "HIGH"
    return None


@dataclass(frozen=True)
class AttackWindowResult:
    asof: str
    state: str               # OFF / LITE / ON
    gate: str                # NORMAL / CAUTION / D1 / D2 / D3
    offense_permission: str  # FORBID / LITE / ALLOW
    reasons_yes: List[str]
    reasons_no: List[str]
    evidence: Dict[str, Any]
    data_freshness: Dict[str, Any]

    def to_slot(self) -> Dict[str, Any]:
        return {
            "meta": {"asof": self.asof, "schema": "AW_V1"},
            "state": self.state,
            "gate": self.gate,
            "offense_permission": self.offense_permission,
            "reasons_yes": list(self.reasons_yes),
            "reasons_no": list(self.reasons_no),
            "evidence": dict(self.evidence),
            "data_freshness": dict(self.data_freshness),
        }


class AttackWindowEvaluator:
    def __init__(self, rule_spec_path: str):
        if yaml is None:
            raise RuntimeError("PyYAML is required to load config/rules/attack_window.yaml (pip install pyyaml).")
        self.rule_spec_path = rule_spec_path
        self.spec = self._load_spec(rule_spec_path)

    @staticmethod
    def _load_spec(path: str) -> Dict[str, Any]:
        with open(path, "r", encoding="utf-8") as f:
            spec = yaml.safe_load(f)
        if not isinstance(spec, dict):
            raise ValueError("attack_window.yaml must be a YAML mapping object.")
        if spec.get("schema_version") != 1:
            raise ValueError("attack_window.yaml schema_version must be 1.")
        if spec.get("rule_id") != "AW-V12-ATTACK-WINDOW-V1":
            raise ValueError("attack_window.yaml rule_id mismatch.")
        if spec.get("status") != "FROZEN":
            raise ValueError("attack_window.yaml status must be FROZEN.")
        return spec

    def evaluate(self, slots: Dict[str, Any]) -> Dict[str, Any]:
        # Prefer global asof sources (structure / data_freshness) because market_overview may lack meta.asof
        asof = (
            _get(slots, "trade_date")
            or _get(slots, "meta.trade_date")
            or _get(slots, "structure.meta.asof")
            or _get(slots, "data_freshness.asof")
            or _get(slots, "market_overview.meta.asof")
            or _get(slots, "breadth.meta.asof")
            or _get(slots, "trend_in_force.meta.asof")
            or _now_date_iso()
        )

        gate = _level_norm(_get(slots, "gate_decision.gate", "UNKNOWN")) or "UNKNOWN"

        reasons_yes: List[str] = []
        reasons_no: List[str] = []

        evidence = self._build_evidence(slots)
        data_freshness = self._freshness_check(slots)

        # P0-FIX-C: Hard veto if critical structure fields are missing.
        critical = ["trend_state", "adv_ratio", "pct_above_ma20", "amount_ma20_ratio", "failure_rate_level", "north_proxy_level"]
        missing_critical = [k for k in critical if evidence.get(k) is None or (isinstance(evidence.get(k), str) and str(evidence.get(k)).strip() == "")]
        # north_proxy_level should never be None; treat NA as missing-for-release (still explicit).
        if str(evidence.get("north_proxy_level") or "").upper() in ("NA", "UNKNOWN"):
            if "north_proxy_level" not in missing_critical:
                missing_critical.append("north_proxy_level")
        if missing_critical:
            evidence["hard_missing_fields"] = missing_critical
            reasons_no.append("Z_hard_missing_fields:" + ",".join(missing_critical))
            evidence["allowed_actions"] = ["HOLD", "TRIM_ON_STRENGTH"]
            evidence["forbidden_actions"] = ["ADD_RISK", "CHASE_ADD", "NEW_POSITION", "ROTATION_ATTACK"]
            evidence["constraint_summary"] = "OFF: critical fields missing → forbid new risk; hold/trim only"
            return {
                "asof": asof,
                "attack_state": "OFF",
                "prev_attack_state": None,
                "gate_state": gate,
                "drs_level": _get(slots, "drs.level"),
                "execution_band": _get(slots, "execution.band"),
                "trend_state": evidence.get("trend_state"),
                "evidence": evidence,
                "decision_reasons": {"yes": reasons_yes, "no": reasons_no},
                "constraint_summary": evidence.get("constraint_summary"),
                "allowed_actions": evidence.get("allowed_actions"),
                "forbidden_actions": evidence.get("forbidden_actions"),
                "data_freshness": data_freshness,
            }

        # ------------------------------------------------------------------
        # BROKEN_OBSERVING -> VERIFY_ONLY (decoupled branch)
        #
        # Design intent:
        # - When trend_in_force is BROKEN but breadth/amount/new-low show repair signs,
        #   allow a strictly limited VERIFY_ONLY window.
        # - This branch MUST NOT inherit ON-only gates (failure_rate forbid, stable_days>=2,
        #   amount>=1.0 etc.).
        # - Hard forbids still apply: leverage/options.
        # ------------------------------------------------------------------
        obs_cfg = (self.spec.get("thresholds") or {}).get("A_structure", {}).get("observing", {})
        if isinstance(obs_cfg, dict) and obs_cfg.get("enable", False):
            trend_state_raw = str(evidence.get("trend_state") or "").strip().lower()
            is_broken = any(x in trend_state_raw for x in ("broken", "broad_damage", "damage"))
            if is_broken:
                pct_ma20_min = float(obs_cfg.get("pct_above_ma20_min", 0.50))
                newlow_max = float(obs_cfg.get("new_low_ratio_pct_max", 1.50))
                amt_min = float(obs_cfg.get("amount_ma20_ratio_min", 0.75))
                proxy_max = float(obs_cfg.get("proxy_top20_amount_ratio_max", 0.80))

                pct_ma20 = _to_float(evidence.get("pct_above_ma20"))
                newlow = _to_float(evidence.get("new_low_ratio_pct"))
                amt = _to_float(evidence.get("amount_ma20_ratio"))
                proxy_top20 = _to_float(evidence.get("proxy_top20_amount_ratio"))

                obs_ok = (
                    (pct_ma20 is not None and pct_ma20 >= pct_ma20_min)
                    and (newlow is not None and newlow <= newlow_max)
                    and (amt is not None and amt >= amt_min)
                    and (proxy_top20 is None or proxy_top20 < proxy_max)
                )

                if obs_ok:
                    evidence["trend_state"] = "BROKEN_OBSERVING"
                    evidence["allowed_actions"] = ["BASE_VERIFY_ADD", "PULLBACK_VERIFY", "HOLD", "TRIM_ON_STRENGTH"]
                    evidence["forbidden_actions"] = ["CHASE_ADD", "SCALE_UP", "NEW_POSITION", "ROTATION_ATTACK", "LEVERAGE", "OPTIONS"]
                    evidence["constraint_summary"] = "VERIFY_ONLY：仅允许验证仓（1手/1档），不追价、不加杠杆、不扩风险；可持有或逢强减仓。"

                    lev_level = _extract_leverage_level(slots)
                    opt_level = _extract_options_level(slots)
                    forbid_lev = _level_norm(lev_level) in ("HIGH", "RED")
                    forbid_opt = _level_norm(opt_level) in ("HIGH", "RED")

                    reasons_yes = ["A_structure_broken_observing:true"]
                    reasons_no: List[str] = []
                    if forbid_lev:
                        reasons_no.append(f"D_leverage_forbid:{lev_level}")
                    if forbid_opt:
                        reasons_no.append(f"D_options_forbid:{opt_level}")

                    raw_state = "VERIFY_ONLY" if (not forbid_lev and not forbid_opt) else "OFF"
                    offense_permission = self._map_permission_by_gate(gate, raw_state)

                    # Gate clipping: LITE => VERIFY_ONLY, ALLOW => VERIFY_ONLY (still verify), FORBID => OFF
                    if offense_permission == "FORBID":
                        state = "OFF"
                    else:
                        state = "VERIFY_ONLY"

                    rb: List[str] = []
                    adv_ratio = _to_float(evidence.get("adv_ratio"))
                    if adv_ratio is not None and adv_ratio < 0.45:
                        rb.append("adv_ratio<0.45")
                    if newlow is not None and newlow > 2.0:
                        rb.append("new_low_ratio_pct>2.0")
                    if amt is not None and amt < 0.72:
                        rb.append("amount_ratio<0.72")
                    if proxy_top20 is not None and proxy_top20 >= 0.80:
                        rb.append("proxy_top20>=0.80")
                    evidence["rollback_triggers_hit"] = rb

                    # Reporter-friendly layered explanations
                    evidence["decision_reasons"] = [
                        "Structure=BROKEN_OBSERVING",
                        f"Gate={gate}",
                    ]
                    evidence["audit_notes"] = [
                        f"failure_rate_level={evidence.get('failure_rate_level')}",
                        f"adv_ratio={adv_ratio}",
                        f"pct_above_ma20={pct_ma20}",
                        f"amount_ma20_ratio={amt}",
                        f"proxy_top20_amount_ratio={proxy_top20}",
                        f"north_proxy_level={evidence.get('north_proxy_level')}",
                    ]

                    return AttackWindowResult(
                        asof=str(asof),
                        state=state,
                        gate=gate,
                        offense_permission=offense_permission,
                        reasons_yes=reasons_yes,
                        reasons_no=reasons_no,
                        evidence=evidence,
                        data_freshness=data_freshness,
                    ).to_slot()

        A_ok, A_yes, A_no = self._check_A_structure(slots)
        B_ok, B_yes, B_no = self._check_B_failure_rate(slots)
        C_ok, C_yes, C_no = self._check_C_participation(slots, evidence)
        D_ok, D_yes, D_no = self._check_D_constraints_release(slots, evidence)

        reasons_yes.extend(A_yes + B_yes + C_yes + D_yes)
        reasons_no.extend(A_no + B_no + C_no + D_no)

        raw_state = "ON" if (A_ok and B_ok and C_ok and D_ok) else "OFF"
        if raw_state == "OFF":
            # LIGHT_ON: allow small, controlled offense in non-bull regimes
            # Requirements: structure ok + participation ok + no hard constraints (D_ok) + failure_rate not in persistent HIGH.
            if A_ok and C_ok and D_ok:
                fr_level = _level_norm(evidence.get("failure_rate_level"))
                improve_days = _to_float(evidence.get("failure_rate_improve_days"))
                if (fr_level in ("LOW", "MED")) or (improve_days is not None and improve_days >= 1):
                    raw_state = "LIGHT_ON"

            # VERIFY_ONLY (legacy LITE): structure & participation ok but constraints prevent real offense
            if raw_state == "OFF" and A_ok and B_ok and C_ok and (not D_ok):
                raw_state = "VERIFY_ONLY"

            if raw_state == "OFF" and A_ok and (not B_ok) and C_ok and D_ok:
                # failure_rate not great, but if not persistently HIGH, allow VERIFY_ONLY
                fr_level = _level_norm(evidence.get("failure_rate_level"))
                if fr_level in ("MED", "LOW"):
                    raw_state = "VERIFY_ONLY"

        offense_permission = self._map_permission_by_gate(gate, raw_state)

        # Map raw_state -> final state under Gate clipping
        if offense_permission == "FORBID":
            state = "OFF"
        elif offense_permission == "LITE":
            # LITE permission always maps to VERIFY_ONLY (no ambiguity)
            state = "VERIFY_ONLY"
        elif offense_permission == "ALLOW":
            state = raw_state if raw_state in ("ON", "LIGHT_ON") else "VERIFY_ONLY"
        else:
            state = "OFF"

        if raw_state in ("ON", "LIGHT_ON") and state not in ("ON", "LIGHT_ON"):
            reasons_no.append(f"gate_clip:{gate}:{raw_state}->{state}")

        # Reporter-friendly layered explanations + action boundaries
        if state == "LIGHT_ON":
            evidence["allowed_actions"] = ["BASE_ETF_ADD", "PULLBACK_ADD", "HOLD", "TRIM_ON_STRENGTH"]
            evidence["forbidden_actions"] = ["CHASE_ADD", "SCALE_UP", "NEW_POSITION", "ROTATION_ATTACK", "LEVERAGE", "OPTIONS"]
            evidence["constraint_summary"] = "LIGHT_ON：允许小规模进攻（分步/不追价/不扩风险），单日新增风险建议≤计划仓位20%，随时可回退。"
            evidence["decision_reasons"] = [
                "State=LIGHT_ON",
                f"Gate={gate}",
            ]
            evidence["audit_notes"] = [
                f"failure_rate_level={evidence.get('failure_rate_level')}",
                f"adv_ratio={evidence.get('adv_ratio')}",
                f"pct_above_ma20={evidence.get('pct_above_ma20')}",
                f"amount_ma20_ratio={evidence.get('amount_ma20_ratio')}",
                f"proxy_top20_amount_ratio={evidence.get('proxy_top20_amount_ratio')}",
            ]
        elif state == "ON":
            evidence["allowed_actions"] = ["ADD_RISK", "PULLBACK_ADD", "HOLD", "TRIM_ON_STRENGTH"]
            evidence["forbidden_actions"] = ["CHASE_ADD", "LEVERAGE", "OPTIONS"]
            evidence["constraint_summary"] = "ON：允许进攻（仍需遵守Gate/Execution约束与仓位上限），优先回撤加、避免追价。"
            evidence["decision_reasons"] = [
                "State=ON",
                f"Gate={gate}",
            ]
        elif state == "VERIFY_ONLY" and "constraint_summary" not in evidence:
            evidence["constraint_summary"] = "VERIFY_ONLY：仅允许验证仓（1手/1档），不追价、不加杠杆、不扩风险。"

        return AttackWindowResult(
            asof=str(asof),
            state=state,
            gate=gate,
            offense_permission=offense_permission,
            reasons_yes=reasons_yes,
            reasons_no=reasons_no,
            evidence=evidence,
            data_freshness=data_freshness,
        ).to_slot()

    def _build_evidence(self, slots: Dict[str, Any]) -> Dict[str, Any]:
        trend_state = (_get(slots, "trend_in_force.state") or _get(slots, "trend_in_force.trend_state") or "unknown")
        # Participation / breadth evidence (prefer structure facts, then breadth_plus, then breadth)
        adv_ratio = (
            _to_float(_get(slots, "structure.crowding_concentration.evidence.adv_ratio"))
            or _to_float(_get(slots, "crowding_concentration.evidence.adv_ratio"))
            or _to_float(_get(slots, "breadth_plus.key_metrics.adv_ratio"))
            or _to_float(_get(slots, "breadth.adv_ratio"))
        )
        pct_above_ma20 = (
            _to_float(_get(slots, "breadth_plus.key_metrics.pct_above_ma20"))
            or _to_float(_get(slots, "breadth.pct_above_ma20"))
        )

        new_low_ratio_pct = _to_float(_get(slots, "breadth_plus.key_metrics.new_low_ratio_pct"))
        if new_low_ratio_pct is None:
            v = _to_float(_get(slots, "breadth.new_low_ratio"))
            if v is not None:
                # breadth.new_low_ratio is typically [0,1] ratio
                new_low_ratio_pct = v * 100.0 if v <= 1.0 else v
        # Hard fallback from breadth factor raw counts (most reliable)
        cnt = _to_float(_get(slots, "factors.breadth.details._raw_data.count_new_low"))
        tot = _to_float(_get(slots, "factors.breadth.details._raw_data.count_total"))
        if cnt is not None and tot is not None and tot > 0:
            new_low_ratio_pct = (cnt / tot) * 100.0

        # Amount contraction evidence (prefer structure.amount.evidence.amount_ratio)
        amount_ma20_ratio = (
            _to_float(_get(slots, "structure.amount.evidence.amount_ratio"))
            or _to_float(_get(slots, "amount.evidence.amount_ratio"))
            or _to_float(_get(slots, "market_overview.amount_ma20_ratio"))
            or _to_float(_get(slots, "structure.amount_ratio"))
        )

        proxy_top20 = _extract_proxy_top20_amount_ratio(slots)
        market_top20 = _extract_market_top20_trade_ratio(slots)

        # For rule use: prefers proxy (crowding) because it is closer to your "拥挤代理" semantics.
        # P0.1 cleanup: keep this as an *internal rule field* only (avoid exposing as evidence key
        # "top20_ratio" which is easily confused with market_top20_trade_ratio).
        rule_top20_ratio = proxy_top20 if proxy_top20 is not None else market_top20

        fr_level = _extract_failure_level(slots)
        fr_improve_days = (
            _get(slots, "failure_rate.improve_days")
            or _get(slots, "failure_rate.improvement_days")
            or _get(slots, "failure_rate.improve_streak_days")
        )

        north_level, north_score = _extract_north_level(slots)
        lev_level = _extract_leverage_level(slots)
        opt_level = _extract_options_level(slots)

        return {
            "trend_state": trend_state,
            "adv_ratio": adv_ratio,
            "pct_above_ma20": pct_above_ma20,
            "new_low_ratio_pct": new_low_ratio_pct,
            "amount_ma20_ratio": amount_ma20_ratio,
            # 口径显性化
            "market_top20_trade_ratio": market_top20,
            "proxy_top20_amount_ratio": proxy_top20,
            # internal rule-only field (do not expose as "top20_ratio")
            "rule_top20_ratio": rule_top20_ratio,
            "failure_rate_level": fr_level,
            "failure_rate_improve_days": fr_improve_days,
            "north_proxy_level": north_level,
            "north_proxy_score": north_score,
            "leverage_level": lev_level,
            "options_level": opt_level,
        }

    def _freshness_check(self, slots: Dict[str, Any]) -> Dict[str, Any]:
        hard = self.spec.get("inputs", {}).get("freshness_policy", {}).get("hard_required", [])
        notes: List[str] = []
        ok = True

        # global data freshness module exists in V12 baseline
        global_asof = (
            _get(slots, "trade_date")
            or _get(slots, "meta.trade_date")
            or _get(slots, "structure.meta.asof")
            or _get(slots, "data_freshness.asof")
        )

        for k in hard:
            # slot exists?
            obj = slots.get(k)
            asof = _get(slots, f"{k}.meta.asof")
            if asof:
                continue

            # market_overview often is a derived view and may not carry meta.asof; infer from trade_date/global_asof
            if k == "market_overview" and (global_asof or _get(slots, "trade_date") or _get(slots, "meta.trade_date")):
                notes.append("asof_inferred:market_overview<-trade_date_or_structure/meta")
                continue

            ok = False
            notes.append(f"missing_asof:{k}")

        return {"asof_ok": ok, "notes": notes}

    def _check_A_structure(self, slots: Dict[str, Any]) -> Tuple[bool, List[str], List[str]]:
        th = self.spec.get("thresholds", {}).get("A_structure", {})
        allowed = [str(x).lower() for x in th.get("trend_states_allowed", ["intact", "recovering"])]
        price_days_min = int(th.get("price_above_ma20_min_days", 2))
        slope_min = float(th.get("ma20_slope_min", 0.0))
        forbid_first_rebound = bool(th.get("forbid_first_rebound", True))

        trend_state = str(_get(slots, "trend_in_force.state", "unknown")).lower()
        price_days = _get(slots, "trend_in_force.price_above_ma20_days")
        ma20_slope = _get(slots, "trend_in_force.ma20_slope")
        is_first_rebound = _get(slots, "trend_in_force.is_first_rebound_day")

        yes: List[str] = []
        no: List[str] = []
        ok = True

        if trend_state not in allowed:
            ok = False
            no.append(f"A_structure_trend_state_forbid:{trend_state}")
        else:
            yes.append(f"A_structure_trend_state_ok:{trend_state}")

        if price_days is None:
            ok = False
            no.append("A_structure_missing:price_above_ma20_days")
        else:
            try:
                if int(price_days) >= price_days_min:
                    yes.append(f"A_structure_price_above_ma20_days_ok:{price_days}")
                else:
                    ok = False
                    no.append(f"A_structure_price_above_ma20_days_lt:{price_days}<{price_days_min}")
            except Exception:
                ok = False
                no.append("A_structure_invalid:price_above_ma20_days")

        if ma20_slope is None:
            ok = False
            no.append("A_structure_missing:ma20_slope")
        else:
            try:
                if float(ma20_slope) > slope_min:
                    yes.append(f"A_structure_ma20_slope_ok:{ma20_slope}")
                else:
                    ok = False
                    no.append(f"A_structure_ma20_slope_le:{ma20_slope}<={slope_min}")
            except Exception:
                ok = False
                no.append("A_structure_invalid:ma20_slope")

        if forbid_first_rebound and isinstance(is_first_rebound, bool):
            if is_first_rebound:
                ok = False
                no.append("A_structure_forbid_first_rebound_day:true")
            else:
                yes.append("A_structure_forbid_first_rebound_day:false")

        return ok, yes, no

    def _check_B_failure_rate(self, slots: Dict[str, Any]) -> Tuple[bool, List[str], List[str]]:
        th = self.spec.get("thresholds", {}).get("B_failure_rate", {})
        forbid_levels = [str(x).upper() for x in th.get("level_forbid", ["HIGH"])]
        allow_levels = [str(x).upper() for x in th.get("allow_if_level", ["LOW", "MED"])]
        improve_min_days = int(th.get("improvement_min_days", 3))

        level = _extract_failure_level(slots)
        improve_days = _get(slots, "failure_rate.improve_days")

        yes: List[str] = []
        no: List[str] = []
        ok = True

        if level is None:
            return False, yes, ["B_failure_rate_missing:level"]

        if level in forbid_levels:
            ok = False
            no.append(f"B_failure_rate_level_forbid:{level}")
        elif level in allow_levels:
            yes.append(f"B_failure_rate_level_ok:{level}")
        else:
            ok = False
            no.append(f"B_failure_rate_level_unknown:{level}")

        if improve_days is None:
            ok = False
            no.append("B_failure_rate_missing:improve_days")
        else:
            try:
                if int(improve_days) >= improve_min_days:
                    yes.append(f"B_failure_rate_improve_days_ok:{improve_days}")
                else:
                    no.append(f"B_failure_rate_improve_days_lt:{improve_days}<{improve_min_days}")
                    ok = ok and (level == "LOW")
            except Exception:
                ok = False
                no.append("B_failure_rate_invalid:improve_days")

        return ok, yes, no

    def _check_C_participation(self, slots: Dict[str, Any], evidence: Dict[str, Any]) -> Tuple[bool, List[str], List[str]]:
        th = self.spec.get("thresholds", {}).get("C_participation", {})
        require_any = bool(th.get("require_any", True))
        conditions = th.get("conditions", []) or []

        yes: List[str] = []
        no: List[str] = []
        passed = 0

        for cond in conditions:
            cid = cond.get("id")
            if cid == "adv_ratio_stable":
                adv_min = float(cond.get("adv_ratio_min", 0.55))
                stable_need = int(cond.get("stable_days", 2))
                adv = evidence.get("adv_ratio")
                stable = _get(slots, "breadth.adv_ratio_stable_days", 1)
                if adv is None:
                    no.append("C_participation_missing:breadth.adv_ratio")
                    continue
                if float(adv) >= adv_min and int(stable) >= stable_need:
                    passed += 1
                    yes.append(f"C_adv_ratio_ok:{adv} days={stable}")
                else:
                    no.append(f"C_adv_ratio_fail:{adv} days={stable} need>={adv_min}/{stable_need}")

            elif cid == "pct_above_ma20_expand":
                pct_min = float(cond.get("pct_above_ma20_min", 0.55))
                stable_need = int(cond.get("stable_days", 2))
                pct = evidence.get("pct_above_ma20")
                stable = _get(slots, "breadth.pct_above_ma20_stable_days", 1)
                if pct is None:
                    no.append("C_participation_missing:breadth.pct_above_ma20")
                    continue
                if float(pct) >= pct_min and int(stable) >= stable_need:
                    passed += 1
                    yes.append(f"C_pct_above_ma20_ok:{pct} days={stable}")
                else:
                    no.append(f"C_pct_above_ma20_fail:{pct} days={stable} need>={pct_min}/{stable_need}")

            elif cid == "amount_ok_and_top20_not_worse":
                amt_min = float(cond.get("amount_ma20_ratio_min", 1.00))
                top20_max = float(cond.get("top20_ratio_max", 0.75))
                amt = evidence.get("amount_ma20_ratio")
                top20 = evidence.get("rule_top20_ratio")
                if amt is None:
                    no.append("C_participation_missing:amount_ma20_ratio")
                    continue
                if top20 is None:
                    no.append("C_participation_missing:rule_top20_ratio")
                    continue
                tval = float(top20)
                if tval > 1.5:
                    tval = tval / 100.0
                if float(amt) >= amt_min and tval <= top20_max:
                    passed += 1
                    yes.append(f"C_amount_top20_ok:amt={amt} top20={tval}")
                else:
                    no.append(f"C_amount_top20_fail:amt={amt} top20={tval} need>={amt_min} and <={top20_max}")

            else:
                no.append(f"C_participation_unknown_condition:{cid}")

        ok = (passed >= 1) if require_any else (passed == len(conditions))
        if not ok:
            no.append(f"C_participation_require_any_fail:passed={passed}")
        return ok, yes, no

    def _check_D_constraints_release(self, slots: Dict[str, Any], evidence: Dict[str, Any]) -> Tuple[bool, List[str], List[str]]:
        th = self.spec.get("thresholds", {}).get("D_constraints_release", {})
        north_forbid = [str(x).upper() for x in th.get("north_proxy_forbid_levels", ["HIGH"])]
        lev_forbid = [str(x).upper() for x in th.get("leverage_forbid_levels", ["HIGH"])]
        opt_forbid = [str(x).upper() for x in th.get("options_forbid_levels", ["HIGH"])]

        north = _level_norm(evidence.get("north_proxy_level"))
        lev = _level_norm(evidence.get("leverage_level"))
        opt = _level_norm(evidence.get("options_level"))

        yes: List[str] = []
        no: List[str] = []
        ok = True

        if north is None:
            ok = False
            no.append("D_constraints_unknown:north_proxy_level")
        elif north in north_forbid:
            ok = False
            no.append(f"D_north_proxy_forbid:{north}")
        else:
            yes.append(f"D_north_proxy_ok:{north}")

        if lev is None:
            ok = False
            no.append("D_constraints_unknown:leverage_level")
        elif lev in lev_forbid:
            ok = False
            no.append(f"D_leverage_forbid:{lev}")
        else:
            yes.append(f"D_leverage_ok:{lev}")

        if opt is None:
            ok = False
            no.append("D_constraints_unknown:options_level")
        elif opt in opt_forbid:
            ok = False
            no.append(f"D_options_forbid:{opt}")
        else:
            yes.append(f"D_options_ok:{opt}")

        return ok, yes, no

    def _map_permission_by_gate(self, gate: str, raw_state: str) -> str:
        mapping = self.spec.get("governance_mapping", {}).get("gate_to_permission", {})
        gate_map = mapping.get(gate)
        if not isinstance(gate_map, dict):
            return "FORBID"
        perm = gate_map.get(raw_state)
        if perm is None:
            return "FORBID"
        return str(perm).upper()
