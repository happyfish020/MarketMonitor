from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union

LOG = logging.getLogger(__name__)


def _as_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        if isinstance(v, bool):
            return None
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, str):
            s = v.strip()
            if not s:
                return None
            return float(s)
    except Exception:
        return None
    return None


def _get_nested(root: Any, path: Tuple[str, ...]) -> Any:
    cur = root
    for k in path:
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return None
    return cur


def _get_details(obj: Any) -> Optional[Dict[str, Any]]:
    # FactorResult may be a dict or an object with .details
    if isinstance(obj, dict):
        d = obj.get("details")
        return d if isinstance(d, dict) else None
    try:
        d = getattr(obj, "details", None)
        return d if isinstance(d, dict) else None
    except Exception:
        return None


def _get_level(obj: Any) -> Optional[str]:
    if isinstance(obj, dict):
        lv = obj.get("level")
        return lv if isinstance(lv, str) else None
    try:
        lv = getattr(obj, "level", None)
        return lv if isinstance(lv, str) else None
    except Exception:
        return None


@dataclass
class DOSBuilder:
    """UnifiedRisk V12 · DOS (Daily Opportunity Signal) Builder

    Frozen contract (DOS_V1):
    - Non-decisive: does NOT change Gate/DRS/Execution; only provides execution-permission hints.
    - Stored at slots['governance']['dos'].

    Design intent:
    - Avoid "always defensive" guidance in rising-index regimes.
    - Allow BASE participation (index ETF) and PULLBACK-only adds for high-beta satellites.
    - Forbid chase adds and leverage adds.
    """

    schema_version: str = "DOS_V1"

    def build(self, *, slots: Dict[str, Any], asof: str) -> Dict[str, Any]:
        warnings: List[str] = []
        reasons: List[str] = []
        constraints: List[str] = []
        evidence: Dict[str, Any] = {}

        gov = slots.get("governance") if isinstance(slots, dict) else None

        # --- trend_in_force ---
        structure = slots.get("structure") if isinstance(slots, dict) else None
        trend_state = None
        if isinstance(structure, dict):
            tif = structure.get("trend_in_force")
            if isinstance(tif, dict):
                ts = tif.get("state")
                if isinstance(ts, str) and ts:
                    trend_state = ts.strip().upper()
        if trend_state is None and isinstance(gov, dict):
            # some pipelines mirror structure facts into governance
            tif = gov.get("trend_in_force")
            if isinstance(tif, dict):
                ts = tif.get("state")
                if isinstance(ts, str) and ts:
                    trend_state = ts.strip().upper()

        # --- drs ---
        drs_signal = None
        drs_slot = slots.get("drs") if isinstance(slots, dict) else None
        if isinstance(drs_slot, dict):
            s = drs_slot.get("signal") or drs_slot.get("level")
            if isinstance(s, str) and s:
                drs_signal = s.strip().upper()
        if drs_signal is None and isinstance(gov, dict):
            d = gov.get("drs")
            if isinstance(d, dict):
                s = d.get("signal") or d.get("level")
                if isinstance(s, str) and s:
                    drs_signal = s.strip().upper()

        # --- execution band ---
        execution_band = None
        ex = slots.get("execution_summary") if isinstance(slots, dict) else None
        if isinstance(ex, dict):
            b = ex.get("band") or ex.get("code")
            if isinstance(b, str) and b:
                execution_band = b.strip().upper()
        if execution_band is None and isinstance(gov, dict):
            exg = gov.get("execution")
            if isinstance(exg, dict):
                b = exg.get("band") or exg.get("code")
                if isinstance(b, str) and b:
                    execution_band = b.strip().upper()

        # --- adv_ratio (0~1) ---
        adv_ratio = None
        # try common places (breadth / participation raw)
        for path in [
            ("breadth_plus_raw", "adv_ratio"),
            ("breadth_plus_raw", "adv_ratio_pct"),
            ("participation_raw", "adv_ratio"),
            ("participation_raw", "adv_ratio_pct"),
            ("market_sentiment_raw", "adv_ratio"),
            ("market_sentiment_raw", "adv_ratio_pct"),
            ("market_overview_raw", "adv_ratio"),
            ("market_overview_raw", "adv_ratio_pct"),
        ]:
            v = _get_nested(slots, path) if isinstance(slots, dict) else None
            f = _as_float(v)
            if f is not None:
                # pct -> ratio
                adv_ratio = f / 100.0 if f > 1.5 else f
                break

        # try factor details (participation)
        if adv_ratio is None and isinstance(slots, dict):
            factors = slots.get("factors")
            if isinstance(factors, dict):
                fr = factors.get("participation") or factors.get("market_sentiment")
                det = _get_details(fr)
                if isinstance(det, dict):
                    f = _as_float(det.get("adv_ratio") or det.get("adv_ratio_pct"))
                    if f is not None:
                        adv_ratio = f / 100.0 if f > 1.5 else f

        # --- top20_ratio (0~1) ---
        top20_ratio = None
        top20_series_3d: Optional[List[Any]] = None

        # prefer liquidity_quality
        candidates = [
            ("liquidity_quality_raw", "details", "top20_ratio"),
            ("liquidity_quality_raw", "top20_ratio"),
            ("liquidity_quality", "details", "top20_ratio"),
            ("liquidity_quality", "top20_ratio"),
            ("market_overview_raw", "details", "top20_ratio"),
            ("market_overview_raw", "top20_ratio"),
        ]
        for path in candidates:
            v = _get_nested(slots, path) if isinstance(slots, dict) else None
            f = _as_float(v)
            if f is not None:
                top20_ratio = f / 100.0 if f > 1.5 else f
                break

        # factor details fallback
        if top20_ratio is None and isinstance(slots, dict):
            factors = slots.get("factors")
            if isinstance(factors, dict):
                fr = factors.get("liquidity_quality")
                det = _get_details(fr)
                if isinstance(det, dict):
                    f = _as_float(det.get("top20_ratio"))
                    if f is not None:
                        top20_ratio = f / 100.0 if f > 1.5 else f
                    # capture 3d series if present
                    ts = det.get("top20_ratio_3d") or det.get("top20_ratio_series_3d") or det.get("top20_ratio_series")
                    if isinstance(ts, list):
                        top20_series_3d = ts[:3]

        # raw details 3d series
        if top20_series_3d is None and isinstance(slots, dict):
            lq = slots.get("liquidity_quality_raw")
            if isinstance(lq, dict):
                det = lq.get("details")
                if isinstance(det, dict):
                    ts = det.get("top20_ratio_3d") or det.get("top20_ratio_series_3d") or det.get("top20_ratio_series")
                    if isinstance(ts, list):
                        top20_series_3d = ts[:3]

        # evidence
        evidence["trend_in_force"] = trend_state
        evidence["drs"] = drs_signal
        evidence["execution"] = execution_band
        evidence["adv_ratio"] = adv_ratio
        evidence["top20_ratio"] = top20_ratio
        if top20_series_3d is not None:
            evidence["top20_ratio_3d"] = top20_series_3d

        # mark missing
        if trend_state is None:
            warnings.append("missing:trend_in_force.state")
        if drs_signal is None:
            warnings.append("missing:drs.signal")
        if execution_band is None:
            warnings.append("missing:execution.band")
        if adv_ratio is None:
            warnings.append("missing:adv_ratio")
        if top20_ratio is None:
            warnings.append("missing:top20_ratio")

        # --- DOS decision ---
        # Default: conservative
        level = "WEAK"
        mode = "BASE_INDEX"
        allowed: List[str] = ["HOLD", "TRIM_ON_STRENGTH"]
        forbidden: List[str] = ["CHASE_ADD", "LEVER_ADD"]

        # Hard veto
        hard_off = False
        if isinstance(drs_signal, str) and drs_signal == "RED":
            hard_off = True
            reasons.append("veto:drs_red")
        if isinstance(execution_band, str) and execution_band == "D3":
            hard_off = True
            reasons.append("veto:execution_d3")
        if isinstance(trend_state, str) and trend_state in ("BROKEN", "FAIL", "FAILED"):
            hard_off = True
            reasons.append("veto:trend_broken")

        if hard_off:
            level = "OFF"
            mode = "BASE_INDEX"
            allowed = ["HOLD", "TRIM_ON_STRENGTH"]
            constraints.append("No add-risk actions while veto active.")
        else:
            # Determine trend strength
            trend_ok = isinstance(trend_state, str) and trend_state in ("OK", "NORMAL", "UP", "IN_FORCE")
            trend_weak = isinstance(trend_state, str) and trend_state in ("WEAK", "WEAKENING", "CAUTION", "SOFT")
            # If trend missing but not veto, keep weak
            if trend_ok:
                # breadth check
                adv_ok = (adv_ratio is None) or (adv_ratio >= 0.45)
                if not adv_ok:
                    reasons.append("breadth:adv_ratio_weak")
                # concentration check
                concentrated = False
                if top20_ratio is not None and top20_ratio >= 0.12:
                    concentrated = True
                    reasons.append("liquidity:top20_ratio_high")
                if adv_ok and not concentrated and (execution_band in (None, "N", "D1", "D2")):
                    level = "ON"
                    mode = "MIX"
                    allowed = ["BASE_ETF_ADD", "PULLBACK_ADD", "HOLD", "TRIM_ON_STRENGTH"]
                    constraints.extend([
                        "BASE_ETF_ADD: ladder (2-3 steps), no chase.",
                        "PULLBACK_ADD: only after pullback confirmation (MA10/MA20 reclaim + volume shrink).",
                    ])
                    reasons.append("trend_ok")
                else:
                    level = "WEAK"
                    mode = "BASE_INDEX"
                    allowed = ["BASE_ETF_ADD", "HOLD", "TRIM_ON_STRENGTH"]
                    constraints.extend([
                        "BASE_ETF_ADD only (small steps); forbid chase adds.",
                    ])
                    if concentrated:
                        constraints.append("High concentration regime: prefer base only; satellites no add.")
            elif trend_weak:
                level = "WEAK"
                mode = "BASE_INDEX"
                allowed = ["BASE_ETF_ADD", "HOLD", "TRIM_ON_STRENGTH"]
                constraints.extend([
                    "Trend weakening: base participation only (small steps); satellites hold/trim.",
                ])
                reasons.append("trend_weakening")
            else:
                # trend unknown
                level = "WEAK"
                mode = "BASE_INDEX"
                allowed = ["BASE_ETF_ADD", "HOLD", "TRIM_ON_STRENGTH"] if (drs_signal != "RED" and execution_band != "D3") else ["HOLD"]
                constraints.append("Trend state unavailable: operate conservatively; no chase adds.")
                reasons.append("trend_unknown")

        # Ensure forbidden chase add always
        if "CHASE_ADD" not in forbidden:
            forbidden.append("CHASE_ADD")
        if "LEVER_ADD" not in forbidden:
            forbidden.append("LEVER_ADD")

        payload: Dict[str, Any] = {
            "schema_version": self.schema_version,
            "level": level,
            "mode": mode,
            "allowed": allowed,
            "forbidden": forbidden,
            "constraints": constraints,
            "reasons": reasons or ["dos_default"],
            "warnings": warnings,
            "evidence": evidence,
            "meta": {"asof": asof},
        }

        LOG.info("[DOS] asof=%s level=%s mode=%s adv=%.3f top20=%.3f drs=%s exec=%s trend=%s",
                 asof,
                 payload.get("level"),
                 payload.get("mode"),
                 adv_ratio if isinstance(adv_ratio, (int, float)) else -1.0,
                 top20_ratio if isinstance(top20_ratio, (int, float)) else -1.0,
                 drs_signal,
                 execution_band,
                 trend_state)

        return payload
