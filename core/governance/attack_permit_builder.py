# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from core.utils.logger import get_logger

LOG = get_logger("AttackPermit")


def _as_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        if isinstance(v, bool):
            return None
        return float(v)
    except Exception:
        return None


def _as_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        if isinstance(v, bool):
            return None
        return int(float(v))
    except Exception:
        return None


def _get_in(d: Any, path: List[str]) -> Any:
    cur = d
    for k in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def _get_block(slots: Dict[str, Any], key: str) -> Any:
    """Get block by key from slots; also supports slots['snapshot'][key]."""
    if not isinstance(slots, dict):
        return None
    v = slots.get(key)
    if v is not None:
        return v
    snap = slots.get("snapshot")
    if isinstance(snap, dict):
        return snap.get(key)
    return None


@dataclass
class AttackPermitConfig:
    """
    Frozen defaults (Route-A):
    - This layer does NOT relax Gate; it only grants explicit "attack permission" notes
      to ActionHint when conditions are met.
    """
    min_adv_ratio: float = 0.68  # adv_ratio >= 68% means breadth diffusion is decent
    max_top20_ratio: float = 0.12  # top20 concentration <= 12% indicates not overly concentrated
    min_pct_above_ma50: float = 55.0  # percent (0-100)
    max_new_low_ratio_pct: float = 1.2  # percent (0-100), 50D new lows / total


class AttackPermitBuilder:
    """
    Build slots['governance']['attack_permit']:

    {
      "schema_version": "ATTACK_PERMIT_V1_2026Q1",
      "asof": "YYYY-MM-DD",
      "permit": "YES|NO",
      "mode": "LIMITED|FULL|NONE",
      "label": "🟡 仅底仓参与（不追涨）" | "🟢 可进攻（分批）" | "⛔ 不可进攻（BLOCK）",
      "allowed": ["BASE_ETF_ADD", "PULLBACK_ADD", ...],
      "constraints": ["..."],
      "evidence": {"adv_ratio":..., "top20_ratio":..., "pct_above_ma50":..., "new_low_ratio_pct":...},
      "warnings": [...]
    }

    Notes:
    - Uses existing snapshot blocks: participation_raw / liquidity_quality_raw / breadth_plus_raw.
    - Never silently fabricates 0s: if missing -> add warnings and keep permit=NO(NONE).
    """
    schema_version = "ATTACK_PERMIT_V1_2026Q1"

    def __init__(self, cfg: Optional[AttackPermitConfig] = None) -> None:
        self.cfg = cfg or AttackPermitConfig()

    def build(self, *, slots: Dict[str, Any], asof: str, gate: str) -> Dict[str, Any]:
        warnings: List[str] = []
        evidence: Dict[str, Any] = {}

        # ----- adv_ratio -----
        # Prefer FactorResults (slots['factors']['participation'].details.adv_ratio)
        # Fallback to legacy raw blocks when present.
        adv_ratio, adv_src = self._pick_adv_ratio(slots)
        if adv_ratio is None:
            warnings.append("missing:adv_ratio")
        else:
            evidence["adv_ratio"] = round(float(adv_ratio), 6)
            evidence["adv_ratio_src"] = adv_src

        # ----- top20_ratio strict -----
        # MUST use liquidity_quality.details.top20_ratio (per project constraint).
        top20_ratio, top20_src = self._pick_top20_ratio(slots)
        if top20_ratio is None:
            warnings.append("missing:top20_ratio (strict:liquidity_quality)")
        else:
            evidence["top20_ratio"] = round(float(top20_ratio), 6)
            evidence["top20_ratio_src"] = top20_src

        # ----- pct_above_ma50 -----
        # Currently best-effort: may be missing in slots during report phase.
        pct_above_ma50, ma50_src = self._pick_pct_above_ma50(slots)
        if pct_above_ma50 is None:
            warnings.append("missing:pct_above_ma50")
        else:
            evidence["pct_above_ma50"] = round(float(pct_above_ma50), 4)
            evidence["pct_above_ma50_src"] = ma50_src

        # ----- new_low_ratio_pct -----
        # Prefer breadth factor when available; fallback to breadth_plus_raw evidence.
        new_low_ratio_pct, nl_src = self._pick_new_low_ratio_pct(slots)
        if new_low_ratio_pct is None:
            warnings.append("missing:new_low_ratio_pct")
        else:
            evidence["new_low_ratio_pct"] = round(float(new_low_ratio_pct), 4)
            evidence["new_low_ratio_pct_src"] = nl_src
            # Surface mismatch override explicitly for audit.
            if "mismatch_override" in str(nl_src):
                warnings.append("data_mismatch:new_low_ratio_pct -> override_to_breadth_plus")

        # Default: no attack permission
        permit = "NO"
        mode = "NONE"
        label = "⛔ 不可进攻（BLOCK）"
        allowed: List[str] = ["HOLD", "TRIM_ON_STRENGTH"]
        constraints: List[str] = []

        # Route-A logic: Gate remains authoritative; this layer only annotates permission.
        # - Gate=FREEZE => always block
        if str(gate).upper() == "FREEZE":
            constraints.append("gate=FREEZE -> block_all_attack")
        else:
            # Evaluate if we have enough data to allow LIMITED / FULL
            ok_adv = (adv_ratio is not None) and (adv_ratio >= self.cfg.min_adv_ratio)
            ok_top20 = (top20_ratio is not None) and (top20_ratio <= self.cfg.max_top20_ratio)
            ok_ma50 = (pct_above_ma50 is not None) and (pct_above_ma50 >= self.cfg.min_pct_above_ma50)
            ok_newlow = (new_low_ratio_pct is not None) and (new_low_ratio_pct <= self.cfg.max_new_low_ratio_pct)

            # LIMITED: require adv + top20 + (new_low ok if available). FULL: require ma50+new_low.
            # Policy: pct_above_ma50 may be missing in report phase; NEVER silently fabricate it.
            # - If pct_above_ma50 is missing but other key conditions are good, allow LIMITED with warnings.
            if ok_adv and ok_top20 and (ok_newlow or new_low_ratio_pct is None):
                permit = "YES"
                mode = "LIMITED"
                label = "🟡 仅底仓参与（不追涨）"
                allowed = ["HOLD", "TRIM_ON_STRENGTH", "BASE_ETF_ADD", "PULLBACK_ADD"]
                constraints.append("仅允许：BASE_ETF_ADD / PULLBACK_ADD(回撤确认)；禁止：CHASE_ADD / 杠杆。")

                # If breadth data is missing, explicitly cap to LIMITED (audit-friendly)
                if pct_above_ma50 is None:
                    warnings.append("cap:ma50_missing -> LIMITED")
                if new_low_ratio_pct is None:
                    warnings.append("cap:new_low_missing -> LIMITED")

                if ok_ma50 and ok_newlow:
                    mode = "FULL"
                    label = "🟢 可进攻（分批）"
                    allowed = ["HOLD", "TRIM_ON_STRENGTH", "BASE_ETF_ADD", "PULLBACK_ADD", "SATELLITE_ADD"]
                    constraints.append("条件满足：宽度强（%>MA50 & 新低比低），允许更积极但仍禁止追价。")

            # Conflict note: if DRS is RED but breadth looks healthy, cap to LIMITED (handled by ActionHint)
            drs = _get_in(slots, ["governance", "drs", "signal"])
            if permit == "YES" and isinstance(drs, str) and drs.upper() == "RED" and (ok_adv and ok_top20):
                warnings.append("conflict:drs_red_vs_breadth_health -> allow_base_only")

        payload = {
            "schema_version": self.schema_version,
            "asof": str(asof),
            "permit": permit,
            "mode": mode,
            "label": label,
            "allowed": allowed,
            "constraints": constraints,
            "evidence": evidence,
            "warnings": warnings,
        }
        LOG.info(
            "[AttackPermit] asof=%s gate=%s permit=%s mode=%s adv=%s top20=%s ma50=%s nl=%s",
            asof, gate, permit, mode, adv_ratio, top20_ratio, pct_above_ma50, new_low_ratio_pct
        )
        return payload

    def _pick_adv_ratio(self, slots: Dict[str, Any]) -> Tuple[Optional[float], str]:
        # Preferred: factors.participation.details.adv_ratio
        factors = slots.get("factors") if isinstance(slots, dict) else None
        if factors is None:
            snap = slots.get("snapshot") if isinstance(slots, dict) else None
            if isinstance(snap, dict):
                factors = snap.get("factors")
        if isinstance(factors, dict):
            p = factors.get("participation")
            if isinstance(p, dict):
                det = p.get("details")
                if isinstance(det, dict):
                    v = det.get("adv_ratio")
                    if v is None:
                        v = det.get("adv_ratio_pct")
                    f = _as_float(v)
                    if f is not None:
                        # Normalize: adv_ratio should be 0..1
                        if f > 1.5:
                            return f / 100.0, "factors.participation.details.adv_ratio_pct"
                        return f, "factors.participation.details.adv_ratio"

        pr = _get_block(slots, "participation_raw")
        if isinstance(pr, dict):
            v = pr.get("adv_ratio")
            f = _as_float(v)
            if f is not None:
                return f, "participation_raw.adv_ratio"
        # fallback: market_sentiment_raw may also carry adv_ratio_pct
        ms = slots.get("market_sentiment_raw")
        if isinstance(ms, dict):
            ev = ms.get("evidence") if isinstance(ms.get("evidence"), dict) else {}
            v = ev.get("adv_ratio") or ev.get("adv_ratio_pct")
            f = _as_float(v)
            if f is not None:
                # if percent
                if f > 1.5:
                    return f / 100.0, "market_sentiment_raw.evidence.adv_ratio_pct"
                return f, "market_sentiment_raw.evidence.adv_ratio"
        return None, "missing"

    def _pick_top20_ratio(self, slots: Dict[str, Any]) -> Tuple[Optional[float], str]:
        # Preferred: factors.liquidity_quality.details.top20_ratio
        factors = slots.get("factors") if isinstance(slots, dict) else None
        if factors is None:
            snap = slots.get("snapshot") if isinstance(slots, dict) else None
            if isinstance(snap, dict):
                factors = snap.get("factors")
        if isinstance(factors, dict):
            lqf = factors.get("liquidity_quality")
            if isinstance(lqf, dict):
                det = lqf.get("details")
                if isinstance(det, dict):
                    v = det.get("top20_ratio")
                    f = _as_float(v)
                    if f is not None:
                        # Normalize: top20_ratio should be 0..1
                        if f > 1.5:
                            return f / 100.0, "factors.liquidity_quality.details.top20_ratio_pct"
                        return f, "factors.liquidity_quality.details.top20_ratio"

        lq = _get_block(slots, "liquidity_quality_raw")
        if isinstance(lq, dict):
            ev = lq.get("evidence") if isinstance(lq.get("evidence"), dict) else {}
            v = ev.get("top20_ratio") or _get_in(lq, ["details", "top20_ratio"])
            f = _as_float(v)
            if f is not None:
                if f > 1.5:
                    return f / 100.0, "liquidity_quality_raw.evidence.top20_ratio_pct"
                return f, "liquidity_quality_raw.evidence.top20_ratio"
        return None, "missing"

    def _pick_pct_above_ma50(self, slots: Dict[str, Any]) -> Tuple[Optional[float], str]:
        # 1) Preferred: breadth_plus_raw.evidence
        bp = _get_block(slots, "breadth_plus_raw")
        if isinstance(bp, dict):
            ev = bp.get("evidence") if isinstance(bp.get("evidence"), dict) else {}
            v = ev.get("pct_above_ma50_pct")
            if v is None:
                v = ev.get("pct_above_ma50")
            f = _as_float(v)
            if f is not None:
                # keep as percent 0-100
                if f <= 1.5:
                    return f * 100.0, "breadth_plus_raw.evidence.pct_above_ma50 (ratio->pct)"
                return f, "breadth_plus_raw.evidence.pct_above_ma50"

        # 2) Fallback: watchlist_lead (can be either a block dict or a FactorResult-like dict)
        wl = _get_block(slots, "watchlist_lead")
        if isinstance(wl, dict):
            # --- legacy: wl.key_metrics ---
            km = wl.get("key_metrics") if isinstance(wl.get("key_metrics"), dict) else {}
            v = km.get("pct_above_ma50") or km.get("pct_above_ma50_pct") or km.get("pct_ma50") or km.get("above_ma50_pct")
            f = _as_float(v)
            if f is not None:
                if f <= 1.5:
                    return f * 100.0, "watchlist_lead.key_metrics.pct_above_ma50 (ratio->pct)"
                return f, "watchlist_lead.key_metrics.pct_above_ma50"

            # --- legacy: wl.lead_panels.breadth_plus.key_metrics ---
            lp = wl.get("lead_panels") if isinstance(wl.get("lead_panels"), dict) else {}
            bp2 = lp.get("breadth_plus") if isinstance(lp.get("breadth_plus"), dict) else {}
            km2 = bp2.get("key_metrics") if isinstance(bp2.get("key_metrics"), dict) else {}
            v2 = km2.get("pct_above_ma50") or km2.get("pct_above_ma50_pct") or km2.get("pct_ma50") or km2.get("above_ma50_pct")
            f2 = _as_float(v2)
            if f2 is not None:
                if f2 <= 1.5:
                    return f2 * 100.0, "watchlist_lead.lead_panels.breadth_plus.key_metrics.pct_above_ma50 (ratio->pct)"
                return f2, "watchlist_lead.lead_panels.breadth_plus.key_metrics.pct_above_ma50"

            # --- FactorResult-like: wl.details.lead_panels.breadth_plus.key_metrics ---
            det = wl.get("details") if isinstance(wl.get("details"), dict) else {}
            km3 = det.get("key_metrics") if isinstance(det.get("key_metrics"), dict) else {}
            v3 = km3.get("pct_above_ma50") or km3.get("pct_above_ma50_pct") or km3.get("pct_ma50") or km3.get("above_ma50_pct")
            f3 = _as_float(v3)
            if f3 is not None:
                if f3 <= 1.5:
                    return f3 * 100.0, "watchlist_lead.details.key_metrics.pct_above_ma50 (ratio->pct)"
                return f3, "watchlist_lead.details.key_metrics.pct_above_ma50"

            lp3 = det.get("lead_panels") if isinstance(det.get("lead_panels"), dict) else {}
            bp3 = lp3.get("breadth_plus") if isinstance(lp3.get("breadth_plus"), dict) else {}
            km4 = bp3.get("key_metrics") if isinstance(bp3.get("key_metrics"), dict) else {}
            v4 = km4.get("pct_above_ma50") or km4.get("pct_above_ma50_pct") or km4.get("pct_ma50") or km4.get("above_ma50_pct")
            f4 = _as_float(v4)
            if f4 is not None:
                if f4 <= 1.5:
                    return f4 * 100.0, "watchlist_lead.details.lead_panels.breadth_plus.key_metrics.pct_above_ma50 (ratio->pct)"
                return f4, "watchlist_lead.details.lead_panels.breadth_plus.key_metrics.pct_above_ma50"

            # last-ditch: some renderers put it directly under details
            v5 = det.get("pct_above_ma50") or det.get("pct_above_ma50_pct") or det.get("pct_ma50") or det.get("above_ma50_pct")
            f5 = _as_float(v5)
            if f5 is not None:
                if f5 <= 1.5:
                    return f5 * 100.0, "watchlist_lead.details.pct_above_ma50 (ratio->pct)"
                return f5, "watchlist_lead.details.pct_above_ma50"

        # 3) Fallback: factors.watchlist_lead.key_metrics (if produced separately)
        factors = slots.get("factors") if isinstance(slots, dict) else None
        if factors is None:
            snap = slots.get("snapshot") if isinstance(slots, dict) else None
            if isinstance(snap, dict):
                factors = snap.get("factors")
        if isinstance(factors, dict):
            wf = factors.get("watchlist_lead")
            if isinstance(wf, dict):
                kmf = wf.get("key_metrics") if isinstance(wf.get("key_metrics"), dict) else {}
                v6 = kmf.get("pct_above_ma50") or kmf.get("pct_above_ma50_pct") or kmf.get("pct_ma50") or kmf.get("above_ma50_pct")
                f6 = _as_float(v6)
                if f6 is not None:
                    if f6 <= 1.5:
                        return f6 * 100.0, "factors.watchlist_lead.key_metrics.pct_above_ma50 (ratio->pct)"
                    return f6, "factors.watchlist_lead.key_metrics.pct_above_ma50"

                detf = wf.get("details") if isinstance(wf.get("details"), dict) else {}
                lp_f = detf.get("lead_panels") if isinstance(detf.get("lead_panels"), dict) else {}
                bp_f = lp_f.get("breadth_plus") if isinstance(lp_f.get("breadth_plus"), dict) else {}
                km_f2 = bp_f.get("key_metrics") if isinstance(bp_f.get("key_metrics"), dict) else {}
                v7 = km_f2.get("pct_above_ma50") or km_f2.get("pct_above_ma50_pct") or km_f2.get("pct_ma50") or km_f2.get("above_ma50_pct")
                f7 = _as_float(v7)
                if f7 is not None:
                    if f7 <= 1.5:
                        return f7 * 100.0, "factors.watchlist_lead.details.lead_panels.breadth_plus.key_metrics.pct_above_ma50 (ratio->pct)"
                    return f7, "factors.watchlist_lead.details.lead_panels.breadth_plus.key_metrics.pct_above_ma50"

        return None, "missing"
    def _pick_new_low_ratio_pct(self, slots: Dict[str, Any]) -> Tuple[Optional[float], str]:
        # Preferred: normalized percent from slots['breadth']['new_low_ratio_pct'] (assembled from lead_panels)
        # Rationale: breadth_raw (older path) may be stale/mis-mapped; slots['breadth'] is closer to report contract.
        b = _get_block(slots, "breadth")
        if isinstance(b, dict):
            v0 = b.get("new_low_ratio_pct")
            f0 = _as_float(v0)
            if f0 is not None:
                # Heuristic normalization:
                # - expected unit is percent (0..100)
                # - if extremely small (<=0.02), treat as ratio and scale to percent
                if 0.0 <= f0 <= 0.02:
                    return f0 * 100.0, "slots.breadth.new_low_ratio_pct (ratio->pct)"
                if 0.0 <= f0 <= 100.0:
                    return f0, "slots.breadth.new_low_ratio_pct"

        # Next: breadth_plus_raw evidence may directly carry new_low_ratio_pct
        bp = _get_block(slots, "breadth_plus_raw")
        if isinstance(bp, dict):
            ev0 = bp.get("evidence") if isinstance(bp.get("evidence"), dict) else {}
            v1 = ev0.get("new_low_ratio_pct")
            f1 = _as_float(v1)
            if f1 is not None:
                if 0.0 <= f1 <= 0.02:
                    return f1 * 100.0, "breadth_plus_raw.evidence.new_low_ratio_pct (ratio->pct)"
                if 0.0 <= f1 <= 100.0:
                    return f1, "breadth_plus_raw.evidence.new_low_ratio_pct"

        # Fallback: factors.breadth.details.new_low_ratio
        factors = slots.get("factors") if isinstance(slots, dict) else None
        if factors is None:
            snap = slots.get("snapshot") if isinstance(slots, dict) else None
            if isinstance(snap, dict):
                factors = snap.get("factors")
        if isinstance(factors, dict):
            bf = factors.get("breadth")
            if isinstance(bf, dict):
                det = bf.get("details")
                if isinstance(det, dict):
                    v = det.get("new_low_ratio")
                    f = _as_float(v)
                    if f is not None:
                        # Normalize to percent
                        # - if value is 0..1, treat as ratio -> pct
                        # - if value is 1..100, treat as pct already
                        if f <= 1.0:
                            f_pct = f * 100.0
                        elif f <= 100.0:
                            f_pct = f
                        else:
                            f_pct = f / 100.0

                        # Mismatch guard: if we also have a breadth_plus pct and they disagree wildly,
                        # prefer breadth_plus to avoid false HARD veto (audit: record override src).
                        if isinstance(b, dict):
                            base = _as_float(b.get("new_low_ratio_pct"))
                            if base is not None:
                                # normalize base to percent using same heuristic
                                if 0.0 <= base <= 0.02:
                                    base_pct = base * 100.0
                                else:
                                    base_pct = base
                                if 0.0 <= base_pct <= 100.0:
                                    if abs(f_pct - base_pct) >= 1.0 and (max(f_pct, base_pct) / max(0.0001, min(f_pct, base_pct))) >= 5.0:
                                        return base_pct, "mismatch_override:slots.breadth.new_low_ratio_pct"

                        if f <= 1.0:
                            return f_pct, "factors.breadth.details.new_low_ratio (ratio->pct)"
                        if f <= 100.0:
                            return f_pct, "factors.breadth.details.new_low_ratio_pct"
                        # extremely large -> likely already *100; cap normalization
                        return f_pct, "factors.breadth.details.new_low_ratio (auto_scale/100)"

        # last-ditch: compute from counts if present
        bp2 = _get_block(slots, "breadth_plus_raw")
        if isinstance(bp2, dict):
            ev = bp2.get("evidence") if isinstance(bp2.get("evidence"), dict) else {}
            nl = _as_int(ev.get("new_low_50d") or ev.get("new_low_50"))
            total = _as_int(ev.get("total") or ev.get("count") or ev.get("coverage"))
            if nl is not None and total and total > 0:
                return (nl / float(total)) * 100.0, "breadth_plus_raw.evidence.new_low_50d/total"
        return None, "missing"
