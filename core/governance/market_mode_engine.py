#-*- coding: utf-8 -*-
"""
UnifiedRisk V12 · MarketModeEngine (Unified Market Mode) · MM_V1

Frozen Spec (2026-02-15):
- Output governance.market_mode as the single "制度状态" truth layer.
- Priority: DRS > Trend > Execution > Gate > Rotation > Attack
- Does NOT change ActionHint / Gate / DRS / Execution (observe & report only at Phase-1).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from core.utils.logger import get_logger

LOG = get_logger("Governance.MarketMode")


@dataclass(frozen=True)
class MarketModeResult:
    schema_version: str
    asof: str
    mode: str
    severity: str
    inputs: Dict[str, Any]
    reasons: List[str]


class MarketModeEngine:
    SCHEMA = "MM_V1"

    @classmethod
    def evaluate(
        cls,
        *,
        slots: Dict[str, Any],
        asof: str,
        gate: Optional[str],
        execution_band: Optional[str],
        trend_state: Optional[str],
    ) -> Dict[str, Any]:
        gov = slots.get("governance") if isinstance(slots.get("governance"), dict) else {}
        drs_level = _pick_drs_level(gov, slots)
        failure_rate_level = _pick_failure_rate_level(slots)
        adv_ratio = _pick_adv_ratio(slots)
        attack_state = _pick_attack_window_state(slots)

        # helpers
        reasons: List[str] = []
        inputs = {
            "drs_level": drs_level,
            "gate": gate,
            "execution_band": execution_band,
            "trend_state": trend_state,
            "failure_rate_level": failure_rate_level,
            "adv_ratio": adv_ratio,
            "attack_window_state": attack_state,
        }

        # 1) DEFENSE_HIGH
        if str(drs_level or "").upper() == "RED":
            reasons.append("DRS=RED")
            return cls._pack(asof=asof, mode="DEFENSE_HIGH", severity="HIGH", inputs=inputs, reasons=reasons)

        if str(trend_state or "").upper() in ("BROKEN_ACTIVE",):
            reasons.append("Trend=BROKEN_ACTIVE")
            return cls._pack(asof=asof, mode="DEFENSE_HIGH", severity="HIGH", inputs=inputs, reasons=reasons)

        if str(failure_rate_level or "").upper() == "HIGH":
            reasons.append("FailureRate=HIGH")
            return cls._pack(asof=asof, mode="DEFENSE_HIGH", severity="HIGH", inputs=inputs, reasons=reasons)

        # 2) DEFENSE
        if str(drs_level or "").upper() == "ORANGE":
            reasons.append("DRS=ORANGE")
            return cls._pack(asof=asof, mode="DEFENSE", severity="MEDIUM", inputs=inputs, reasons=reasons)

        if str(execution_band or "").upper() == "D3":
            reasons.append("Execution=D3")
            return cls._pack(asof=asof, mode="DEFENSE", severity="MEDIUM", inputs=inputs, reasons=reasons)

        try:
            if adv_ratio is not None and float(adv_ratio) < 0.35:
                reasons.append("adv_ratio<0.35")
                return cls._pack(asof=asof, mode="DEFENSE", severity="MEDIUM", inputs=inputs, reasons=reasons)
        except Exception:
            pass

        # 3) REPAIR (best-effort: requires DRS transition + lead + breadth improve)
        if cls._is_repair(slots=slots, drs_level=drs_level):
            reasons.append("REPAIR: DRS RED→YELLOW + Lead>=ORANGE + breadth_improve>=2")
            return cls._pack(asof=asof, mode="REPAIR", severity="LOW", inputs=inputs, reasons=reasons)

        # 4) ATTACK_PREP
        if str(attack_state or "").upper() == "VERIFY_ONLY" and str(drs_level or "").upper() != "RED":
            reasons.append("AttackWindow=VERIFY_ONLY")
            return cls._pack(asof=asof, mode="ATTACK_PREP", severity="LOW", inputs=inputs, reasons=reasons)

        # 5) ATTACK
        if cls._is_attack(gate=gate, drs_level=drs_level, trend_state=trend_state):
            reasons.append("Gate=NORMAL + DRS=GREEN + Trend=IN_FORCE")
            return cls._pack(asof=asof, mode="ATTACK", severity="LOW", inputs=inputs, reasons=reasons)

        # 6) default STABLE
        reasons.append("DEFAULT")
        return cls._pack(asof=asof, mode="STABLE", severity="LOW", inputs=inputs, reasons=reasons)

    @classmethod
    def _pack(cls, *, asof: str, mode: str, severity: str, inputs: Dict[str, Any], reasons: List[str]) -> Dict[str, Any]:
        obj = {
            "schema_version": cls.SCHEMA,
            "asof": asof,
            "mode": mode,
            "severity": severity,
            "inputs": inputs,
            "reasons": reasons,
        }
        try:
            LOG.info("[MarketMode] asof=%s mode=%s severity=%s inputs=%s reasons=%s", asof, mode, severity, inputs, reasons)
        except Exception:
            pass
        return obj

    @staticmethod
    def _is_attack(*, gate: Optional[str], drs_level: Optional[str], trend_state: Optional[str]) -> bool:
        g = str(gate or "").upper()
        d = str(drs_level or "").upper()
        t = str(trend_state or "").upper()
        if g in ("NORMAL", "ALLOW", "OPEN") and d == "GREEN" and t in ("IN_FORCE", "OK", "UP"):
            return True
        return False

    @staticmethod
    def _is_repair(*, slots: Dict[str, Any], drs_level: Optional[str]) -> bool:
        if str(drs_level or "").upper() != "YELLOW":
            return False
        # DRS transition (best-effort)
        prev = None
        obs = slots.get("observations") if isinstance(slots.get("observations"), dict) else {}
        drs_obs = obs.get("drs") if isinstance(obs.get("drs"), dict) else {}
        cont = drs_obs.get("continuity") if isinstance(drs_obs.get("continuity"), dict) else {}
        prev = cont.get("prev_signal") or cont.get("prev_level") or cont.get("prev")
        if str(prev or "").upper() != "RED":
            return False

        lead_level = _pick_lead_level(slots)
        if _level_rank(lead_level) < _level_rank("ORANGE"):
            return False

        breadth_improve = _pick_breadth_improve_days(slots)
        try:
            if int(breadth_improve or 0) < 2:
                return False
        except Exception:
            return False
        return True


def _pick_drs_level(gov: Dict[str, Any], slots: Dict[str, Any]) -> Optional[str]:
    gd = gov.get("drs") if isinstance(gov.get("drs"), dict) else {}
    val = gd.get("level") or gd.get("drs_level") or gd.get("signal")
    if isinstance(val, str) and val.strip():
        return val.strip()
    # fallback: slots['drs']['signal']
    d = slots.get("drs")
    if isinstance(d, dict):
        v = d.get("signal") or d.get("level")
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _pick_failure_rate_level(slots: Dict[str, Any]) -> Optional[str]:
    fr = slots.get("failure_rate")
    if isinstance(fr, dict):
        v = fr.get("level") or fr.get("state")
        if isinstance(v, str) and v.strip():
            return v.strip()
    # fallback: structure facts
    st = slots.get("structure") if isinstance(slots.get("structure"), dict) else {}
    fr2 = st.get("failure_rate")
    if isinstance(fr2, dict):
        v = fr2.get("level") or fr2.get("state")
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _pick_adv_ratio(slots: Dict[str, Any]) -> Optional[float]:
    def _norm(v: Any) -> Optional[float]:
        try:
            if v is None:
                return None
            f = float(v)
            return f / 100.0 if f > 1.5 else f
        except Exception:
            return None

    # 1) structure canonical path used by StructureFactsBuilder
    st = slots.get("structure") if isinstance(slots.get("structure"), dict) else {}
    cc = st.get("crowding_concentration") if isinstance(st.get("crowding_concentration"), dict) else {}
    ev = cc.get("evidence") if isinstance(cc.get("evidence"), dict) else {}
    v = _norm(ev.get("adv_ratio"))
    if v is not None:
        return v

    # 2) factors.participation.details
    factors = slots.get("factors") if isinstance(slots.get("factors"), dict) else {}
    p_fac = factors.get("participation") if isinstance(factors.get("participation"), dict) else {}
    p_det = p_fac.get("details") if isinstance(p_fac.get("details"), dict) else {}
    v = _norm(p_det.get("adv_ratio") if p_det else None)
    if v is not None:
        return v

    # 3) top-level participation fallback
    p = slots.get("participation")
    if isinstance(p, dict):
        v = _norm(p.get("adv_ratio"))
        if v is not None:
            return v
    return None


def _pick_attack_window_state(slots: Dict[str, Any]) -> Optional[str]:
    aw = slots.get("attack_window")
    if isinstance(aw, dict):
        v = aw.get("state") or aw.get("attack_state")
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _pick_lead_level(slots: Dict[str, Any]) -> Optional[str]:
    wl = slots.get("watchlist_lead")
    if isinstance(wl, dict):
        v = wl.get("overall")
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _pick_breadth_improve_days(slots: Dict[str, Any]) -> Optional[int]:
    st = slots.get("structure") if isinstance(slots.get("structure"), dict) else {}
    # common keys: breadth / breadth_plus / new_lows
    for k in ("breadth", "breadth_plus", "new_lows", "breadth_facts"):
        obj = st.get(k)
        if isinstance(obj, dict):
            v = obj.get("improve_days") or obj.get("improvement_days")
            try:
                return int(v) if v is not None else None
            except Exception:
                continue
    return None


def _level_rank(level: Optional[str]) -> int:
    lv = str(level or "").upper()
    return {"MISSING": 0, "GREEN": 1, "YELLOW": 2, "ORANGE": 3, "RED": 4}.get(lv, 0)
