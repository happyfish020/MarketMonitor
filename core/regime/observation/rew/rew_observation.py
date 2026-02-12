# -*- coding: utf-8 -*-
"""Regime Early Warning (REW) Observation

UnifiedRisk V12 · Phase-2 Observation（冻结）

性质：
- 解释性 Observation（leading warning）
- 只读 Phase-2 输出（structure + factors）
- 不构成预测/推荐/行动指令
- 不参与 GateDecision / DRS / 全市场评分

输入（best-effort，允许缺失）：
- inputs["structure"]: StructureFactsBuilder 输出 dict
- inputs["factors"]  : FactorResult dict（原始对象或 dict），用于读取 participation 等未进入 structure 的因子

输出（供 Quickcard/报告读取的稳定字段）：
- observation.level: GREEN | YELLOW | ORANGE | RED
- observation.scope: LOCAL | GLOBAL
- observation.reasons: List[str]

注意：
- 缺数据 ≠ 错误：输出 GREEN/MISSING 语义并带 warnings/evidence
- 冻结：永不抛异常
"""

from __future__ import annotations

from dataclasses import asdict
from typing import Any, Dict, List, Optional, Tuple

from core.regime.observation.observation_base import ObservationBase, ObservationMeta


class REWObservation(ObservationBase):
    """Regime Early Warning Observation."""

    @property
    def meta(self) -> ObservationMeta:
        return ObservationMeta(
            kind="regime_early_warning",
            profile="index",
            phase="P2",
            asof="EOD",
            inputs=[
                "structure.breadth",
                "structure.failure_rate",
                "structure.trend_in_force",
                "structure.amount",
                "structure.crowding_concentration",
                "structure.north_proxy_pressure",
                "structure.index_tech",
                "factors.participation",
            ],
            note=(
                "REW 为领先预警：用于提示未来 1–3 天风险偏好可能收缩的概率上升。"
                "Observation-only，不参与 Gate/DRS/评分。"
            ),
        )

    # -----------------------------
    # Public
    # -----------------------------
    def build(self, *, inputs: Dict[str, Any], asof: str) -> Dict[str, Any]:
        structure = inputs.get("structure") if isinstance(inputs, dict) else None
        factors = inputs.get("factors") if isinstance(inputs, dict) else None

        st = structure if isinstance(structure, dict) else {}
        fx = factors if isinstance(factors, dict) else {}

        warnings: List[str] = []

        # ---- structure states ----
        breadth_state = _safe_state(st.get("breadth"))
        frf_state = _safe_state(st.get("failure_rate")) or _safe_state(st.get("frf"))
        trend_state = _safe_state(st.get("trend_in_force"))
        amount_state = _safe_state(st.get("amount"))
        crowd_state = _safe_state(st.get("crowding_concentration"))
        north_state = _safe_state(st.get("north_proxy_pressure"))
        index_state = _safe_state(st.get("index_tech"))

        # ---- participation factor (not in structure by default) ----
        part_state = self._safe_factor_state(fx.get("participation"))

        # evidence snapshot (for audit)
        evidence: Dict[str, Any] = {
            "asof": asof,
            "breadth_state": breadth_state,
            "failure_rate_state": frf_state,
            "trend_in_force_state": trend_state,
            "amount_state": amount_state,
            "crowding_state": crowd_state,
            "north_proxy_pressure_state": north_state,
            "index_tech_state": index_state,
            "participation_state": part_state,
        }

        # missing flags
        if breadth_state is None:
            warnings.append("missing:structure.breadth")
        if frf_state is None:
            warnings.append("missing:structure.failure_rate")
        if trend_state is None:
            warnings.append("missing:structure.trend_in_force")
        if part_state is None:
            warnings.append("missing:factors.participation")

        # -----------------------------
        # Frozen decision (minimal MVP)
        # -----------------------------
        level, scope, reasons = self._decide_level(
            breadth_state=breadth_state,
            frf_state=frf_state,
            trend_state=trend_state,
            amount_state=amount_state,
            crowd_state=crowd_state,
            north_state=north_state,
            index_state=index_state,
            part_state=part_state,
        )

        observation = {
            "level": level,
            "scope": scope,
            "reasons": reasons,
        }

        result = {
            "meta": asdict(self.meta),
            "evidence": evidence,
            "warnings": warnings,
            "observation": observation,
        }

        self.validate(observation=result)
        return result

    # -----------------------------
    # Helpers
    # -----------------------------
    @staticmethod
    def _safe_factor_state(fr: Any) -> Optional[str]:
        """Extract a 'state' string from FactorResult.details.state."""
        try:
            if fr is None:
                return None
            details = fr.get("details") if isinstance(fr, dict) else getattr(fr, "details", None)
            if not isinstance(details, dict):
                return None
            v = details.get("state")
            if isinstance(v, str) and v.strip():
                return v.strip()
            return None
        except Exception:
            return None

    @staticmethod
    def _decide_level(
        *,
        breadth_state: Optional[str],
        frf_state: Optional[str],
        trend_state: Optional[str],
        amount_state: Optional[str],
        crowd_state: Optional[str],
        north_state: Optional[str],
        index_state: Optional[str],
        part_state: Optional[str],
    ) -> Tuple[str, str, List[str]]:
        """Return (level, scope, reasons).

        Frozen MVP rules:
        - RED: structural break (trend broken) OR (breadth damaged + frf elevated)
        - ORANGE: elevated risk signals or broad hidden weakness
        - YELLOW: early cracks (watch / narrow leadership / high crowding)
        - GREEN: none of the above (or insufficient evidence)

        Scope:
        - MVP default LOCAL (future: GLOBAL if overseas shock / macro stress)
        """

        reasons: List[str] = []

        # normalize
        bs = (breadth_state or "").lower()
        fs = (frf_state or "").lower()
        ts = (trend_state or "").lower()
        am = (amount_state or "").lower()
        cr = (crowd_state or "").lower()
        ns = (north_state or "").lower()
        ix = (index_state or "").lower()
        ps = (part_state or "").strip().lower().replace(" ", "_")

        # hard RED conditions
        if ts == "broken":
            reasons.append("trend_in_force:broken")
            return "RED", "LOCAL", reasons

        if bs == "damaged" and fs == "elevated_risk":
            reasons.append("breadth:damaged")
            reasons.append("failure_rate:elevated_risk")
            return "RED", "LOCAL", reasons

        # ORANGE conditions
        orange_hits = 0
        if fs == "elevated_risk":
            orange_hits += 1
            reasons.append("failure_rate:elevated_risk")
        if bs == "damaged":
            orange_hits += 1
            reasons.append("breadth:damaged")
        if ps in {"hidden_weakness", "broad_down"}:
            orange_hits += 1
            reasons.append(f"participation:{ps}")
        if ns == "pressure_high":
            orange_hits += 1
            reasons.append("north_proxy_pressure:high")
        if ix == "weak":
            orange_hits += 1
            reasons.append("index_tech:weak")

        if orange_hits >= 2:
            return "ORANGE", "LOCAL", reasons

        # YELLOW conditions
        if fs == "watch":
            reasons.append("failure_rate:watch")
        if ps == "narrow_leadership":
            reasons.append("participation:narrow_leadership")
        if cr == "high":
            reasons.append("crowding:high")
        if am == "contracting":
            reasons.append("amount:contracting")
        if ns == "pressure_medium":
            reasons.append("north_proxy_pressure:medium")
        if ts == "weakening":
            reasons.append("trend_in_force:weakening")

        if reasons:
            # if we had only a single ORANGE hit earlier, keep ORANGE if it is strong
            if orange_hits == 1:
                return "ORANGE", "LOCAL", reasons
            return "YELLOW", "LOCAL", reasons

        return "GREEN", "LOCAL", []


def _safe_state(node: Any) -> Optional[str]:
    """Extract node['state'] from a structure fact."""
    if not isinstance(node, dict):
        return None
    v = node.get("state")
    if isinstance(v, str) and v.strip():
        return v.strip()
    return None
