from __future__ import annotations

import logging
from typing import Any, Dict, List, Tuple, Optional, Literal

from core.factors.factor_result import FactorResult
from core.regime.observation.observation_base import ObservationBase, ObservationMeta, RiskLevel
from core.regime.observation.watchlist.coverage_loader import WatchlistCoverageLoader

LOG = logging.getLogger(__name__)

StructureState = Literal["healthy", "neutral", "weakening", "damaged"]
TrendState = Literal["up", "sideways", "down", "unknown"]
Direction = Literal["inflow", "outflow", "flat"]
Intensity = Literal["weak", "medium", "strong"]
TempTrend = Literal["up", "flat", "down", "unknown"]
Consistency = Literal["aligned", "mixed", "reversed", "unknown"]
RotationState = Literal["leading", "improving", "lagging", "fading", "unknown"]


class WatchlistSectorsObservation(ObservationBase):
    """
    profile=sector 的 Observation 实例
    说明：
    - 当前版本以“已存在的市场级因子”提供最小可运行输出；
    - 若未来引入板块级因子（sector_flow/sector_momentum），可在此处无痛增强；
    - 不做推荐，不输出行动。
    """

    def __init__(self):
        self._coverage = WatchlistCoverageLoader().load()

    @property
    def meta(self) -> ObservationMeta:
        return ObservationMeta(
            kind="watchlist.sectors",
            profile="sector",
            phase="Phase-2",
            asof="unknown",  # build() 时覆盖
            inputs=[
                "breadth",
                "index_tech",
                "north_nps",
                "amount",
                "margin",
                "sector_rotation",
            ],
            coverage_source=self._coverage.source,
        )

    def build(self, *, inputs: Dict[str, Any], asof: str) -> Dict[str, Any]:
        meta = self.meta
        meta = ObservationMeta(
            kind=meta.kind,
            profile=meta.profile,
            phase=meta.phase,
            asof=asof,
            inputs=meta.inputs,
            coverage_source=meta.coverage_source,
            note=meta.note,
        )

        # Phase-2 slots（只读）
        slots: Dict[str, FactorResult] = {}
        for k, v in (inputs or {}).items():
            if isinstance(v, FactorResult):
                slots[k] = v

        breadth = slots.get("breadth")
        index_tech = slots.get("index_tech")
        north_nps = slots.get("north_nps")
        amount = slots.get("amount")
        margin = slots.get("margin")
        sector_rotation = slots.get("sector_rotation")

        evidence = {
            "breadth": self._summarize_factor(breadth),
            "index_tech": self._summarize_factor(index_tech),
            "north_nps": self._summarize_factor(north_nps),
            "amount": self._summarize_factor(amount),
            "margin": self._summarize_factor(margin),
            "sector_rotation": self._summarize_factor(sector_rotation),
        }

        sectors_out: Dict[str, Any] = {}
        for cat, sector_list in self._coverage.groups.items():
            for sector in sector_list:
                # 注意：当前缺少“板块级专用因子”，因此使用 market-level slot 形成最小可运行输出，
                # 并在 comment 中明确“板块级数据未接入”。
                structure_state = self._structure_state(breadth=breadth, index_tech=index_tech)
                trend_state = self._trend_state(index_tech=index_tech)
                flow_nb = self._northbound_state(north_nps=north_nps)
                flow_to = self._amount_state(amount=amount)
                flow_mg = self._margin_state(margin=margin)

                mom3, mom5, cons = self._momentum_state(index_tech=index_tech)
                rot_state = self._rotation_state(sector_rotation=sector_rotation)

                risk_level = self._risk_level(structure_state=structure_state, rot_state=rot_state, nb_dir=flow_nb[0])

                comment = self._comment(
                    structure_state=structure_state,
                    trend_state=trend_state,
                    nb_dir=flow_nb[0],
                    nb_int=flow_nb[1],
                    rot_state=rot_state,
                    has_sector_specific=False,
                )

                sectors_out[sector] = {
                    "category": cat,
                    "structure": {
                        "state": structure_state,
                        "breadth": {"state": self._safe_detail_state(breadth), "evidence": "breadth"},
                        "trend": {"state": trend_state, "evidence": "index_tech"},
                    },
                    "flow": {
                        "northbound": {
                            "direction": flow_nb[0],
                            "intensity": flow_nb[1],
                            "evidence": "north_nps",
                        },
                        "amount": {"state": flow_to, "evidence": "amount"},
                        "margin": {"state": flow_mg, "evidence": "margin"},
                    },
                    "momentum": {
                        "trend_3d": mom3,
                        "trend_5d": mom5,
                        "consistency": cons,
                    },
                    "rotation": {
                        "state": rot_state,
                        "evidence": "sector_rotation",
                    },
                    "observation": {
                        "state": structure_state,
                        "risk_level": risk_level,
                        "comment": comment,
                    },
                }

        out = {
            "meta": {
                "kind": meta.kind,
                "profile": meta.profile,
                "phase": meta.phase,
                "asof": meta.asof,
                "inputs": meta.inputs,
                "coverage_source": meta.coverage_source,
                "note": meta.note,
            },
            "evidence": evidence,
            "sectors": sectors_out,
        }

        self.validate(observation=out)
        LOG.info("[WatchlistSectorsObservation] built sectors=%d asof=%s", len(sectors_out), asof)
        return out

    # -------------------------
    # helpers (frozen defaults)
    # -------------------------
    def _summarize_factor(self, fr: FactorResult | None) -> str:
        if fr is None:
            return "NA"
        return f"name={fr.name},score={fr.score:.2f},lv={fr.level}"

    def _safe_detail_state(self, fr: FactorResult | None) -> str:
        if fr is None:
            return "unknown"
        st = fr.details.get("state")
        if isinstance(st, str) and st.strip():
            return st.strip()
        return "unknown"

    def _structure_state(self, *, breadth: FactorResult | None, index_tech: FactorResult | None) -> StructureState:
        # 优先使用 details.state；否则使用 score 区间映射（冻结阈值）
        b = (breadth.score if breadth else 50.0)
        t = (index_tech.score if index_tech else 50.0)

        # breadth 低是结构损伤更关键
        if b < 40.0:
            return "damaged"
        if b < 48.0 or t < 45.0:
            return "weakening"
        if b > 58.0 and t > 55.0:
            return "healthy"
        return "neutral"

    def _trend_state(self, *, index_tech: FactorResult | None) -> TrendState:
        if index_tech is None:
            return "unknown"
        st = index_tech.details.get("trend_state")
        if isinstance(st, str) and st in ("up", "sideways", "down"):
            return st
        s = index_tech.score
        if s >= 58.0:
            return "up"
        if s <= 42.0:
            return "down"
        return "sideways"

    def _northbound_state(self, *, north_nps: FactorResult | None) -> Tuple[Direction, Intensity]:
        if north_nps is None:
            return ("flat", "weak")
        s = north_nps.score
        # 冻结默认阈值（可未来配置化）
        if s >= 58.0:
            return ("inflow", "strong")
        if s >= 53.0:
            return ("inflow", "medium")
        if s <= 42.0:
            return ("outflow", "strong")
        if s <= 47.0:
            return ("outflow", "medium")
        return ("flat", "weak")

    def _amount_state(self, *, amount: FactorResult | None) -> str:
        if amount is None:
            return "unknown"
        s = amount.score
        if s >= 60.0:
            return "hot"
        if s <= 40.0:
            return "cold"
        return "normal"

    def _margin_state(self, *, margin: FactorResult | None) -> str:
        if margin is None:
            return "NA"
        s = margin.score
        if s >= 58.0:
            return "expanding"
        if s <= 42.0:
            return "contracting"
        return "neutral"

    def _momentum_state(self, *, index_tech: FactorResult | None) -> Tuple[TempTrend, TempTrend, Consistency]:
        if index_tech is None:
            return ("unknown", "unknown", "unknown")
        t3 = index_tech.details.get("trend_3d")
        t5 = index_tech.details.get("trend_5d")
        if t3 not in ("up", "flat", "down"):
            t3 = "unknown"
        if t5 not in ("up", "flat", "down"):
            t5 = "unknown"

        if t3 == "unknown" or t5 == "unknown":
            return (t3, t5, "unknown")
        if t3 == t5:
            return (t3, t5, "aligned")
        # reversed: up vs down
        if (t3, t5) in (("up", "down"), ("down", "up")):
            return (t3, t5, "reversed")
        return (t3, t5, "mixed")

    def _rotation_state(self, *, sector_rotation: FactorResult | None) -> RotationState:
        # 当前 sector_rotation 因子未提供 leading/lagging 列表时，返回 unknown
        if sector_rotation is None:
            return "unknown"
        st = sector_rotation.details.get("rotation_state")
        if isinstance(st, str) and st in ("leading", "improving", "lagging", "fading"):
            return st
        return "unknown"

    def _risk_level(self, *, structure_state: StructureState, rot_state: RotationState, nb_dir: Direction) -> RiskLevel:
        if structure_state == "damaged":
            return "HIGH"
        if rot_state == "fading" and nb_dir == "outflow":
            return "HIGH"
        if structure_state == "healthy" and nb_dir == "inflow":
            return "LOW"
        return "NEUTRAL"

    def _comment(
        self,
        *,
        structure_state: StructureState,
        trend_state: TrendState,
        nb_dir: Direction,
        nb_int: Intensity,
        rot_state: RotationState,
        has_sector_specific: bool,
    ) -> str:
        base = f"结构={structure_state},趋势={trend_state},北向={nb_dir}/{nb_int},轮动={rot_state}"
        if not has_sector_specific:
            return base + "；板块级资金/主力/3D-5D明细尚未接入（当前为市场级最小观测输出）"
        return base
