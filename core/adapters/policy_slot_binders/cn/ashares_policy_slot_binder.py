from __future__ import annotations

import logging
from typing import Dict

from core.adapters.policy_slot_binders.binder_base import PolicySlotBinderBase
from core.factors.factor_result import FactorResult
from core.regime.observation.watchlist.watchlist_ob_builder import WatchlistObservationBuilder

LOG = logging.getLogger(__name__)


class ASharesPolicySlotBinder(PolicySlotBinderBase):
    """
    A 股制度槽位绑定（冻结）
    - 输入：Dict[str, FactorResult]（key = *_raw）
    - 输出：Dict[str, Any]（key = slot 名；值通常为 FactorResult，也可包含 Observation dict）
    - 只做绑定与 Phase-2 Observation 产物注入（watchlist）
    - 不修改 FactorResult（FactorResult frozen）
    """

    SLOT_MAP = {
        "breadth": "breadth",
        "north_nps": "north_nps",
        "turnover": "turnover",
        "unified_emotion": "unified_emotion",
        
        "sector_rotation": "sector_rotation",
        "global_macro": "global_macro",
        "global_lead": "global_lead",
        "index_global": "index_global",
        
        "index_tech": "index_tech",
        "failure_rate": "failure_rate", 
        "etf_index_sync_daily": "etf_index_sync_daily",
        # ----- structure-level (慎用) -----
        # 建议不要放 raw 映射；若保留请明确注释
        "trend_in_force": "trend_in_force",
        # 允许继续 append-only
    }

    def __init__(self):
        self._watchlist_builder = WatchlistObservationBuilder()

    def bind(self, factors: Dict[str, FactorResult]) -> Dict[str, object]:
        if not isinstance(factors, dict):
            raise TypeError("factors must be Dict[str, FactorResult]")

        bound: Dict[str, object] = {}

        # 1) raw -> slot
        for raw_name, fr in factors.items():
            print(raw_name)
            if not isinstance(raw_name, str):
                continue
            if not isinstance(fr, FactorResult):
                continue

            slot = self.bind_slot(raw_name, fr)
            print(raw_name)
            if slot is None:
                continue
            bound[slot] = fr

        # 2) inject Phase-2 Observation: watchlist.sectors
        # asof：尽量从任一因子的 details 中取 trade_date，否则使用 unknown
        asof = self._infer_asof(factors)
        try:
            watchlist = self._watchlist_builder.build(slots=bound, asof=asof)
            bound["watchlist"] = watchlist
            LOG.info("[ASharesPolicySlotBinder] watchlist injected asof=%s", asof)
        except FileNotFoundError as e:
            # coverage 缺失属于配置错误：明确失败
            LOG.exception("[ASharesPolicySlotBinder] watchlist coverage missing: %s", e)
            raise
        except Exception:
            LOG.exception("[ASharesPolicySlotBinder] failed to build watchlist observation")
            raise

        LOG.info("[ASharesPolicySlotBinder] bind finished: slots=%s", sorted(bound.keys()))
        return bound

    def bind_slot(self, raw_name: str, fr: FactorResult) -> str | None:
        return self.SLOT_MAP.get(raw_name)

    def _infer_asof(self, factors: Dict[str, FactorResult]) -> str:
        # 只用于 meta.asof，不影响 Gate/Action
        for fr in factors.values():
            if not isinstance(fr, FactorResult):
                continue
            td = fr.details.get("trade_date") or fr.details.get("date")
            if isinstance(td, str) and td.strip():
                return td.strip()
        return "unknown"
