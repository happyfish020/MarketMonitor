from core.adapters.policy_slot_binders.binder_base import PolicySlotBinderBase
from core.factors.factor_result import FactorResult


class ASharesPolicySlotBinder(PolicySlotBinderBase):
    """
    A 股制度槽位绑定（冻结）
    """

    SLOT_MAP = {
        "breadth_raw": "breadth",
        "north_nps_raw": "north_nps",
        "turnover_raw": "turnover",
        "unified_emotion_raw": "unified_emotion",
        "index_tech_raw": "index_tech",
        "sector_rotation_raw": "sector_rotation",
        "global_lead_raw": "global_lead",
        "margin_raw": "margin",
        "index_global_raw": "index_global",
        "global_macro_raw": "global_macro",
         # ✅ 新增：ETF × Index × Spot 同步性（反证型）
        "etf_index_sync_raw": "etf_index_sync",
    }        
         
    def bind_slot(self, raw_name: str, fr: FactorResult) -> str | None:
        """
        raw_name -> slot_name
        只做映射，不做制度判断
        """
        return self.SLOT_MAP.get(raw_name)

    # ------------------------------------------------------------
    @staticmethod
    def apply_structural_downgrade(
        *,
        current_gate: str,
        slots: dict[str, FactorResult],
    ) -> str:
        """
        结构性反证降级钩子（冻结）

        规则（铁律）：
        - etf_index_sync 只能 downgrade
        - 只在 NORMAL 下生效
        - 不允许任何 upgrade
        """

        if current_gate != "NORMAL":
            return current_gate

        etf_sync = slots.get("etf_index_sync")
        if etf_sync and etf_sync.level == "LOW":
            return "CAUTION"

        return current_gate

    def bind_slot(self, raw_name: str, fr: FactorResult) -> str | None:
        return self.SLOT_MAP.get(raw_name)
