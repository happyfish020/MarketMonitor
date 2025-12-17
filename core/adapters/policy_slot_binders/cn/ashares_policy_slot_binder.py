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
        
         
    }

    def bind_slot(self, raw_name: str, fr: FactorResult) -> str | None:
        return self.SLOT_MAP.get(raw_name)
