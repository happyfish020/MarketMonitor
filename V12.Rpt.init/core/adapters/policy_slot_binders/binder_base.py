from abc import ABC, abstractmethod
from typing import Dict
from core.factors.factor_result import FactorResult


class PolicySlotBinderBase(ABC):
    """
    PolicySlotBinderBase（冻结版）

    职责：
    - 输入：Dict[str, FactorResult]（key = *_raw）
    - 输出：Dict[str, FactorResult]（key = 制度槽位名）
    - 只做“raw → slot”的绑定
    - ❌ 不修改 FactorResult（FactorResult 为 frozen）
    """

    def bind(self, factors: Dict[str, FactorResult]) -> Dict[str, FactorResult]:
        bound: Dict[str, FactorResult] = {}

        for raw_name, fr in factors.items():
            slot = self.bind_slot(raw_name, fr)
            if slot is None:
                continue
            bound[slot] = fr

        return bound

    @abstractmethod
    def bind_slot(self, raw_name: str, fr: FactorResult) -> str | None:
        """
        返回制度槽位名（不带 _raw）
        返回 None 表示该因子不进入 Prediction
        """
        raise NotImplementedError
