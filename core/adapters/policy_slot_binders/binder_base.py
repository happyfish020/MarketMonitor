from abc import ABC, abstractmethod
from typing import Dict, Optional
import logging

from core.factors.factor_result import FactorResult


logger = logging.getLogger(__name__)


class PolicySlotBinderBase(ABC):
    """
    PolicySlotBinderBase（冻结版 · 兼容性审计通过）

    职责：
    - 输入：Dict[str, FactorResult]（key = *_raw）
    - 输出：Dict[str, FactorResult]（key = 制度槽位名）
    - 只做“raw → slot”的绑定
    - ❌ 不修改 FactorResult（FactorResult 为 frozen）
    """

    def bind(self, factors: Dict[str, FactorResult]) -> Dict[str, FactorResult]:
        """
        将 raw 因子绑定到制度槽位。

        运行时安全保证：
        - raw_name 必须为 str
        - slot 不允许重复（否则立即失败）
        """
        bound: Dict[str, FactorResult] = {}

        for raw_name, fr in factors.items():
            # ---- 类型防御（零假设）----
            if not isinstance(raw_name, str):
                logger.error(
                    "[PolicySlotBinder] raw_name type error: %r (%s)",
                    raw_name,
                    type(raw_name),
                )
                raise TypeError("PolicySlotBinder expects factor keys to be str")

            if not isinstance(fr, FactorResult):
                logger.error(
                    "[PolicySlotBinder] factor value type error: key=%s, value=%r (%s)",
                    raw_name,
                    fr,
                    type(fr),
                )
                raise TypeError("PolicySlotBinder expects values to be FactorResult")

            # ---- 绑定槽位 ----
            slot = self.bind_slot(raw_name, fr)
            if slot is None:
                continue

            # ---- 制度安全：禁止 silent override ----
            if slot in bound:
                logger.critical(
                    "[PolicySlotBinder] slot collision detected: slot=%s, "
                    "raw=%s conflicts with existing factor=%s",
                    slot,
                    raw_name,
                    bound[slot].name,
                )
                raise RuntimeError(f"Policy slot collision: {slot}")

            bound[slot] = fr

        return bound

    @abstractmethod
    def bind_slot(self, raw_name: str, fr: FactorResult) -> Optional[str]:
        """
        返回制度槽位名（不带 _raw）
        返回 None 表示该因子不进入 Prediction
        """
        raise NotImplementedError
