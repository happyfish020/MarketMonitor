# -*- coding: utf-8 -*-
"""
UnifiedRisk V12
CN A-Share FactorCompute (Policy Slots Builder)

职责（冻结）：
- 将 FactorResult 集合转换为 PolicySlots（制度槽位）
- 仅做“制度表达层面的映射/归一化”，不做状态判断、不做 Gate 裁决
- 不访问 snapshot / raw datasource
- 以现有 ASharesPolicySlotBinder 为唯一数据来源（先包裹，后重构）

输入（冻结）：
- factors: Dict[str, FactorResult]

输出（冻结）：
- Dict[str, Any]  (PolicySlots)
"""

from __future__ import annotations

from typing import Any, Dict

from core.utils.logger import get_logger
from core.factors.factor_result import FactorResult
from core.adapters.policy_slot_binders.cn.ashares_policy_slot_binder import (
    ASharesPolicySlotBinder,
)

LOG = get_logger("Policy.AshareFactorCompute")


class AshareFactorCompute:
    """
    A股 FactorCompute（制度槽位构建器）

    说明：
    - 当前版本是“包裹现有 Binder”的过渡实现（制度一致性优先）
    - 后续如需拆细映射逻辑，应在本类内部逐步替换 binder 的输出结构，
      但对外输出契约必须保持稳定
    """

    def __init__(self, *, binder: ASharesPolicySlotBinder | None = None) -> None:
        # 允许外部注入 binder（便于测试/替换），默认使用现有实现
        self._binder = binder or ASharesPolicySlotBinder()

    def compute(self, *, factors: Dict[str, FactorResult]) -> Dict[str, Any]:
        """
        构建 PolicySlots（制度槽位）

        注意：
        - 本方法不做任何“市场状态/风险”判断
        - 仅负责把 factors 映射为 slots
        """
        if not isinstance(factors, dict):
            raise TypeError("factors must be a dict[str, FactorResult]")

        # 轻校验：key 必须是 str；value 必须是 FactorResult
        for k, v in factors.items():
            if not isinstance(k, str):
                raise TypeError("factors keys must be str")
            if not isinstance(v, FactorResult):
                raise TypeError(f"factors['{k}'] must be FactorResult")

        try:
            slots = self._binder.bind(factors)
        except Exception:
            LOG.exception("[AshareFactorCompute] binder.bind failed")
            raise

        if not isinstance(slots, dict):
            raise TypeError("PolicySlots must be a dict")

        # 基础兜底：只补 key，不覆盖（避免下游 KeyError）
        # 注意：这里是“制度槽位结构兜底”，不是制度判断
        slots.setdefault("watchlist", None)

        LOG.info(
            "[AshareFactorCompute] built policy slots keys=%s",
            sorted(list(slots.keys())),
        )
        return slots

    # 可选：支持 callable 注入（给 Orchestrator 用）
    def __call__(self, *, factors: Dict[str, FactorResult]) -> Dict[str, Any]:
        return self.compute(factors=factors)
