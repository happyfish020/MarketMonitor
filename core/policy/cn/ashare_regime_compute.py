# -*- coding: utf-8 -*-
"""
UnifiedRisk V12
CN A-Share RegimeCompute (Structure / Regime Evaluator)

职责（冻结）：
- 基于 PolicySlots / FactorResult，构建结构性判断（RegimeResult）
- 仅做“结构与状态”的归纳与判定
- 不产生交易裁决、不生成 ActionHint、不参与 Report
- 当前版本包裹既有 StructureFactsBuilder（过渡实现）
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from core.utils.logger import get_logger
from core.factors.factor_result import FactorResult
from core.regime.observation.structure.structure_facts_builder import (
    StructureFactsBuilder,
)

LOG = get_logger("Policy.AshareRegimeCompute")


class AshareRegimeCompute:
    """
    A股 RegimeCompute（结构/状态判断器）

    输出（冻结）：
    RegimeResult = {
        "structure": Dict[str, Any],
        "regime": Optional[str],
        "warnings": list[str],
    }
    """

    def __init__(self, *, structure_builder: StructureFactsBuilder | None = None) -> None:
        # 允许注入（测试 / 将来替换）
        self._structure_builder = structure_builder or StructureFactsBuilder()

    def compute(
        self,
        *,
        factors: Dict[str, FactorResult],
        policy_slots: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        构建 RegimeResult（结构事实 + 状 reminder）

        注意：
        - 当前阶段 regime 字段可为空（None）
        - warnings 仅作为制度提示，不影响 Gate
        """
        if not isinstance(factors, dict):
            raise TypeError("factors must be dict[str, FactorResult]")
        if not isinstance(policy_slots, dict):
            raise TypeError("policy_slots must be dict")

        try:
            structure = self._structure_builder.build(factors=factors)
        except Exception:
            LOG.exception("[AshareRegimeCompute] structure builder failed")
            raise

        if not isinstance(structure, dict):
            raise TypeError("structure must be a dict")

        # regime：当前版本不强行定义市场 regime
        regime: Optional[str] = None

        # warnings：仅做结构性提示（非裁决）
        warnings: list[str] = []

        # 示例：如果 breadth 明显破坏，给出提示（不裁决）
        breadth = structure.get("breadth")
        if isinstance(breadth, dict):
            state = breadth.get("state")
            if state in ("damaged", "broken"):
                warnings.append(f"Market breadth {state}")

        LOG.info(
            "[AshareRegimeCompute] built regime result | "
            "structure_keys=%s warnings=%s",
            sorted(list(structure.keys())),
            warnings,
        )

        return {
            "structure": structure,
            "regime": regime,
            "warnings": warnings,
        }

    # 支持 callable 注入
    def __call__(
        self,
        *,
        factors: Dict[str, FactorResult],
        policy_slots: Dict[str, Any],
    ) -> Dict[str, Any]:
        return self.compute(factors=factors, policy_slots=policy_slots)
