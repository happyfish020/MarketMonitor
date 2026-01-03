# -*- coding: utf-8 -*-
"""
UnifiedRisk V12
Structure Facts Mapper (CN A-Share) · delegate

职责：
- 兼容旧调用入口（StructureFactsMapper.build）
- 不再维护任何 meaning / 文案，避免口径漂移
- 结构事实的 state/modifier/evidence 统一由 StructureFactsBuilder 生成
- 报告人话解释统一在 StructureFactsBlock（单一事实来源）

说明：
- 若上层已直接使用 StructureFactsBuilder，可逐步 deprecate 本文件引用。
"""

from __future__ import annotations

from typing import Dict, Any, Optional

from core.factors.factor_result import FactorResult
from core.regime.observation.structure.structure_facts_builder import StructureFactsBuilder


class StructureFactsMapper:
    """Deprecated shim: delegate to StructureFactsBuilder."""

    @classmethod
    def build111111(
        cls,
        factor_results: Dict[str, FactorResult],
        *,
        distribution_risk_active: bool = False,
        drs_signal: Optional[str] = None,
    ) -> Dict[str, Any]:
        return StructureFactsBuilder().build(
            factors=factor_results,
            distribution_risk_active=distribution_risk_active,
            drs_signal=drs_signal,
        )
