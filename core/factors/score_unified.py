# -*- coding: utf-8 -*-
"""
Unified Score Builder (V11.6.2)
- 统一因子评分
- 增加 factor_details 字段（支持 margin 等详细信息输出）
"""

from __future__ import annotations
from typing import Dict, Any, Mapping

from core.models.factor_result import FactorResult


class UnifiedScoreBuilder:
    """
    将多个 FactorResult 合并成统一结果 summary：
    {
        "total_score": float,
        "risk_level": str,
        "factor_scores": {name: score},
        "factor_signals": {name: signal},
        "factor_details": {name: <detail dict or raw>},
    }
    """

    def unify(self, factors: Mapping[str, FactorResult]) -> Dict[str, Any]:
        summary: Dict[str, Any] = {
            "factor_scores": {},
            "factor_signals": {},
            "factor_details": {},   # <-- 🔥 新增字段
        }

        # -------- 汇总每个因子 --------
        total = 0.0
        for name, factor in factors.items():
            sc = float(factor.score)
            total += sc

            summary["factor_scores"][name] = sc
            summary["factor_signals"][name] = factor.signal

            # 🔥 detail 统一写入 factor_details
            # Margin 等高级因子的 detail 保存在 factor.raw 中
            summary["factor_details"][name] = factor.raw or {}

        # -------- 平均评分作为综合得分 --------
        n = len(factors)
        if n > 0:
            summary["total_score"] = round(total / n, 2)
        else:
            summary["total_score"] = 50.0

        # -------- 风险等级规则 --------
        ts = summary["total_score"]
        if ts >= 60:
            summary["risk_level"] = "偏强"
        elif ts >= 45:
            summary["risk_level"] = "中性"
        else:
            summary["risk_level"] = "偏弱"

        return summary
