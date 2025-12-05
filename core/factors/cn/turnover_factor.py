# -*- coding: utf-8 -*-
"""
UnifiedRisk v11.7 — Turnover 因子（详细量化版 B）
"""

from __future__ import annotations
from typing import Dict, Any
from core.models.factor_result import FactorResult

class TurnoverFactor:
    name = "turnover"

    def compute_from_daily(self, processed: Dict[str, Any]) -> FactorResult:
        data = processed or {}
        block = data.get("turnover") or {}

        sh = float(block.get("shanghai") or 0.0)
        sz = float(block.get("shenzhen") or 0.0)
        total = float(block.get("total") or (sh + sz))

        # 缺失数据处理
        if total <= 0:
            report_block = (
                "  - turnover: 50.00（中性）\n"
                "      · 两市成交额数据缺失\n"
            )
            return FactorResult(
                name=self.name,
                score=50.0,
                details={},
                level="中性",
                signal="成交额缺失",
                raw={},
                report_block=report_block,
            )

        # ---- 热度区间 ----
        if total >= 12000:
            zone = "极度放量（高温）"
            score = 85
        elif total >= 9000:
            zone = "明显放量"
            score = 75
        elif total >= 6000:
            zone = "活跃"
            score = 65
        elif total >= 4000:
            zone = "偏冷"
            score = 50
        else:
            zone = "极度缩量（冷却）"
            score = 35

        level = zone
        signal = f"{zone}，两市成交额 {total:.0f} 亿"

        # ---- 成交结构点评 ----
        structure = (
            "深市成交额 > 上市 → 中小盘较活跃"
            if sz > sh else
            "沪市成交额占优 → 大盘主导"
        )

        # ---- 报告 ----
        report_block = (
            f"  - turnover: {score:.2f}（{level}）\n"
            f"      · 上证成交额：{sh:.0f} 亿；深证成交额：{sz:.0f} 亿\n"
            f"      · 全市场成交额：{total:.0f} 亿（{zone}）\n"
            f"      · 成交结构点评：{structure}\n"
        )

        return FactorResult(
            name=self.name,
            score=score,
            level=level,
            signal=signal,
            details={"total": total, "sh": sh, "sz": sz, "zone": zone},
            raw=block,
            report_block=report_block,
        )
