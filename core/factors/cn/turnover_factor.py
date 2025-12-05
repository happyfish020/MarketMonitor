# -*- coding: utf-8 -*-
"""
UnifiedRisk v11.7 — Turnover 因子（成交额因子，新版）
统一单位：亿元（e9）
依赖 processed["turnover"] 或 snapshot["turnover"]：
  {
    "shanghai":  xxxx,   # 上证成交额
    "shenzhen":  xxxx,   # 深证成交额
    "total":     xxxx
  }
若数据缺失，则返回中性评分。
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

        # 如果几乎没有数据，直接中性
        if total <= 0:
            score = 50.0
            level = "中性"
            signal = "成交额数据缺失，视为中性"
            raw = {"sh_turnover_e9": sh, "sz_turnover_e9": sz, "total_turnover_e9": total}
            details = {
                "level": level,
                "sh_turnover_e9": sh,
                "sz_turnover_e9": sz,
                "total_turnover_e9": total,
                "liquidity_zone": "未知",
            }
            report_block = (
                "  - turnover: 50.00（中性）\n"
                "      · 成交额数据缺失，无法评估流动性\n"
            )
            return FactorResult(
                name=self.name,
                score=score,
                details=details,
                level=level,
                signal=signal,
                raw=raw,
                report_block=report_block,
            )

        # ----------------- 流动性区间与打分 -----------------
        # 粗略：< 5000 亿：偏冷；5000~9000：中性；>9000：偏热
        if total >= 12000:
            zone = "极度放量"
            base = 85
        elif total >= 9000:
            zone = "明显放量"
            base = 75
        elif total >= 6000:
            zone = "正常偏上"
            base = 65
        elif total >= 4000:
            zone = "正常偏下"
            base = 50
        else:
            zone = "严重缩量"
            base = 35

        score = float(base)
        level = zone
        signal = f"{zone}（两市成交额约 {total:.0f} 亿）"

        raw = {
            "sh_turnover_e9": sh,
            "sz_turnover_e9": sz,
            "total_turnover_e9": total,
            "liquidity_zone": zone,
        }
        details = {
            "level": level,
            "sh_turnover_e9": sh,
            "sz_turnover_e9": sz,
            "total_turnover_e9": total,
            "liquidity_zone": zone,
        }

        report_block = (
            f"  - turnover: {score:.2f}（{level}）\n"
            f"      · 上证成交额：{sh:.0f} 亿；深证成交额：{sz:.0f} 亿\n"
            f"      · 全市场成交额：{total:.0f} 亿（{zone}）\n"
        )

        return FactorResult(
            name=self.name,
            score=score,
            details=details,
            level=level,
            signal=signal,
            raw=raw,
            report_block=report_block,
        )
