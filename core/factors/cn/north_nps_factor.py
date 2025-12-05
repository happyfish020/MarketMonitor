# -*- coding: utf-8 -*-
"""
UnifiedRisk v11.7 — NorthNPS 因子（详细量化版 B）
"""

from __future__ import annotations
from typing import Dict, Any
from core.models.factor_result import FactorResult

class NorthNPSFactor:
    name = "north_nps"

    def compute_from_daily(self, processed: Dict[str, Any]) -> FactorResult:
        data = processed or {}

        block = (
            data.get("north_nps")
            or data.get("north")
            or data.get("etf_proxy")
            or {}
        )

        now = float(
            block.get("north_flow_e9")
            or block.get("net_etf_flow_e9")
            or block.get("etf_flow_e9")
            or 0.0
        )

        trend_10d = float(block.get("trend_10d") or 0.0)
        acc_3d = float(block.get("acc_3d") or 0.0)

        # ------------------- 区间标签 ---------------------
        if now >= 30:
            zone = "超强流入"
        elif now >= 15:
            zone = "强流入"
        elif now >= 5:
            zone = "温和流入"
        elif now <= -30:
            zone = "超强流出"
        elif now <= -15:
            zone = "强流出"
        elif now <= -5:
            zone = "温和流出"
        else:
            zone = "中性"

        # ------------------- 得分 -------------------------
        clipped = max(-40.0, min(40.0, now))
        score = 50.0 + (clipped / 40.0) * 40.0
        score = max(0.0, min(100.0, score))

        if score >= 70:
            level = "偏多（净流入）"
        elif score >= 55:
            level = "略偏多"
        elif score >= 45:
            level = "中性"
        elif score >= 30:
            level = "偏空（净流出）"
        else:
            level = "显著偏空（强流出）"

        signal = f"{zone}，当日净流入 {now:.2f} 亿"

        # ------------------- 报告块（详细 B 版） -----------------------
        report_block = (
            f"  - north_nps: {score:.2f}（{level}）\n"
            f"      · 当日北向代理净流入：{now:.2f} 亿（{zone}）\n"
            f"      · 10日趋势（斜率）：{trend_10d:.2f}（趋势 {'上行' if trend_10d>0 else '下行' if trend_10d<0 else '中性'}）\n"
            f"      · 3日加速度：{acc_3d:.2f} 亿（{'加速流入' if acc_3d>0 else '加速流出' if acc_3d<0 else '中性'}）\n"
            f"      · 北向强弱区间判断：{zone}\n"
        )

        details = {
            "north_flow_e9": now,
            "trend_10d": trend_10d,
            "acc_3d": acc_3d,
            "zone": zone,
            "level": level,
        }

        return FactorResult(
            name=self.name,
            score=score,
            details=details,
            level=level,
            signal=signal,
            raw=details,
            report_block=report_block,
        )
