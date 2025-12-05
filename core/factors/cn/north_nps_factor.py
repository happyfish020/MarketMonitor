# -*- coding: utf-8 -*-
"""
UnifiedRisk v11.7 — NorthNPS 因子（新版，适配 FactorResult V11.7）
统一单位：亿元（e9）
依赖 processed 中的北向相关数据：
  - processed["north_nps"] 或 processed["north"] 或 processed["etf_proxy"]
若数据缺失则返回中性评分。
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

        # 关键：这里要把 etf_flow_e9 纳入
        now = float(
            block.get("north_flow_e9")
            or block.get("net_etf_flow_e9")
            or block.get("etf_flow_e9")      # ★ 新增：你的字段
            or 0.0
        )

        # —— 下面是简单区间/打分，可以沿用你原来的逻辑，这里给一套示例 —— 
        if now >= 30:
            zone_label = "超强流入"
        elif now >= 15:
            zone_label = "强流入"
        elif now >= 5:
            zone_label = "温和流入"
        elif now <= -30:
            zone_label = "超强流出"
        elif now <= -15:
            zone_label = "强流出"
        elif now <= -5:
            zone_label = "温和流出"
        else:
            zone_label = "中性"

        clipped = max(-40.0, min(40.0, now))
        score = 50.0 + (clipped / 40.0) * 40.0
        score = max(0.0, min(100.0, score))

        if score >= 75:
            level = "北向显著净流入（偏多）"
        elif score >= 60:
            level = "北向温和净流入"
        elif score >= 40:
            level = "北向中性"
        elif score >= 25:
            level = "北向温和净流出"
        else:
            level = "北向显著净流出（偏空）"

        signal = f"{zone_label}，当日净流入 {now:.2f} 亿"

        raw = {
            "north_flow_e9": now,
            "zone": zone_label,
        }
        details = {
            "level": level,
            "north_flow_e9": now,
            "zone": zone_label,
        }

        report_block = (
            f"  - north_nps: {score:.2f}（{level}）\n"
            f"      · 当日北向代理净流入：{now:.2f} 亿（{zone_label}）\n"
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