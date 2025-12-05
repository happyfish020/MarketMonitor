# -*- coding: utf-8 -*-
"""
UnifiedRisk v11.7 — MarginFactor（两融因子，新版）
统一单位：亿元（e9）

依赖 processed["margin"]：
  {
    "series": [
      {"date": "YYYY-MM-DD", "rz_e9": ..., "rq_e9": ..., "total_e9": ...},
      ...
    ]
  }
若数据缺失，则返回中性评分。
"""

from __future__ import annotations

import numpy as np
from typing import Dict, Any, List
from core.models.factor_result import FactorResult


class MarginFactor:
    name = "margin"

    def compute_from_daily(self, processed: Dict[str, Any]) -> FactorResult:
        data = processed or {}
        margin_block = data.get("margin") or {}
        series: List[Dict[str, Any]] = margin_block.get("series") or []

        if not series:
            score = 50.0
            level = "中性"
            signal = "两融数据缺失，视为中性"
            raw = {"series": []}
            details = {
                "level": level,
                "rz_last_e9": None,
                "rq_last_e9": None,
                "total_last_e9": None,
                "trend_10d": None,
                "acc_3d": None,
                "zone_label": "未知",
            }
            report_block = (
                "  - margin: 50.00（中性）\n"
                "      · 两融数据缺失，暂不评估杠杆风险\n"
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

        # 转为 np 向量
        rz = np.array([float(x.get("rz_e9") or 0.0) for x in series], dtype=float)
        rq = np.array([float(x.get("rq_e9") or 0.0) for x in series], dtype=float)
        total = np.array([float(x.get("total_e9") or (rz_i + rq_i)) for x, rz_i, rq_i in zip(series, rz, rq)], dtype=float)

        dates = [x.get("date") for x in series]
        n = len(total)

        # 末日数值
        rz_last = float(rz[-1])
        rq_last = float(rq[-1])
        total_last = float(total[-1])

        # 10 日趋势（简单回归斜率）
        if n >= 10:
            y = total[-10:]
            x = np.arange(len(y))
            A = np.vstack([x, np.ones_like(x)]).T
            slope_10, _ = np.linalg.lstsq(A, y, rcond=None)[0]
        else:
            slope_10 = 0.0

        # 3 日加速度（后 3 日差分）
        if n >= 3:
            acc_3d = float(total[-1] - total[-3])
        else:
            acc_3d = 0.0

        # 杠杆区间（粗略，以总额为 proxy）
        if total_last >= 20000:
            zone = "极高杠杆"
            base_score = 30
        elif total_last >= 16000:
            zone = "高杠杆"
            base_score = 40
        elif total_last >= 12000:
            zone = "中等偏高杠杆"
            base_score = 50
        elif total_last >= 8000:
            zone = "中性杠杆"
            base_score = 60
        else:
            zone = "低杠杆"
            base_score = 70

        # 趋势修正：若近期大幅上升 → 风险偏高
        trend_adj = 0
        if slope_10 > 50:   # 10 日内快速上升
            trend_adj = -5
        elif slope_10 < -50:
            trend_adj = +5

        # 综合得分
        score = float(base_score + trend_adj)
        score = max(0.0, min(100.0, score))

        # 文本 level
        if score >= 70:
            level = "杠杆偏低（有弹药）"
        elif score >= 55:
            level = "杠杆中性"
        elif score >= 40:
            level = "杠杆偏高（需谨慎）"
        else:
            level = "杠杆显著偏高（风险区）"

        signal = f"{zone}，当日两融总额约 {total_last:.0f} 亿"

        raw = {
            "dates": dates,
            "rz": rz.tolist(),
            "rq": rq.tolist(),
            "total": total.tolist(),
            "trend_10d": slope_10,
            "acc_3d": acc_3d,
            "zone_label": zone,
        }
        details = {
            "level": level,
            "rz_last_e9": rz_last,
            "rq_last_e9": rq_last,
            "total_last_e9": total_last,
            "trend_10d": slope_10,
            "acc_3d": acc_3d,
            "zone_label": zone,
        }

        report_lines = [
            f"  - margin: {score:.2f}（{level}）",
            f"      · 当日融资余额：{rz_last:.0f} 亿；融券余额：{rq_last:.0f} 亿；两融总额：{total_last:.0f} 亿（{zone}）",
            f"      · 10日趋势：{slope_10:.2f}（总额回归斜率，>0 为上升）",
            f"      · 3日加速度：{acc_3d:.2f} 亿",
            "",
        ]
        report_block = "\n".join(report_lines)

        return FactorResult(
            name=self.name,
            score=score,
            details=details,
            level=level,
            signal=signal,
            raw=raw,
            report_block=report_block,
        )
