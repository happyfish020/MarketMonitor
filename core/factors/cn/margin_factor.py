# -*- coding: utf-8 -*-
"""
UnifiedRisk v11.7.2 — MarginFactor（两融因子）
修复：
- 东财 RZRQ 数据为 T-1（非今日）
- 字段名兼容 rz/rz_e9, rq/rq_e9, rzrq/total_e9
- 非 T 当日 → 自动向后回退 + 报告中标注日期
"""

from __future__ import annotations
import numpy as np
from typing import Dict, Any, List
from core.models.factor_result import FactorResult


class MarginFactor:
    name = "margin"

    def compute_from_daily(self, processed: Dict[str, Any], trade_date=None) -> FactorResult:
        data = processed or {}
        margin_block = data.get("margin") or {}
        series: List[Dict[str, Any]] = margin_block.get("series") or []

        # ===== 无序列：完全缺失 =====
        if not series:
            return FactorResult(
                name=self.name,
                score=50.0,
                level="中性",
                signal="两融数据缺失",
                details={},
                raw={},
                report_block=(
                    "  - margin: 50.00（中性）\n"
                    "      · 两融数据缺失，无法评估杠杆风险\n"
                ),
            )

        # ===== 识别 T-1 数据 =====
        last_item = series[-1]
        series_last_date = str(last_item.get("date"))

        is_t_minus_one = (trade_date and series_last_date < str(trade_date))

        # ===== 字段名称兼容（东财：rz, rq, rzrq） =====
        def _get_value(item, *keys):
            for k in keys:
                if k in item:
                    return float(item[k] or 0.0)
            return 0.0

        rz = np.array([_get_value(x, "rz_e9", "rz") for x in series], dtype=float)
        rq = np.array([_get_value(x, "rq_e9", "rq") for x in series], dtype=float)
        total = np.array(
            [
                _get_value(x, "total_e9", "rzrq", "rz+rq") or (rz_i + rq_i)
                for x, rz_i, rq_i in zip(series, rz, rq)
            ],
            dtype=float,
        )

        dates = [str(x.get("date")) for x in series]
        n = len(total)

        rz_last = rz[-1]
        rq_last = rq[-1]
        total_last = total[-1]

        # ===== 特殊：若真实只有某天没有数据，而不是全 0，则不判为缺失 =====
        if np.sum(total) == 0:
            return FactorResult(
                name=self.name,
                score=50.0,
                level="中性",
                signal="两融数据可能接口异常（全为 0）",
                details={},
                raw={"series": series},
                report_block=(
                    "  - margin: 50.00（中性）\n"
                    "      · 警告：两融序列全部为 0，推测为接口异常，暂不解读\n"
                ),
            )

        # ===== 10日趋势回归（斜率） =====
        if n >= 10:
            y = total[-10:]
            x = np.arange(len(y))
            A = np.vstack([x, np.ones_like(x)]).T
            slope_10, _ = np.linalg.lstsq(A, y, rcond=None)[0]
        else:
            slope_10 = 0.0

        # ===== 3 日加速度 =====
        acc_3d = float(total[-1] - total[-3]) if n >= 3 else 0.0

        # ===== 杠杆区间判定 =====
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

        # 趋势修正
        trend_adj = -5 if slope_10 > 50 else (5 if slope_10 < -50 else 0)
        score = max(0.0, min(100.0, base_score + trend_adj))

        if score >= 70:
            level = "杠杆偏低（有弹药）"
        elif score >= 55:
            level = "杠杆中性"
        elif score >= 40:
            level = "杠杆偏高（需谨慎）"
        else:
            level = "杠杆显著偏高（风险区）"

        # ===== 报告文本（新增 T-1 提示） =====
        date_note = (
            f"（数据日期：{series_last_date}，非今日；东财官方为 T-1 更新）"
            if is_t_minus_one else ""
        )

        trend_label = (
            "快速上升（风险抬升）" if slope_10 > 50 else
            "缓慢上升" if slope_10 > 0 else
            "缓慢下降（去杠杆）" if slope_10 < -50 else
            "震荡/中性"
        )

        acc_label = (
            "近期明显加杠杆" if acc_3d > 50 else
            "略有加杠杆" if acc_3d > 0 else
            "略有去杠杆" if acc_3d < 0 else
            "中性"
        )

        report_block = (
            f"  - margin: {score:.2f}（{level}）\n"
            f"      · 当日两融（T-1）数据：融资 {rz_last:.0f} 亿；融券 {rq_last:.0f} 亿；总额 {total_last:.0f} 亿（{zone}）{date_note}\n"
            f"      · 10日趋势（回归斜率）：{slope_10:.2f}（{trend_label}）\n"
            f"      · 3日加速度：{acc_3d:.2f} 亿（{acc_label}）\n"
        )

        details = {
            "rz_last_e9": rz_last,
            "rq_last_e9": rq_last,
            "total_last_e9": total_last,
            "trend_10d": slope_10,
            "acc_3d": acc_3d,
            "zone_label": zone,
            "series_last_date": series_last_date,
            "is_t_minus_one": is_t_minus_one,
        }

        raw = {
            "dates": dates,
            "rz": rz.tolist(),
            "rq": rq.tolist(),
            "total": total.tolist(),
        }

        return FactorResult(
            name=self.name,
            score=score,
            level=level,
            signal=f"{zone}，T-1 总额 {total_last:.0f} 亿",
            details=details,
            raw=raw,
            report_block=report_block,
        )
