# -*- coding: utf-8 -*-
"""
UnifiedRisk v11.7
margin_factor.py — 全系统统一单位版（内部全部使用“亿元(e9)”）
"""

from __future__ import annotations

import numpy as np
from typing import Dict, Any
from core.models.factor_result import FactorResult
from core.adapters.datasources.cn.em_margin_client import EastmoneyMarginClientCN


class MarginFactor:
    name = "margin"

    def compute_from_daily(self, processed: Dict[str, Any]) -> FactorResult:
        # 获取最近 20~60 日两融数据（单位已经在 client 中转换为“亿”）
        client = EastmoneyMarginClientCN()
        series = client.get_recent_series(max_days=60)

        if not series or len(series) < 5:
            return FactorResult(
                name=self.name,
                score=50,
                signal="数据不足",
                raw={},
                report_block=f"  - {self.name}: 50.00（数据不足）\n",
            )

        # 直接使用“亿”单位
        rz = np.array([float(x["rz"]) for x in series], dtype=float)
        rq = np.array([float(x["rq"]) for x in series], dtype=float)
        total = rz + rq
        dates = [x["date"] for x in series]

        rz_now, rz_prev = rz[-1], rz[-2]
        rq_now, rq_prev = rq[-1], rq[-2]
        total_now, total_prev = total[-1], total[-2]

        # 涨跌幅（比例）
        rz_chg = (rz_now - rz_prev) / rz_prev if rz_prev != 0 else 0
        rq_chg = (rq_now - rq_prev) / rq_prev if rq_prev != 0 else 0
        total_chg_pct = (total_now - total_prev) / total_prev if total_prev != 0 else 0

        # 10 日趋势
        slope_10 = rz[-1] - rz[-10]
        if slope_10 > 0:
            trend_label = "融资持续增加（偏多）"
        elif slope_10 < 0:
            trend_label = "融资持续减少（偏空）"
        else:
            trend_label = "趋势中性"

        # 3 日加速度
        acc_3d = rz[-1] - rz[-3]
        if acc_3d > 0:
            acc_label = "加速流入（偏多）"
        elif acc_3d < 0:
            acc_label = "明显减速（偏空）"
        else:
            acc_label = "中性"

        # 杠杆区间（真实 A 股两融余额 ≈ 15000–18000 亿）
        if total_now >= 18000:
            zone_label = "高杠杆区（需警惕系统性风险）"
        elif total_now >= 15000:
            zone_label = "正常偏高"
        else:
            zone_label = "正常区间"

        # 得分
        score = 50
        score += 10 if slope_10 > 0 else -10 if slope_10 < 0 else 0
        score += 5 if acc_3d > 0 else -5 if acc_3d < 0 else 0
        if total_now >= 18000:
            score -= 10

        score = max(0, min(100, score))

        # 信号
        if score >= 60:
            signal = "偏多"
        elif score <= 40:
            signal = "偏空"
        else:
            signal = "中性"

        # 报告块
        report_block = f"""  - {self.name}: {score:.2f}（{signal}）
        · 当日融资余额：{rz_now:.2f} 亿（{rz_chg*100:.2f}%）
        · 当日融券余额：{rq_now:.2f} 亿（{rq_chg*100:.2f}%）
        · 两融总余额：{total_now:.2f} 亿（{total_chg_pct*100:.2f}%）
        · 10日趋势：{trend_label}
        · 3日加速度：{acc_3d:.2f} 亿（{acc_label}）
        · 杠杆风险区间：{zone_label}
"""

        return FactorResult(
            name=self.name,
            score=score,
            signal=signal,
            raw={
                "dates": dates,
                "rz": rz.tolist(),
                "rq": rq.tolist(),
                "total": total.tolist(),
                "trend_label": trend_label,
                "acc_3d": acc_3d,
                "zone_label": zone_label,
            },
            report_block=report_block,
        )
