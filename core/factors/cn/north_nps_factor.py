# -*- coding: utf-8 -*-
"""
UnifiedRisk v11.7 — NorthNPS 因子
统一单位：亿元（e9）
结构与 margin 因子一致（金额 / 趋势 / 加速度 / 区间）
"""

from __future__ import annotations

import numpy as np
from typing import Dict, Any
from core.models.factor_result import FactorResult


class NorthNPSFactor:
    name = "north_nps"

    # ======================================================================
    # 主入口
    # ======================================================================
    def compute_from_daily(self, processed: Dict[str, Any]) -> FactorResult:

        f = processed["features"]

        # -------------------------------------------------------------
        # 一、读取当日北向代理资金（ETF Proxy）
        # etf_flow_e9 单位本身就是“亿”
        # -------------------------------------------------------------
        flow_now = float(f.get("etf_flow_e9") or 0.0)       # 如 -0.10 亿
        turnover_now = float(f.get("etf_turnover_e9") or 0.0)
        hs300_pct = float(f.get("hs300_proxy_pct") or 0.0)

        # -------------------------------------------------------------
        # 二、构造历史序列（占位：10 天相同值）
        # 未来接入东财北向真实序列后即可替换
        # -------------------------------------------------------------
        series = np.array([flow_now] * 10, dtype=float)

        now = series[-1]
        prev = series[-2]

        # =============================================================
        # 三、趋势（10 日变化）
        # =============================================================
        slope_10 = series[-1] - series[0]
        if slope_10 > 0:
            trend_label = "资金持续流入（偏多）"
        elif slope_10 < 0:
            trend_label = "资金持续流出（偏空）"
        else:
            trend_label = "历史数据有限，趋势中性"

        # =============================================================
        # 四、3 日加速度
        # =============================================================
        acc_3d = series[-1] - series[-3]
        if acc_3d > 0:
            acc_label = "加速流入（偏多）"
        elif acc_3d < 0:
            acc_label = "流入减速（偏空）"
        else:
            acc_label = "中性（历史数据有限）"

        # =============================================================
        # 五、北向区间判断（方向 + 规模）
        # 逻辑与 margin 风格一致，更贴近交易直觉
        # =============================================================
        abs_now = abs(now)

        if abs_now < 2:
            zone_label = "观望区（当日流入流出有限）"
        elif now > 0:
            zone_label = "偏强区（温和净流入）"
        else:
            zone_label = "偏弱区（温和净流出）"

        # =============================================================
        # 六、得分（结合方向 + 规模 + 趋势 + 加速度）
        # =============================================================
        score = 50

        # --- 规模决定方向力度 ---
        if now > 10:
            score += 10
        elif now > 2:
            score += 5
        elif now < -10:
            score -= 10
        elif now < -2:
            score -= 5

        # --- 趋势 ---
        if slope_10 > 0:
            score += 5
        elif slope_10 < 0:
            score -= 5

        # --- 加速度 ---
        if acc_3d > 0:
            score += 3
        elif acc_3d < 0:
            score -= 3

        # 限制范围
        score = max(0, min(100, score))

        # 信号
        if score >= 60:
            signal = "偏多"
        elif score <= 40:
            signal = "偏空"
        else:
            signal = "中性"

        # =============================================================
        # 七、报告文本（完全对齐 margin 风格）
        # =============================================================
        report_block = f"""  - {self.name}: {score:.2f}（{signal}）
        · 当日北向代理流入：{now:.2f} 亿
        · 10日趋势：{trend_label}
        · 3日加速度：{acc_3d:.2f} 亿（{acc_label}）
        · 北向资金区间：{zone_label}
"""

        return FactorResult(
            name=self.name,
            score=score,
            signal=signal,
            raw={
                "north_flow_e9": now,
                "trend_10d": slope_10,
                "acc_3d": acc_3d,
                "zone": zone_label,
            },
            report_block=report_block,
        )
