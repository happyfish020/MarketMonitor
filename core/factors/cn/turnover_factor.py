# -*- coding: utf-8 -*-
"""
UnifiedRisk v11.7
TurnoverFactor — 成交额因子（单位统一为：亿元）
"""

from __future__ import annotations

from typing import Dict, Any
from core.models.factor_result import FactorResult


class TurnoverFactor:
    name = "turnover"

    def compute_from_daily(self, processed: Dict[str, Any]) -> FactorResult:
        f = processed["features"]

        # 这里的数值在 snapshot 中已是“亿”单位
        sh = float(f.get("sh_turnover_e9") or 0.0)
        sz = float(f.get("sz_turnover_e9") or 0.0)
        total = float(f.get("total_turnover_e9") or (sh + sz))

        # === 1. 成交热度区间（绝对规模） ===
        # 你可以根据现在 A 股的常态，之后再微调区间
        if total >= 1000:
            zone_label = "极度放量（需关注情绪亢奋与筹码松动）"
            base_score = 70
        elif total >= 700:
            zone_label = "放量活跃（风险与机会并存）"
            base_score = 60
        elif total >= 400:
            zone_label = "正常偏弱（略有缩量）"
            base_score = 50
        elif total >= 250:
            zone_label = "缩量偏弱（风险偏防御）"
            base_score = 40
        else:
            zone_label = "极度缩量（风险偏回避，博弈氛围浓）"
            base_score = 35

        # === 2. 结构简单点评（上/深占比，目前先给中性占位） ===
        structure_label = "结构中性（上/深成交占比正常）"

        # 如需精细：可用 sh / total 判断权重偏向主板/创成长

        # === 3. 得分（目前先用 base_score，后续可接入历史趋势） ===
        score = max(0.0, min(100.0, float(base_score)))

        if score >= 60:
            signal = "偏多"
        elif score <= 40:
            signal = "偏空"
        else:
            signal = "中性"

        # === 4. 报告块 ===
        report_block = f"""  - {self.name}: {score:.2f}（{signal}）
        · 上证成交额：{sh:.2f} 亿
        · 深证成交额：{sz:.2f} 亿
        · 全市场成交额：{total:.2f} 亿
        · 当日成交热度：{zone_label}
        · 历史趋势：历史数据有限，暂不评估
        · 成交结构点评：{structure_label}
"""

        return FactorResult(
            name=self.name,
            score=score,
            signal=signal,
            raw={
                "sh_turnover_e9": sh,
                "sz_turnover_e9": sz,
                "total_turnover_e9": total,
                "zone_label": zone_label,
                "structure_label": structure_label,
            },
            report_block=report_block,
        )
