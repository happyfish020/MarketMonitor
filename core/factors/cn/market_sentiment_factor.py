# -*- coding: utf-8 -*-
"""
UnifiedRisk v11.7
MarketSentimentFactor — 市场宽度 + 涨跌停 + HS300 代理
"""

from __future__ import annotations

from typing import Dict, Any
from core.models.factor_result import FactorResult


class MarketSentimentFactor:
    name = "market_sentiment"

    def compute_from_daily(self, processed: Dict[str, Any]) -> FactorResult:
        f = processed["features"]

        adv = int(f.get("adv") or 0)
        dec = int(f.get("dec") or 0)
        total = int(f.get("total_stocks") or (adv + dec))
        limit_up = int(f.get("limit_up") or 0)
        limit_down = int(f.get("limit_down") or 0)
        hs300_pct = float(f.get("hs300_proxy_pct") or 0.0)  # %

        total = max(total, 1)
        adv_ratio = adv / total

        # === 1. 宽度评分（以 adv_ratio 为主） ===
        score = 50.0
        width_comment = ""

        if adv_ratio >= 0.65:
            score += 15
            width_comment = "多头占绝对优势，情绪偏亢奋"
        elif adv_ratio >= 0.55:
            score += 7
            width_comment = "多头略占优，情绪偏乐观"
        elif adv_ratio >= 0.45:
            width_comment = "多空均衡，情绪中性"
        elif adv_ratio >= 0.35:
            score -= 7
            width_comment = "空头略占优，情绪偏谨慎"
        else:
            score -= 15
            width_comment = "空头主导，情绪偏悲观"

        # === 2. 涨跌停结构评分 ===
        lmt_comment = ""
        if limit_up >= 80 and limit_down <= 10:
            score += 8
            lmt_comment = "涨停家数较多，强势股活跃"
        elif limit_up >= 40 and limit_down <= 20:
            score += 3
            lmt_comment = "涨停活跃度尚可"
        elif limit_down >= 30 and limit_up <= 10:
            score -= 8
            lmt_comment = "跌停家数偏多，恐慌盘较重"
        else:
            if not lmt_comment:
                lmt_comment = "涨跌停结构中性"

        # === 3. HS300 方向修正 ===
        if hs300_pct >= 1.5:
            score += 5
        elif hs300_pct <= -1.5:
            score -= 5

        # 限制 0~100
        score = max(0.0, min(100.0, score))

        if score >= 60:
            signal = "偏多"
        elif score <= 40:
            signal = "偏空"
        else:
            signal = "中性"

        # === 4. 报告文本 ===
        report_block = f"""  - {self.name}: {score:.2f}（{signal}）
        · 涨跌：上涨 {adv} ；下跌 {dec} ；总数 {total}
        · 涨停：{limit_up} ；跌停：{limit_down}
        · adv_ratio：{adv_ratio:.2f}
        · HS300 代理涨跌：{hs300_pct:.2f}%
        · 宽度点评：{width_comment}
        · 涨跌停结构：{lmt_comment}
"""

        return FactorResult(
            name=self.name,
            score=score,
            signal=signal,
            raw={
                "adv": adv,
                "dec": dec,
                "total": total,
                "adv_ratio": adv_ratio,
                "limit_up": limit_up,
                "limit_down": limit_down,
                "hs300_pct": hs300_pct,
                "width_comment": width_comment,
                "lmt_comment": lmt_comment,
            },
            report_block=report_block,
        )
