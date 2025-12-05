# -*- coding: utf-8 -*-
"""
UnifiedRisk v11.7 — MarketSentiment 因子（宽度 + 涨跌停 + HS300 代理，详细版 B）

依赖 processed["breadth"]：
  {
    "adv":        上涨家数,
    "dec":        下跌家数,
    "total":      总家数,
    "limit_up":   涨停数,
    "limit_down": 跌停数,
    "adv_ratio":  上涨占比（可选，缺失时由 adv/total 计算）
  }
以及可选 HS300 代理涨跌：
  processed["hs300_pct"] 或 processed["etf_proxy"]["hs300_pct"]
"""

from __future__ import annotations

from typing import Dict, Any
from core.models.factor_result import FactorResult


class MarketSentimentFactor:
    name = "market_sentiment"

    def compute_from_daily(self, processed: Dict[str, Any]) -> FactorResult:
        data = processed or {}
        b = data.get("breadth") or {}

        adv = float(b.get("adv") or 0)
        dec = float(b.get("dec") or 0)
        total = float(b.get("total") or (adv + dec) or 1)
        lu = float(b.get("limit_up") or 0)
        ld = float(b.get("limit_down") or 0)

        adv_ratio = float(b.get("adv_ratio") or (adv / total if total > 0 else 0))

        # HS300 代理涨跌
        hs300_pct = 0.0
        if "hs300_pct" in data:
            hs300_pct = float(data.get("hs300_pct") or 0.0)
        else:
            etf_proxy = data.get("etf_proxy") or {}
            hs300_pct = float(etf_proxy.get("hs300_pct") or 0.0)

        # ----------------- 1）宽度评分 -----------------
        if adv_ratio >= 0.7:
            width_label = "普涨"
            width_score = 85
        elif adv_ratio >= 0.55:
            width_label = "涨多跌少"
            width_score = 70
        elif adv_ratio >= 0.45:
            width_label = "震荡"
            width_score = 55
        elif adv_ratio >= 0.3:
            width_label = "跌多涨少"
            width_score = 40
        else:
            width_label = "普跌"
            width_score = 25

        # ----------------- 2）涨跌停氛围 -----------------
        if lu >= 100 and ld <= 10:
            lmt_label = "强势涨停潮"
            lmt_score = 85
        elif lu >= 50 and ld <= 20:
            lmt_label = "较强涨停氛围"
            lmt_score = 70
        elif ld >= 50 and lu <= 10:
            lmt_label = "强势杀跌氛围"
            lmt_score = 25
        elif ld >= 30 and lu <= 20:
            lmt_label = "偏空杀跌"
            lmt_score = 40
        else:
            lmt_label = "中性"
            lmt_score = 55

        # ----------------- 3）HS300 方向评分 -----------------
        if hs300_pct >= 2.0:
            idx_label = "大盘强势上攻"
            idx_score = 85
        elif hs300_pct >= 0.5:
            idx_label = "大盘温和上涨"
            idx_score = 70
        elif hs300_pct > -0.5:
            idx_label = "大盘震荡"
            idx_score = 55
        elif hs300_pct > -2.0:
            idx_label = "大盘回调"
            idx_score = 40
        else:
            idx_label = "大盘大幅下跌"
            idx_score = 25

        # ----------------- 综合情绪评分 -----------------
        score = float((width_score + lmt_score + idx_score) / 3.0)

        if score >= 75:
            level = "情绪亢奋偏多"
        elif score >= 60:
            level = "情绪偏多"
        elif score >= 45:
            level = "情绪中性"
        elif score >= 30:
            level = "情绪偏空"
        else:
            level = "情绪恐慌偏空"

        signal = f"{width_label}，{idx_label}，{lmt_label}"

        raw = {
            "adv": adv,
            "dec": dec,
            "total": total,
            "adv_ratio": adv_ratio,
            "limit_up": lu,
            "limit_down": ld,
            "hs300_pct": hs300_pct,
            "width_label": width_label,
            "lmt_label": lmt_label,
            "idx_label": idx_label,
        }

        details = {
            "level": level,
            "adv": adv,
            "dec": dec,
            "total": total,
            "adv_ratio": adv_ratio,
            "limit_up": lu,
            "limit_down": ld,
            "hs300_pct": hs300_pct,
            "width_label": width_label,
            "lmt_label": lmt_label,
            "idx_label": idx_label,
            "width_score": width_score,
            "lmt_score": lmt_score,
            "idx_score": idx_score,
        }

        # ========= 详细报告（B 版） =========
        report_block = (
            f"  - market_sentiment: {score:.2f}（{level}）\n"
            f"      · 涨跌家数：上涨 {int(adv)}；下跌 {int(dec)}；总数 {int(total)}；上涨占比 {adv_ratio:.2%}（{width_label}）\n"
            f"      · 涨跌停结构：涨停 {int(lu)}；跌停 {int(ld)}（{lmt_label}）\n"
            f"      · HS300 代理涨跌：{hs300_pct:.2f}%（{idx_label}）\n"
            f"      · 情绪综合判断：{signal}\n"
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
