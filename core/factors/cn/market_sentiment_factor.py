from __future__ import annotations

from typing import Dict, Any
from core.models.factor_result import FactorResult


class MarketSentimentFactor:
    """A股市场情绪因子（涨跌家数 + 涨跌停宽度）。"""

    name = "market_sentiment"

    def compute_from_daily(self, processed: Dict[str, Any]) -> FactorResult:
        f = processed.get("features", {})

        adv = int(f.get("adv", 0) or 0)
        dec = int(f.get("dec", 0) or 0)
        total = int(f.get("total_stocks", 0) or 0)
        lup = int(f.get("limit_up", 0) or 0)
        ldn = int(f.get("limit_down", 0) or 0)

        total_safe = max(total, 1)
        adv_ratio = adv / total_safe
        dec_ratio = dec / total_safe

        if adv_ratio > 0.65 and lup >= 30 and ldn <= 5:
            score = 85.0
            desc = "普涨 + 涨停活跃，情绪极度乐观"
        elif adv_ratio > 0.55:
            score = 70.0
            desc = "上涨占优，情绪偏乐观"
        elif 0.45 <= adv_ratio <= 0.55:
            score = 50.0
            desc = "涨跌均衡，情绪中性"
        elif dec_ratio > 0.55 and ldn >= 10:
            score = 30.0
            desc = "下跌占优 + 跌停增多，情绪偏恐慌"
        else:
            score = 40.0
            desc = "略偏弱，情绪谨慎"

        signal = f"{desc}（score={score:.1f}）"

        return FactorResult(
            name=self.name,
            score=score,
            signal=signal,
            raw={
                "adv": adv,
                "dec": dec,
                "limit_up": lup,
                "limit_down": ldn,
                "total": total,
                "adv_ratio": adv_ratio,
                "dec_ratio": dec_ratio,
            },
        )