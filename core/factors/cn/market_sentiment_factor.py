from __future__ import annotations

from typing import Dict, Any

from core.models.factor_result import FactorResult


class MarketSentimentFactor:
    """
    A股市场情绪因子（宽度 + 涨跌停 + 权重ETF 情绪）。

    设计目标：
    - 仅依赖日级聚合数据（breadth + ETF proxy），不调用额外数据源；
    - 输出 0–100 分的相对情绪强度；
    - 文本信号可用于报告解释。
    """

    name = "market_sentiment"

    def compute_from_daily(self, processed: Dict[str, Any]) -> FactorResult:
        f = processed.get("features", {}) or {}

        # ---- 1. 宽度：涨跌家数 ----
        adv = int(f.get("adv", 0) or 0)
        dec = int(f.get("dec", 0) or 0)
        lup = int(f.get("limit_up", 0) or 0)
        ldn = int(f.get("limit_down", 0) or 0)
        total = int(f.get("total", 0) or 0)

        if total <= 0 or (adv + dec) <= 0:
            score = 50.0
            signal = "市场宽度数据缺失（视为情绪中性）"
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
                },
            )

        adv_ratio = adv / total
        dec_ratio = dec / total

        # 以 (adv - dec) / total 作为宽度原始值，控制在 [-0.5, 0.5] 区间
        breadth_raw = (adv - dec) / total
        breadth_raw = max(-0.5, min(0.5, breadth_raw))
        # 映射到 [-1, 1]
        breadth_score = breadth_raw / 0.3
        breadth_score = max(-1.0, min(1.0, breadth_score))

        # ---- 2. 涨跌停扩散（轻权重） ----
        # 对于大多数交易日，涨跌停占比极低，这部分只作为情绪极端时的“加分/减分项”
        limit_base = max(total, 10)
        limit_raw = (lup - ldn) / limit_base
        limit_raw = max(-0.1, min(0.1, limit_raw))
        limit_score = limit_raw / 0.06
        limit_score = max(-1.0, min(1.0, limit_score))

        # ---- 3. 权重 ETF 情绪（hs300_pct） ----
        hs300_pct = float(f.get("hs300_pct", 0.0) or 0.0)
        # 以 ±3% 作为情绪极值参考
        hs_raw = hs300_pct / 3.0
        hs_raw = max(-1.5, min(1.5, hs_raw))
        hs_score = max(-1.0, min(1.0, hs_raw))

        # ---- 4. 综合情绪得分 ----
        # 宽度为主（50%），权重ETF 次之（30%），涨跌停为轻量修正（20%）
        combined = 0.5 * breadth_score + 0.3 * hs_score + 0.2 * limit_score
        score = self._map_to_0_100(combined)

        # 文本信号
        if score >= 75:
            desc = "市场情绪偏热，风险偏好较高"
        elif score >= 60:
            desc = "市场情绪偏暖，做多意愿较强"
        elif score >= 45:
            desc = "市场情绪中性略偏谨慎"
        elif score >= 30:
            desc = "市场情绪偏冷，资金偏防御"
        else:
            desc = "市场情绪极度低迷或恐慌"

        signal = (
            f"{desc}（adv={adv} / dec={dec} / total={total}，"
            f"涨停={lup} 跌停={ldn}，"
            f"adv_ratio={adv_ratio:.2f}，hs300_pct={hs300_pct:.2f}%）"
        )

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
                "breadth_score": breadth_score,
                "limit_score": limit_score,
                "hs300_pct": hs300_pct,
                "combined_raw": combined,
            },
        )

    @staticmethod
    def _map_to_0_100(raw: float) -> float:
        """将 [-1, 1] 范围的原始情绪分数映射到 [0, 100]。"""
        raw_clamped = max(-1.0, min(1.0, raw))
        return round(50.0 + raw_clamped * 50.0, 2)
