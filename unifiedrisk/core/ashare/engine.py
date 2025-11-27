
"""AShareDailyEngine (UnifiedRisk v4.0).

This engine glues together:

- core.ashare.data_fetcher.DataFetcher
- common.scoring (for level mapping)
- reporting.daily_writer.DailyReportWriter

It is deliberately light on external dependencies so that you can drop it
into your existing `unifiedrisk` package with minimal friction.
"""
from __future__ import annotations

import datetime as _dt
import logging
from dataclasses import dataclass, field
from typing import Any, Dict

from .data_fetcher import DataFetcher
from unifiedrisk.common.scoring import RiskSummary
from unifiedrisk.reporting.daily_writer import DailyReportWriter


LOG = logging.getLogger("UnifiedRisk.AShare")


@dataclass
class AShareRiskScorer:
    """Compute a single total score from the raw payload.

    The current implementation is intentionally simple and conservative:
    - If a factor has no numeric input yet, it contributes 0.
    - Factor weights can be tuned later without touching the engine.

    You can replace the internals of this class with your full v4 logic
    (11-factor model, T+1 高频因子等)，只要保持 `compute()` 的输出结构不变：

        {
            "total_score": float,
            "risk_level": str,
            "risk_description": str,
            "factor_scores": {factor_name: score_int},
        }
    """

    raw: Dict[str, Any]
    factor_scores: Dict[str, int] = field(default_factory=dict)

    def compute(self) -> Dict[str, Any]:
        # --- 1) gather per-factor scores (placeholders = 0) ---------------
        self.factor_scores = {
            "turnover": self._score_turnover(),
            "northbound": self._score_northbound(),
            "margin": self._score_margin(),
            "liquidity": self._score_liquidity(),
            "fund_flow": self._score_fund_flow(),
            "style": self._score_style(),
            "valuation": self._score_valuation(),
            "volume_price": self._score_volume_price(),
            "macro_reflection": self._score_macro_reflection(),
            "tech_pattern": self._score_tech_pattern(),
        }

        # --- 2) total score (simple sum for now) ---------------------------
        total = float(sum(self.factor_scores.values()))

        # --- 3) risk level mapping via common.scoring ---------------------
        summary = RiskSummary.from_score(total)

        result = summary.to_dict()
        result["factor_scores"] = self.factor_scores
        return result

    # ---- Individual factor scoring stubs ---------------------------------
    # NOTE: All stubs below return 0 for now. They are placeholders where you
    #       will inject your own detailed scoring rules.

    def _score_turnover(self) -> int:
        return 0

    def _score_northbound(self) -> int:
        return 0

    def _score_margin(self) -> int:
        return 0

    def _score_liquidity(self) -> int:
        return 0

    def _score_fund_flow(self) -> int:
        return 0

    def _score_style(self) -> int:
        return 0

    def _score_valuation(self) -> int:
        return 0

    def _score_volume_price(self) -> int:
        return 0

    def _score_macro_reflection(self) -> int:
        return 0

    def _score_tech_pattern(self) -> int:
        return 0


class AShareDailyEngine:
    """High-level façade used by your main() entry.

    Typical usage:

        engine = AShareDailyEngine()
        payload = engine.run()  # returns dict with raw + score
    """

    def __init__(self) -> None:
        self.fetcher = DataFetcher()
        self.writer = DailyReportWriter()

    def run(self, trade_date: str | None = None) -> Dict[str, Any]:
        LOG.info("Running AShareDailyEngine (v4.0 with DataFetcher)")

        if trade_date:
            as_of = _dt.date.fromisoformat(trade_date)
        else:
            as_of = None

        raw = self.fetcher.build_payload(as_of=as_of)
        scorer = AShareRiskScorer(raw=raw)
        score = scorer.compute()

        payload = {"raw": raw, "score": score}

        # 写出日级报告（可根据需要增加不同类型的报告 writer）
        try:
            self.writer.write_daily_report(payload)
        except Exception as exc:  # pragma: no cover - 防御性
            LOG.warning("Failed to write daily report: %s", exc)

        return payload
