# -*- coding: utf-8 -*-
"""UnifiedRisk V12 - AmountFactor (CN)

Uses relative turnover strength to describe liquidity environment for structure interpretation.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from core.factors.factor_base import FactorBase
from core.factors.factor_result import FactorResult


class AmountFactor(FactorBase):
    """Daily amount structure factor."""

    def __init__(self, ma_window: int = 20) -> None:
        super().__init__(name="amount")
        self.ma_window = int(ma_window) if ma_window and ma_window > 0 else 20

    @staticmethod
    def _to_float(v: Any) -> Optional[float]:
        try:
            if v is None:
                return None
            return float(v)
        except Exception:
            return None

    @staticmethod
    def _mean(vals: List[float]) -> Optional[float]:
        if not vals:
            return None
        return sum(vals) / float(len(vals))

    def compute(self, snapshot: Dict[str, Any]) -> FactorResult:
        raw = snapshot.get("amount_raw") or {}
        if not isinstance(raw, dict):
            raw = {}

        total = self._to_float(raw.get("total_amount"))
        window = raw.get("window")

        series: List[float] = []
        rows_asc: List[tuple[str, float]] = []
        if isinstance(window, list):
            for row in window:
                if not isinstance(row, dict):
                    continue
                v = self._to_float(row.get("total_amount"))
                if v is not None:
                    series.append(v)
                td = row.get("trade_date") or row.get("date") or row.get("dt")
                if td and v is not None:
                    rows_asc.append((str(td), float(v)))

        if rows_asc:
            rows_asc.sort(key=lambda x: x[0])  # old -> new
            series_ordered = [x[1] for x in rows_asc]
        else:
            series_ordered = list(series)

        ma = self._mean(series_ordered[-self.ma_window:]) if series_ordered else None
        ma60 = self._mean(series_ordered[-60:]) if series_ordered else None
        ratio = (total / ma) if (total is not None and ma not in (None, 0.0)) else None
        ratio_ma60 = (total / ma60) if (total is not None and ma60 not in (None, 0.0)) else None

        latest_val = total
        prev_val: Optional[float] = None
        delta_prev: Optional[float] = None
        ratio_prev: Optional[float] = None

        try:
            if len(rows_asc) >= 2:
                latest_val = float(rows_asc[-1][1])
                prev_val = float(rows_asc[-2][1])
                delta_prev = latest_val - prev_val
            elif isinstance(window, list) and len(window) >= 2:
                first = window[0] if isinstance(window[0], dict) else {}
                second = window[1] if isinstance(window[1], dict) else {}
                fv = self._to_float(first.get("total_amount") or first.get("amount") or first.get("total"))
                sv = self._to_float(second.get("total_amount") or second.get("amount") or second.get("total"))
                if fv is not None and sv is not None:
                    latest_val = float(fv)
                    prev_val = float(sv)
                    delta_prev = latest_val - prev_val
        except Exception:
            pass

        if total is None and latest_val is not None:
            total = latest_val

        if prev_val is not None and ma not in (None, 0.0):
            try:
                ratio_prev = float(prev_val) / float(ma)
            except Exception:
                ratio_prev = None

        ratio_slope_3d: Optional[float] = None
        try:
            if ma not in (None, 0.0) and len(series_ordered) >= 5:
                recent = series_ordered[-5:]
                ratios = [float(x) / float(ma) for x in recent]
                ratio_slope_3d = float(ratios[-1] - (sum(ratios[-4:-1]) / 3.0))
        except Exception:
            ratio_slope_3d = None

        amount_trend_signal = "neutral"
        if isinstance(ratio, float):
            if ratio >= 1.08 and (ratio_slope_3d is None or ratio_slope_3d >= 0.0):
                amount_trend_signal = "strengthening"
            elif ratio <= 0.95:
                amount_trend_signal = "weakening"
            elif ratio_slope_3d is not None and ratio_slope_3d <= -0.02:
                amount_trend_signal = "weakening"

        details: Dict[str, Any] = {
            "data_status": "OK" if total is not None else "MISSING",
            "amount_total": total,
            "amount_ma20": ma,
            "amount_ma60": ma60,
            "amount_ratio": ratio,
            "amount_ratio_ma60": ratio_ma60,
            "amount_prev": prev_val,
            "amount_delta_prev": delta_prev,
            "amount_ratio_prev": ratio_prev,
            "amount_ratio_slope_3d": ratio_slope_3d,
            "amount_trend_signal": amount_trend_signal,
            "_raw_data": json.dumps(raw, ensure_ascii=False)[:2000],
        }

        score = 50.0
        reasons: List[str] = []

        if total is None:
            score = 50.0
            reasons.append("missing_total")
        else:
            if ratio is not None:
                if ratio >= 1.30:
                    score = 90.0
                    reasons.append("ratio>=1.30")
                elif ratio >= 1.15:
                    score = 75.0
                    reasons.append("ratio>=1.15")
                elif ratio <= 0.80:
                    score = 25.0
                    reasons.append("ratio<=0.80")
                elif ratio <= 0.90:
                    score = 35.0
                    reasons.append("ratio<=0.90")
                else:
                    score = 55.0
                    reasons.append("ratio~1.00")

                if amount_trend_signal == "weakening":
                    score = max(0.0, score - 8.0)
                    reasons.append("trend_signal=weakening")
                elif amount_trend_signal == "strengthening":
                    score = min(100.0, score + 5.0)
                    reasons.append("trend_signal=strengthening")
            else:
                if total >= 10000:
                    score = 85.0
                    reasons.append("total>=10000")
                elif total >= 8000:
                    score = 70.0
                    reasons.append("total>=8000")
                elif total <= 4000:
                    score = 30.0
                    reasons.append("total<=4000")
                else:
                    score = 50.0
                    reasons.append("no_ma_use_total")

        state = "neutral"
        if score >= 70:
            state = "expanding"
        elif score <= 30:
            state = "contracting"

        details["state"] = state
        details["reasons"] = reasons

        return self.build_result(score=score, details=details)
