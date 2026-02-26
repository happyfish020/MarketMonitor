# -*- coding: utf-8 -*-
"""Failure-Rate Factor (FRF) - V12."""

from __future__ import annotations

from typing import Any, Dict

from core.factors.factor_base import FactorBase
from core.factors.factor_result import FactorResult


class FRFFactor(FactorBase):
    def __init__(self) -> None:
        super().__init__("failure_rate")

    def compute(self, snapshot: Dict[str, Any]) -> FactorResult:
        trend_facts = snapshot.get("trend_in_force") or {}
        amount = trend_facts.get("amount", {}) if isinstance(trend_facts, dict) else {}
        price = trend_facts.get("price", {}) if isinstance(trend_facts, dict) else {}

        a_slope_10d = amount.get("slope_10d")
        p_slope_10d = price.get("slope_10d") if isinstance(price, dict) else None
        window = amount.get("window", [])

        # Combined fail-event: only mark hard fail when price and amount both deteriorate.
        def _combined_fail() -> bool:
            if isinstance(a_slope_10d, (int, float)) and isinstance(p_slope_10d, (int, float)):
                return float(a_slope_10d) < 0 and float(p_slope_10d) < 0
            if isinstance(a_slope_10d, (int, float)):
                return float(a_slope_10d) < 0
            return False

        fail = _combined_fail()

        if not isinstance(window, list) or len(window) < 5 or a_slope_10d is None:
            score = 100.0 if fail else 0.0
            level = "HIGH" if fail else "LOW"
            if a_slope_10d is None:
                state = "DATA_MISSING"
                meaning = "趋势事实数据不足，无法评估结构失败率"
            elif fail:
                state = "UNSTABLE"
                meaning = "价量同步走弱，出现结构失败迹象"
            else:
                state = "STABLE"
                meaning = "未观察到价量同步失败迹象"

            return FactorResult(
                name=self.name,
                score=score,
                level=level,
                details={
                    "data_status": "OK" if a_slope_10d is not None else "DATA_NOT_CONNECTED",
                    "state": state,
                    "meaning": meaning,
                    "mode": "snapshot_fallback",
                    "fail_event": fail,
                    "amount_slope_10d": a_slope_10d,
                    "price_slope_10d": p_slope_10d,
                    "_raw_data": {
                        "amount": {
                            "slope_10d": a_slope_10d,
                            "window_len": len(window) if isinstance(window, list) else 0,
                        },
                        "price": {"slope_10d": p_slope_10d},
                    },
                },
            )

        def _calc_fail_rate(n: int) -> Dict[str, Any]:
            use = window[-n:]
            flags = [fail for _ in use]
            rate = sum(1 for f in flags if f) / len(flags) if flags else 0.0
            dates = [r.get("trade_date") for r in use if isinstance(r, dict)]
            return {"n": len(use), "rate": rate, "flags": flags, "dates": dates}

        fr10 = _calc_fail_rate(10) if len(window) >= 10 else None
        fr5 = _calc_fail_rate(5)

        if fr10 and fr10["n"] == 10:
            used = "10d"
            rate = fr10["rate"]
            used_block = fr10
        else:
            used = "5d"
            rate = fr5["rate"]
            used_block = fr5

        if rate >= 0.6:
            level = "HIGH"
            state = "UNSTABLE"
            meaning = "失败事件频繁出现，趋势结构不稳定"
        elif rate >= 0.2:
            level = "NEUTRAL"
            state = "RISING"
            meaning = "失败率上升，趋势结构承压"
        else:
            level = "LOW"
            state = "STABLE"
            meaning = "失败率较低，趋势结构保持稳定"

        score = round(rate * 100.0, 2)

        return FactorResult(
            name=self.name,
            score=score,
            level=level,
            details={
                "data_status": "OK",
                "state": state,
                "meaning": meaning,
                "mode": f"window_{used}",
                "fail_rate_5d": fr5["rate"],
                "fail_rate_10d": fr10["rate"] if fr10 else None,
                "used": used,
                "_raw_data": {
                    "amount_slope_10d": a_slope_10d,
                    "price_slope_10d": p_slope_10d,
                    "window_len": len(window),
                    "used_block": used_block,
                },
            },
        )
