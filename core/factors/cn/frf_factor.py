# -*- coding: utf-8 -*-
"""
Failure-Rate Factor (FRF) - V12 冻结版

职责（P0）：
- 消费 snapshot['trend_in_force']
- 在不引入新 DS / 不读 history 的前提下，
  对“趋势结构失效迹象”做窗口化失败率评估
- 输出 FactorResult(name="failure_rate", score/level/details)
- 不影响 Gate、不做预测、不产生交易含义
state  - STABLE
RISING
UNSTABLE
DATA_MISSING
"""

from __future__ import annotations

from typing import Any, Dict

from core.factors.factor_base import FactorBase
from core.factors.factor_result import FactorResult
from core.utils.logger import get_logger

LOG = get_logger("Factor.FRF")


class FRFFactor(FactorBase):
    """
    FRF（Failure-Rate Factor）

    冻结语义：
    - 失败定义：fail_event := slope_10d < 0
    - 不引入新失败定义
    - state 用于制度层判断（Execution / Phase-2/3）
    """

    def __init__(self) -> None:
        super().__init__("failure_rate")

    def compute(self, snapshot: Dict[str, Any]) -> FactorResult:
        # ---- 取事实 ----
        trend_facts = snapshot.get("trend_in_force") 
        turnover = (trend_facts or {}).get("turnover", {})

        slope_10d = turnover.get("slope_10d")
        window = turnover.get("window", [])

        # ==========================================================
        # 回退分支：数据不足（不抛异常）
        # ==========================================================
        if not isinstance(window, list) or len(window) < 5 or slope_10d is None:
            fail = bool(slope_10d is not None and slope_10d < 0)
            score = 100.0 if fail else 0.0
            level = "HIGH" if fail else "LOW"

            # ✅ 冻结 state 映射
            if slope_10d is None:
                state = "DATA_MISSING"
                meaning = "趋势事实数据不足，无法评估结构失效率"
            elif fail:
                state = "UNSTABLE"
                meaning = "趋势斜率为负，出现结构失效迹象"
            else:
                state = "STABLE"
                meaning = "趋势斜率为正，未观察到结构失效迹象"

            details = {
                "data_status": "OK" if slope_10d is not None else "DATA_NOT_CONNECTED",
                "state": state,
                "meaning": meaning,
                "mode": "snapshot_fallback",
                "fail_event": fail,
                "slope_10d": slope_10d,
                "_raw_data": {
                    "turnover": {
                        "slope_10d": slope_10d,
                        "window_len": len(window) if isinstance(window, list) else 0,
                    }
                },
            }

            return FactorResult(
                name=self.name,
                score=score,
                level=level,
                details=details,
            )

        # ==========================================================
        # 窗口化失败率计算
        # ==========================================================
        def _calc_fail_rate(n: int) -> Dict[str, Any]:
            use = window[-n:]
            fail_flag = bool(slope_10d < 0)  # 冻结失败定义
            flags = [fail_flag for _ in use]
            rate = sum(1 for f in flags if f) / len(flags) if flags else 0.0
            dates = [r.get("trade_date") for r in use]
            return {
                "n": len(use),
                "rate": rate,
                "flags": flags,
                "dates": dates,
            }

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

        # ==========================================================
        # level + state 映射（冻结）
        # ==========================================================
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

        details = {
            "data_status": "OK",
            "state": state,
            "meaning": meaning,
            "mode": f"window_{used}",
            "fail_rate_5d": fr5["rate"],
            "fail_rate_10d": fr10["rate"] if fr10 else None,
            "used": used,
            "_raw_data": {
                "slope_10d": slope_10d,
                "window_len": len(window),
                "used_block": used_block,
            },
        }

        return FactorResult(
            name=self.name,
            score=score,
            level=level,
            details=details,
        )
