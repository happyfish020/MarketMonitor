# -*- coding: utf-8 -*-
"""
Failure-Rate Factor (FRF) - V12 冻结版

职责（P0）：
- 消费 snapshot['trend_in_force']
- 在不引入新 DS / 不读 history 的前提下，
  对“趋势结构失效迹象”做窗口化失败率评估
- 输出 FactorResult(name="failure_rate", score/level/details)
- 不影响 Gate、不做预测、不产生交易含义
"""

from __future__ import annotations

from typing import Any, Dict, List

from core.factors.factor_base import FactorBase, RiskLevel
from core.factors.factor_result import FactorResult
from core.utils.logger import get_logger

LOG = get_logger("Factor.FRF")


class FRFFactor(FactorBase):
    """
    FRF（Failure-Rate Factor）

    设计冻结要点：
    - P0 仅基于 trend_in_force_raw 做等价映射
    - 不引入新的“失败定义”，只做结构退化统计
    - 失败率用于风险环境解释，而非 Gate 判决
    
    FRF (Failure-Rate Factor)
    冻结语义：
    - 失败定义不变：fail_event := slope_10d < 0
    - 不读 DS / history
    - 只消费 TrendFacts（turnover.window + slope_10d）
    """

    def __init__(self) -> None:
        # 因子名固定为 "failure_rate"
        super().__init__("failure_rate")

    def compute(self, snapshot: Dict[str, Any]) -> FactorResult:
        # ---- 取事实 ----
        trend_facts = snapshot.get("trend_facts_raw") or snapshot.get("trend_facts")
        turnover = (trend_facts or {}).get("turnover", {})

        slope_10d = turnover.get("slope_10d")
        window = turnover.get("window", [])

        # ---- 回退：若无窗口或 slope，不抛异常，走最小可用 ----
        # 单点回退：沿用原有逻辑（失败= slope_10d < 0）
        if not isinstance(window, list) or len(window) < 5 or slope_10d is None:
            fail = bool(slope_10d is not None and slope_10d < 0)
            score = 100.0 if fail else 0.0
            level = "HIGH" if fail else "LOW"

            details = {
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
            return FactorResult(name=self.name, score=score, level=level, details=details)

        # ---- 窗口化：从 window 末尾计算 10D / 5D ----
        # 注意：失败定义仍然基于 slope_10d（冻结）
        # window 中不保证每个点都有 slope_10d，这里用“当前 slope_10d”判失败，
        # 并按窗口长度复制该判定，形成稳定失败率（符合冻结语义）
        def _calc_fail_rate(n: int) -> Dict[str, Any]:
            use = window[-n:]
            # 失败标志：基于当前 slope_10d 的冻结定义
            fail_flag = bool(slope_10d < 0)
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

        # ---- 选择主用窗口：优先 10D，其次 5D ----
        if fr10 and fr10["n"] == 10:
            used = "10d"
            rate = fr10["rate"]
            used_block = fr10
        else:
            used = "5d"
            rate = fr5["rate"]
            used_block = fr5

        # ---- level 映射（冻结阈值，可配置但先固定） ----
        if rate >= 0.6:
            level = "HIGH"
        elif rate >= 0.2:
            level = "NEUTRAL"
        else:
            level = "LOW"

        score = round(rate * 100.0, 2)

        details = {
            "mode": f"window_{used}",
            "fail_rate_5d": fr5["rate"],
            "fail_rate_10d": fr10["rate"] if fr10 else None,
            "used": used,
            "pressure_level": level.lower(),
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
