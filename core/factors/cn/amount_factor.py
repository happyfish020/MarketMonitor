# -*- coding: utf-8 -*-
"""UnifiedRisk V12 - AmountFactor (CN)

用“成交额相对强弱”描述当日量能环境（结构解释用）。

计算：
- total = amount_raw.total_amount
- maN  = 最近 N 日成交额均值（不足 N 用可用窗口均值）
- ratio = total / maN

输出给 StructureFactsBuilder：details.state = expanding / neutral / contracting。

注意：该因子仅用于解释与结构验证，不直接作为进攻/调仓依据。
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from core.factors.factor_base import FactorBase
from core.factors.factor_result import FactorResult


class AmountFactor(FactorBase):
    """日度量能（成交额）结构因子"""

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
        if isinstance(window, list):
            for row in window:
                if isinstance(row, dict):
                    v = self._to_float(row.get("total_amount"))
                    if v is not None:
                        series.append(v)

        # 允许只有 total 没有 window：此时只能给出弱解释
        ma = self._mean(series[-self.ma_window :]) if series else None
        ratio = (total / ma) if (total is not None and ma not in (None, 0.0)) else None

        details: Dict[str, Any] = {
            "data_status": "OK" if total is not None else "MISSING",
            "amount_total": total,
            "amount_ma20": ma,
            "amount_ratio": ratio,
            "_raw_data": json.dumps(raw, ensure_ascii=False)[:2000],
        }

        # score 设计：以 ratio 为主，绝对量作为兜底
        #（单位不确定时，ratio 更稳；绝对量只做“极端高量/极端低量”的辅助）
        score = 50.0
        reasons: List[str] = []

        if total is None:
            score = 50.0
            reasons.append("missing_total")
        else:
            # ratio 信号
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
            else:
                # 无 ma 时，尝试用绝对量给粗略分档（阈值可后续配置化）
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

        # state：与结构层语义对齐（结构层优先读 state）
        state = "neutral"
        if score >= 70:
            state = "expanding"
        elif score <= 30:
            state = "contracting"

        details["state"] = state
        details["reasons"] = reasons

        return self.build_result(score=score, details=details)
