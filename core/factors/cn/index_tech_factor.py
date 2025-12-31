# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Dict, Any, Optional, List

from core.factors.factor_base import FactorBase
from core.factors.factor_result import FactorResult


def _get_node(data: Dict[str, Any], key: str) -> Optional[Dict[str, Any]]:
    v = data.get(key)
    return v if isinstance(v, dict) else None


def _get_float(node: Optional[Dict[str, Any]], field: str) -> Optional[float]:
    if not isinstance(node, dict):
        return None
    v = node.get(field)
    return float(v) if isinstance(v, (int, float)) else None


def _clamp01(x: float) -> float:
    if x < 0.0:
        return 0.0
    if x > 100.0:
        return 100.0
    return x


class IndexTechFactor(FactorBase):
    """
    IndexTechFactor (V12)

    语义（统一口径）：
    - 这是“指数技术面因子”（趋势/均线/动量），不是“科技板块/成长风格”
    - 输入来自 IndexTechBlockBuilder（客观特征），不在本 factor 生成文案 meaning
    - 输出 score(0~100)：越高表示指数技术面越强；50 表示中性

    Input:
      snapshot["index_tech"]["hs300"]["score"] in [-100,100]
      snapshot["index_tech"]["zz500"]["score"] ...
      snapshot["index_tech"]["kc50"]["score"] ...

    Output:
      FactorResult.score: 0~100
      FactorResult.level: HIGH / NEUTRAL / LOW
      details: 数值证据（用于报告 Block 做统一解释）
    """

    def __init__(self):
        super().__init__("index_tech")

    def compute(self, input_block: Dict[str, Any]) -> FactorResult:
        data = self.pick(input_block, "index_tech", {})
        if not isinstance(data, dict) or not data:
            return FactorResult(
                name=self.name,
                score=50.0,
                level="NEUTRAL",
                details={
                    "data_status": "DATA_NOT_CONNECTED",
                    "reason": "index_tech block missing or empty",
                    "score_semantics": "higher=stronger_index_technical",
                },
            )

        n_hs300 = _get_node(data, "hs300")
        n_zz500 = _get_node(data, "zz500")
        n_kc50  = _get_node(data, "kc50")

        # 技术评分（来自 blkbd，-100~100）
        hs300_score = _get_float(n_hs300, "score")
        zz500_score = _get_float(n_zz500, "score")
        kc50_score  = _get_float(n_kc50, "score")

        # 真实 1D 涨跌幅（pct_1d，≈ -0.05~0.05）
        hs300_pct = _get_float(n_hs300, "pct_1d")
        zz500_pct = _get_float(n_zz500, "pct_1d")
        kc50_pct  = _get_float(n_kc50, "pct_1d")

        components_score = {
            "hs300": hs300_score,
            "zz500": zz500_score,
            "kc50": kc50_score,
        }
        valid: List[float] = [v for v in components_score.values() if isinstance(v, float)]

        if not valid:
            return FactorResult(
                name=self.name,
                score=50.0,
                level="NEUTRAL",
                details={
                    "data_status": "DATA_INVALID",
                    "reason": "no valid index score fields",
                    "components_score": components_score,
                    "score_semantics": "higher=stronger_index_technical",
                },
            )

        # 等权平均：仍在 -100~100
        avg_index_score = sum(valid) / len(valid)

        # 映射到 0~100：-100->0, 0->50, +100->100
        final_score = _clamp01(50.0 + avg_index_score / 2.0)

        # 强制口径：HIGH/NEUTRAL/LOW（避免 base 的枚举差异）
        if final_score >= 66.0:
            level = "HIGH"
        elif final_score <= 34.0:
            level = "LOW"
        else:
            level = "NEUTRAL"

        fr = FactorResult(
            name=self.name,
            score=final_score,
            level=level,
            details={
                "data_status": "OK",
                "score_semantics": "higher=stronger_index_technical",
                "hs300_score": hs300_score,
                "zz500_score": zz500_score,
                "kc50_score": kc50_score,
                "avg_index_score": avg_index_score,
                "used_indices": [k for k, v in components_score.items() if isinstance(v, float)],
                "aggregation": "equal_weight_avg",

                # 旧链路兼容：真实 pct_1d（不是 score）
                "hs300_pct": hs300_pct,
                "zz500_pct": zz500_pct,
                "kc50_pct": kc50_pct,
                "_raw_data": data,
            },

        )

        return fr
