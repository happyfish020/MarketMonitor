from __future__ import annotations

from typing import Dict, Any, Optional
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


class IndexTechFactor(FactorBase):
    """
    IndexTechFactor (V12)

    Input: input_block["index_tech"] 结构（来自 index_tech_blkbd）:
      data["hs300"]["score"], data["hs300"]["pct_1d"], ...
      data["zz500"]["score"], ...
      data["kc50"]["score"], ...

    Output:
      FactorResult.score: 0~100 (供 execution_summary_builder._score 消费)
      details: 同时提供 *_score（技术评分） 与 *_pct（1日涨跌幅，真实 pct_1d）以兼容旧链路
    """

    def __init__(self):
        super().__init__("index_tech")

    def compute(self, input_block: Dict[str, Any]) -> FactorResult:
        data = self.pick(input_block, "index_tech", {})
        assert data, "snapshot index_tech missing"
        if not isinstance(data, dict) or not data:
            return FactorResult(
                name=self.name,
                score=50.0,
                level="NEUTRAL",
                details={
                    "data_status": "DATA_NOT_CONNECTED",
                    "reason": "index_tech block missing or empty",
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
        valid = [v for v in components_score.values() if isinstance(v, float)]

        if not valid:
            return FactorResult(
                name=self.name,
                score=50.0,
                level="NEUTRAL",
                details={
                    "data_status": "DATA_INVALID",
                    "reason": "no valid index score fields",
                    "components_score": components_score,
                },
            )

        # 等权平均：仍在 -100~100
        avg_index_score = sum(valid) / len(valid)

        # 映射到 0~100：-100->0, 0->50, +100->100
        final_score = 50.0 + avg_index_score / 2.0
        final_score = max(0.0, min(100.0, final_score))

        level = self.level_from_score(final_score)

        return FactorResult(
            name=self.name,
            score=final_score,
            level=level,
            details={
                # ✅ 新语义（推荐后续全部改用这些）
                "hs300_score": hs300_score,
                "zz500_score": zz500_score,
                "kc50_score": kc50_score,
                "avg_index_score": avg_index_score,
                "used_indices": [k for k, v in components_score.items() if isinstance(v, float)],
                "aggregation": "equal_weight_avg",
                "data_status": "OK",

                # ✅ 旧链路兼容：这次真的是 pct_1d（不是 score）
                "hs300_pct": hs300_pct,
                "zz500_pct": zz500_pct,
                "kc50_pct": kc50_pct,
                "_raw_data": data
            },
        )
