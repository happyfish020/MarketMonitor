from __future__ import annotations

from typing import Dict

from core.factors.factor_result import FactorResult


class StructureFactsBuilder:
    """
    UnifiedRisk V12 · StructureFactsBuilder（Phase-2 / 冻结）

    职责：
    - 将 Phase-2 已完成的 FactorResult
      翻译为“结构事实（state + meaning）”
    - 只做语义映射，不参与计算、不参与裁决

    输出：
    - context.slots["structure"]
    """

    # ===============================
    # Public API
    # ===============================
    def build(self, *, factors: Dict[str, FactorResult]) -> Dict[str, Dict[str, str]]:
        structure: Dict[str, Dict[str, str]] = {}

        if "index_tech_raw" in factors:
            structure["index_tech"] = self._map_index_tech(factors["index_tech_raw"])

        if "turnover_raw" in factors:
            structure["turnover"] = self._map_turnover(factors["turnover_raw"])

        if "breadth_raw" in factors:
            structure["breadth"] = self._map_breadth(factors["breadth_raw"])

        if "north_nps_raw" in factors:
            structure["north_nps"] = self._map_north_nps(factors["north_nps_raw"])

        if "trend_in_force_raw" in factors:
            structure["trend_in_force"] = self._map_north_nps(factors["trend_in_force_raw"])
       
        # ---- 综合一句话（非计算，仅归纳）----
        structure["_summary"] = self._build_summary(structure)

        return structure

    # ===============================
    # Factor → Structure 映射
    # ===============================
    def _map_index_tech(self, fr: FactorResult) -> Dict[str, str]:
        if fr.level == "HIGH":
            return {
                "state": "strong",
                "meaning": "成长/科技方向相对指数占优，结构偏强"
            }
        if fr.level == "LOW":
            return {
                "state": "weak",
                "meaning": "成长/科技方向明显弱于指数，结构承压"
            }
        return {
            "state": "neutral",
            "meaning": "成长/科技与指数表现大致同步"
        }

    def _map_turnover(self, fr: FactorResult) -> Dict[str, str]:
        if fr.level == "LOW":
            return {
                "state": "contracting",
                "meaning": "成交缩量，进攻动能不足"
            }
        if fr.level == "HIGH":
            return {
                "state": "expanding",
                "meaning": "成交活跃，资金参与度较高"
            }
        return {
            "state": "neutral",
            "meaning": "成交处于中性水平"
        }

    def _map_breadth(self, fr: FactorResult) -> Dict[str, str]:
        if fr.level == "LOW":
            return {
                "state": "not_broken",
                "meaning": "广度偏弱但尚未出现趋势性破坏"
            }
        if fr.level == "HIGH":
            return {
                "state": "healthy",
                "meaning": "市场广度健康，趋势结构稳定"
            }
        return {
            "state": "neutral",
            "meaning": "市场广度中性"
        }

    def _map_north_nps(self, fr: FactorResult) -> Dict[str, str]:
        if fr.level == "LOW":
            return {
                "state": "outflow",
                "meaning": "资金偏向流出，需关注持续性"
            }
        if fr.level == "HIGH":
            return {
                "state": "inflow",
                "meaning": "资金呈现主动流入迹象"
            }
        return {
            "state": "neutral",
            "meaning": "资金以调仓为主，未出现连续撤退"
        }

    # ===============================
    # Summary（语义归纳，不是计算）
    # ===============================
    def _build_summary(self, structure: Dict[str, Dict[str, str]]) -> Dict[str, str]:
        breadth = structure.get("breadth", {}).get("state")
        turnover = structure.get("turnover", {}).get("state")

        if breadth == "not_broken" and turnover == "contracting":
            return {
                "meaning": "成长中性，动能不足，未见趋势性破坏"
            }

        if breadth == "healthy" and turnover == "expanding":
            return {
                "meaning": "结构健康，动能改善"
            }

        return {
            "meaning": "结构中性，仍需进一步观察"
        }
