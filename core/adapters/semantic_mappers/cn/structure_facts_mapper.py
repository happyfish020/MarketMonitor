# -*- coding: utf-8 -*-
"""
UnifiedRisk V12
Structure Facts Semantic Mapper (CN A-Share)

职责：
- 将 Phase-2 已计算完成的 FactorResult
  映射为“结构事实（structure.facts）”
- 生成结构性总结（_summary），用于报告中的人话总述
- 只做语义归纳，不参与裁决、不参与预测、不访问 raw DS
"""

from typing import Dict, Any
from core.factors.factor_result import FactorResult


class StructureFactsMapper:
    """
    将多个 FactorResult 聚合为结构事实（structure.facts）
    """

    REQUIRED_FACTORS = {
        "index_tech",
        "turnover",
        "breadth",
        "north_nps",
    }

    @classmethod
    def build(cls, factor_results: Dict[str, FactorResult]) -> Dict[str, Any]:
        """
        主入口：生成 structure.facts

        :param factor_results: key -> FactorResult
        :return: structure facts dict
        """
        structure: Dict[str, Any] = {}

        # ---- index_tech ----
        if "index_tech" in factor_results:
            fr = factor_results["index_tech"]
            structure["index_tech"] = cls._map_index_tech(fr)
        else:
            structure["index_tech"] = cls._missing("index_tech")

        # ---- turnover ----
        if "turnover_raw" in factor_results:
            fr = factor_results["turnover_raw"]
            structure["turnover"] = cls._map_turnover(fr)
        else:
            structure["turnover"] = cls._missing("turnover")

        # ---- breadth ----
        if "breadth_raw" in factor_results:
            fr = factor_results["breadth_raw"]
            structure["breadth"] = cls._map_breadth(fr)
        else:
            structure["breadth"] = cls._missing("breadth")

        # ---- north_nps ----
        if "north_nps_raw" in factor_results:
            fr = factor_results["north_nps_raw"]
            structure["north_nps"] = cls._map_north_nps(fr)
        else:
            structure["north_nps"] = cls._missing("north_nps")

        # ---- structure summary (人话总述) ----
        structure["_summary"] = cls._build_summary(structure)

        return structure

    # ------------------------------------------------------------------
    # 单因子 → 结构语义 映射
    # ------------------------------------------------------------------

    @staticmethod
    def _map_index_tech(fr: FactorResult) -> Dict[str, str]:
        level = fr.level

        if level == "HIGH":
            return {
                "state": "weak",
                "meaning": "成长/科技相对弱于指数，风格偏谨慎",
            }
        elif level == "LOW":
            return {
                "state": "strong",
                "meaning": "成长/科技相对占优，风格偏进攻",
            }
        else:
            return {
                "state": "neutral",
                "meaning": "成长/科技与指数表现大致同步",
            }

    @staticmethod
    def _map_turnover(fr: FactorResult) -> Dict[str, str]:
        level = fr.level

        if level == "HIGH":
            return {
                "state": "contracting",
                "meaning": "成交缩量，进攻动能不足",
            }
        elif level == "LOW":
            return {
                "state": "expanding",
                "meaning": "成交放量，具备进攻动能",
            }
        else:
            return {
                "state": "neutral",
                "meaning": "成交量处于中性水平",
            }

    @staticmethod
    def _map_breadth(fr: FactorResult) -> Dict[str, str]:
        level = fr.level

        if level == "HIGH":
            return {
                "state": "broken",
                "meaning": "广度出现明显破坏，需警惕趋势性风险",
            }
        elif level == "LOW":
            return {
                "state": "healthy",
                "meaning": "广度结构健康，未见趋势破坏",
            }
        else:
            return {
                "state": "not_broken",
                "meaning": "广度偏弱但尚未出现趋势性破坏",
            }

    @staticmethod
    def _map_north_nps(fr: FactorResult) -> Dict[str, str]:
        level = fr.level

        if level == "HIGH":
            return {
                "state": "outflow",
                "meaning": "资金呈现连续撤退迹象",
            }
        elif level == "LOW":
            return {
                "state": "inflow",
                "meaning": "资金出现回流迹象",
            }
        else:
            return {
                "state": "neutral",
                "meaning": "资金以调仓为主，未出现连续撤退",
            }

    # ------------------------------------------------------------------
    # 结构性总结（人话总述）
    # ------------------------------------------------------------------

    @staticmethod
    def _build_summary(structure: Dict[str, Any]) -> Dict[str, str]:
        """
        基于各维度 meaning，生成一条稳定的人话结构总述
        只用于解释，不参与 Gate / Action / Prediction
        """

        parts = []

        # 成长/科技风格
        it = structure.get("index_tech", {})
        if it.get("state") == "weak":
            parts.append("成长承压")
        elif it.get("state") == "strong":
            parts.append("成长占优")
        else:
            parts.append("成长中性")

        # 成交/动能
        tv = structure.get("turnover", {})
        if tv.get("state") == "contracting":
            parts.append("动能不足")
        elif tv.get("state") == "expanding":
            parts.append("动能改善")

        # 广度（趋势性判断）
        br = structure.get("breadth", {})
        if br.get("state") == "broken":
            parts.append("存在趋势破坏风险")
        else:
            parts.append("未见趋势性破坏")

        summary = "，".join(parts)

        return {
            "meaning": summary
        }

    # ------------------------------------------------------------------
    # 缺失处理（显式，不 silent）
    # ------------------------------------------------------------------

    @staticmethod
    def _missing(name: str) -> Dict[str, str]:
        return {
            "state": "missing",
            "meaning": f"{name} 因子缺失，结构判断受限",
        }
 
