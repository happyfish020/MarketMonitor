from __future__ import annotations

from typing import Dict, Optional

from core.factors.factor_result import FactorResult


# ===============================
# Modifier 定义（命名规范合规）
# ===============================
MOD_NONE = None
MOD_SUCCESS_RATE_DECLINING = "success_rate_declining"
MOD_DISTRIBUTION_RISK = "distribution_risk"
MOD_HIGH_EXECUTION_RISK = "high_execution_risk"


class StructureFactsBuilder:
    """
    UnifiedRisk V12 · StructureFactsBuilder（制度语义感知版）

    职责：
    - FactorResult → 结构事实（state）
    - 制度上下文（distribution / drs）→ modifier
    - state + modifier → 制度安全语义（meaning）

    设计原则：
    - 不暴露 phase 编号
    - modifier 只表达“制度含义”
    - 不产生进攻或行为许可
    """

    # ===============================
    # Public API
    # ===============================
    def build(
        self,
        *,
        factors: Dict[str, FactorResult],
        distribution_risk_active: bool = False,
        drs_signal: Optional[str] = None,  # GREEN / YELLOW / RED
    ) -> Dict[str, Dict[str, str]]:
        """
        参数：
        - factors: Phase-2 FactorResult
        - distribution_risk_active: 是否存在结构性分布风险
        - drs_signal: 日度风险信号（GREEN / YELLOW / RED）
        """

        modifier = self._resolve_modifier(
            distribution_risk_active=distribution_risk_active,
            drs_signal=drs_signal,
        )

        structure: Dict[str, Dict[str, str]] = {}

        if "index_tech" in factors:
            structure["index_tech"] = self._map_index_tech(
                factors["index_tech"], modifier
            )

        if "turnover" in factors:
            structure["turnover"] = self._map_turnover(
                factors["turnover"], modifier
            )

        if "breadth" in factors:
            structure["breadth"] = self._map_breadth(
                factors["breadth"], modifier
            )

        if "north_nps" in factors:
            structure["north_nps"] = self._map_north_nps(
                factors["north_nps"], modifier
            )

        if "trend_in_force" in factors:
            structure["trend_in_force"] = self._map_trend_in_force(
                factors["trend_in_force"], modifier
            )

        if "failure_rate" in factors:
            structure["failure_rate"] = self._map_failure_rate(
                factors["failure_rate"]
            )

        # ---- 结构总结（永远制度中性）----
        structure["_summary"] = self._build_summary(structure, modifier)

        return structure

    # ===============================
    # Modifier 解析（命名规范合规）
    # ===============================
    def _resolve_modifier(
        self,
        *,
        distribution_risk_active: bool,
        drs_signal: Optional[str],
    ) -> Optional[str]:
        """
        modifier 优先级（高 → 低）：
        1. distribution_risk
        2. drs_signal = RED
        3. drs_signal = YELLOW
        """
        if distribution_risk_active:
            return MOD_DISTRIBUTION_RISK

        if drs_signal == "RED":
            return MOD_HIGH_EXECUTION_RISK

        if drs_signal == "YELLOW":
            return MOD_SUCCESS_RATE_DECLINING

        return MOD_NONE

    # ===============================
    # Factor → Structure 映射
    # ===============================
    def _map_index_tech(
        self,
        fr: FactorResult,
        modifier: Optional[str],
    ) -> Dict[str, str]:
        if fr.level == "HIGH":
            return {
                "state": "strong",
                "modifier": modifier,
                "meaning": self._explain_index_tech_strong(modifier),
            }

        if fr.level == "LOW":
            return {
                "state": "weak",
                "modifier": modifier,
                "meaning": "成长/科技方向弱于指数表现，结构相对承压。",
            }

        return {
            "state": "neutral",
            "modifier": modifier,
            "meaning": "成长/科技与指数表现大致同步。",
        }

    def _map_turnover(
        self,
        fr: FactorResult,
        modifier: Optional[str],
    ) -> Dict[str, str]:
        if fr.level == "HIGH":
            return {
                "state": "expanding",
                "modifier": modifier,
                "meaning": self._explain_turnover_expanding(modifier),
            }

        if fr.level == "LOW":
            return {
                "state": "contracting",
                "modifier": modifier,
                "meaning": "成交缩量，市场参与度下降，结构进入观望阶段。",
            }

        return {
            "state": "neutral",
            "modifier": modifier,
            "meaning": "成交处于中性水平，未显示明确方向性特征。",
        }

    def _map_breadth(
        self,
        fr: FactorResult,
        modifier: Optional[str],
    ) -> Dict[str, str]:
        if fr.level == "HIGH":
            return {
                "state": "healthy",
                "modifier": modifier,
                "meaning": (
                    "市场广度未出现系统性破坏，"
                    "但扩散程度有限，需结合其他结构指标判断。"
                ),
            }

        if fr.level == "LOW":
            return {
                "state": "not_broken",
                "modifier": modifier,
                "meaning": "市场广度偏弱，但尚未出现趋势性破坏迹象。",
            }

        return {
            "state": "neutral",
            "modifier": modifier,
            "meaning": "市场广度处于中性状态。",
        }

    def _map_north_nps(
        self,
        fr: FactorResult,
        modifier: Optional[str],
    ) -> Dict[str, str]:
        if fr.level == "HIGH":
            return {
                "state": "inflow",
                "modifier": modifier,
                "meaning": (
                    "资金出现阶段性流入迹象，"
                    "但尚不足以单独构成趋势性判断依据。"
                ),
            }

        if fr.level == "LOW":
            return {
                "state": "outflow",
                "modifier": modifier,
                "meaning": "资金呈现流出倾向，需关注持续性与节奏变化。",
            }

        return {
            "state": "neutral",
            "modifier": modifier,
            "meaning": "资金以调仓为主，未出现连续性撤退或流入信号。",
        }

    def _map_trend_in_force(
        self,
        fr: FactorResult,
        modifier: Optional[str],
    ) -> Dict[str, str]:
        if fr.level == "HIGH":
            return {
                "state": "in_force",
                "modifier": modifier,
                "meaning": self._explain_trend_in_force(modifier),
            }

        if fr.level == "LOW":
            return {
                "state": "broken",
                "modifier": modifier,
                "meaning": "趋势结构已被破坏，原有趋势不再具备制度可信度。",
            }

        return {
            "state": "weakening",
            "modifier": modifier,
            "meaning": "趋势动能减弱，结构进入观察与评估阶段。",
        }

    def _map_failure_rate(self, fr: FactorResult) -> Dict[str, str]:
        if fr.level == "HIGH":
            return {
                "state": "elevated_risk",
                "meaning": "近期趋势结构失效频繁，结构性风险显著上升。",
            }

        if fr.level == "NEUTRAL":
            return {
                "state": "watch",
                "meaning": "趋势结构存在失效迹象，但尚未形成连续性破坏。",
            }

        return {
            "state": "stable",
            "meaning": "未观察到趋势结构失效迹象，结构保持稳定。",
        }

    # ===============================
    # 解释函数（state × modifier）
    # ===============================
    def _explain_index_tech_strong(self, modifier: Optional[str]) -> str:
        if modifier == MOD_DISTRIBUTION_RISK:
            return (
                "成长/科技方向相对指数表现占优，"
                "但处于分布风险与成功率下降阶段，"
                "仅反映相对表现，不构成进攻或调仓依据。"
            )
        return "成长/科技方向相对指数表现占优。"

    def _explain_turnover_expanding(self, modifier: Optional[str]) -> str:
        if modifier == MOD_DISTRIBUTION_RISK:
            return (
                "成交放大，但在分布风险阶段更可能反映分歧或调仓行为，"
                "而非新增进攻性资金。"
            )
        return "成交放大，市场参与度有所提升。"

    def _explain_trend_in_force(self, modifier: Optional[str]) -> str:
        if modifier in (MOD_DISTRIBUTION_RISK, MOD_SUCCESS_RATE_DECLINING):
            return (
                "趋势结构仍然成立，"
                "但成功率下降，需避免过度利用趋势。"
            )
        return "趋势结构仍然成立。"

    # ===============================
    # Summary（制度中性）
    # ===============================
    def _build_summary(
        self,
        structure: Dict[str, Dict[str, str]],
        modifier: Optional[str],
    ) -> Dict[str, str]:
        if modifier == MOD_DISTRIBUTION_RISK:
            return {
                "meaning": (
                    "结构未出现系统性破坏，"
                    "但已进入成功率下降与分布风险阶段。"
                )
            }

        if modifier == MOD_SUCCESS_RATE_DECLINING:
            return {
                "meaning": "结构仍在，但成功率出现下降迹象。"
            }

        return {
            "meaning": "结构中性，需结合制度状态与其他信号综合判断。"
        }
