"""
UnifiedRisk V12 FULL
A-share ActionHint Builder (Decision Expression Layer)

制度定位（冻结）：
- 行为建议构建器（ActionHint Builder）
- 只负责把 Gate / Regime / FactorResult
  翻译为“允许/禁止/约束”的行为表达
- 不参与任何制度裁决
"""

from typing import Dict, Any, List, Optional


class AshareActionHintBuilder:
    """
    A股行为建议构建器（表达层）

    ⚠️ 铁律：
    - 本类不产生 Gate / Regime / Factor
    - 所有输入视为只读
    - 只做“制度结果 → 行为建议”的翻译
    """

    def __init__(self) -> None:
        """
        ActionHint Builder 不依赖任何计算模块
        所有行为建议均来自制度结果的表达
        """
        pass

    def build(
        self,
        *,
        snapshot: Any,
        policy_result: Any,
        trade_date: str,
        market: str = "CN_A",
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        构建 A 股行为建议（ActionHintResult）

        输入（冻结接口）：
        - snapshot: MarketSnapshot（结构事实，只读，仅用于解释）
        - policy_result: PolicyDecisionBundle（制度裁决）
        - trade_date: 交易日
        - market: 市场标识
        - context: 运行上下文（仅透传）

        输出（冻结结构）：
        - ActionHintResult（Dict[str, Any]）
        """

        gate_decision = getattr(policy_result, "gate_decision", None)
        regime_result = getattr(policy_result, "regime_result", None)

        # ---------------------------
        # 1. Allowed / Forbidden Actions（表达层）
        # ---------------------------
        allowed_actions: List[str] = []
        forbidden_actions: List[str] = []

        if gate_decision is not None:
            gate = getattr(gate_decision, "gate", None)

            # 注意：这里不是“判断 Gate”，而是“翻译 Gate 结果”
            if gate == "NORMAL":
                allowed_actions.extend([
                    "OPEN_POSITION",
                    "ADD_POSITION",
                    "HOLD_POSITION",
                ])
            elif gate == "CAUTION":
                allowed_actions.append("HOLD_POSITION")
                forbidden_actions.extend([
                    "AGGRESSIVE_ADD",
                ])
            elif gate == "PLAN_B":
                allowed_actions.append("REDUCE_POSITION")
                forbidden_actions.extend([
                    "OPEN_POSITION",
                    "ADD_POSITION",
                ])
            elif gate == "FREEZE":
                forbidden_actions.extend([
                    "OPEN_POSITION",
                    "ADD_POSITION",
                    "HOLD_POSITION",
                ])

        # ---------------------------
        # 2. Position Guidance（仓位指引）
        # ---------------------------
        position_guidance: Dict[str, Any] = {
            "max_exposure": None,
            "position_note": None,
        }

        if gate_decision is not None:
            constraints = getattr(gate_decision, "constraints", None)
            if isinstance(constraints, dict):
                position_guidance.update(constraints)

        # ---------------------------
        # 3. Explanation（制度解释字段）
        # ---------------------------
        explanation_parts: List[str] = []

        if regime_result is not None:
            regime = getattr(regime_result, "regime", None)
            if regime is not None:
                explanation_parts.append(f"Market regime: {regime}")

        if gate_decision is not None:
            reason = getattr(gate_decision, "reason", None)
            if reason:
                explanation_parts.append(f"Gate reason: {reason}")

        explanation = " | ".join(explanation_parts)

        # ---------------------------
        # 4. Risk Notes（风险提示）
        # ---------------------------
        risk_notes: List[str] = []

        factor_results = getattr(policy_result, "factor_results", None)
        if isinstance(factor_results, dict):
            for name, fr in factor_results.items():
                level = getattr(fr, "level", None)
                if level == "HIGH":
                    risk_notes.append(f"High risk factor detected: {name}")

        # ---------------------------
        # 5. ActionHintResult（冻结输出）
        # ---------------------------
        action_hint: Dict[str, Any] = {
            "allowed_actions": allowed_actions,
            "forbidden_actions": forbidden_actions,
            "position_guidance": position_guidance,
            "explanation": explanation,
            "risk_notes": risk_notes,
        }

        return action_hint
