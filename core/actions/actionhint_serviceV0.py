from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, TypedDict, Literal

from core.actions.summary_mapper import SummaryMapper

logger = logging.getLogger(__name__)

# ===============================
# 类型定义（冻结）
# ===============================

GateLevel = Literal["NORMAL", "CAUTION", "FREEZE"]
Action = Literal["BUY", "HOLD", "SELL", "FREEZE"]
SummaryCode = Literal["A", "N", "D"]

# 新增：执行风险等级
ExecutionCode = Literal["A", "D1", "D2", "D3"]


class ExecutionRisk(TypedDict):
    code: ExecutionCode
    horizon: str
    meaning: str


class ActionHint(TypedDict):
    gate: GateLevel
    action: Action
    summary: SummaryCode
    execution: ExecutionRisk          # ← 新增
    reason: str
    allowed: List[str]
    forbidden: List[str]
    limits: str
    conditions: str


# ===============================
# ActionHintService（冻结）
# ===============================

class ActionHintService:
    """
    UnifiedRisk V12 · ActionHintService（冻结版）

    制度铁律：
    1. ActionHint 是【行为裁决】，不是结构裁决
    2. gate 在此层只能是 str（NORMAL / CAUTION / FREEZE）
    3. 本服务不创建、不修改、不推断结构性 gate dict
    4. 所有输出必须是“给人看的中文”
    5. Execution（D1/D2/D3）描述的是【未来 2–5D 的执行风险】
    """

    # ===============================
    # Public API
    # ===============================
    def build_actionhint(
        self,
        *,
        gate: GateLevel,
        structure: Optional[Dict[str, Any]],
        watchlist: Optional[Dict[str, Any]],
        conditions_runtime: Optional[Any],
    ) -> ActionHint:
        """
        构建最终 ActionHint（给用户 / 报告使用）
        """

        # -------- 输入校验（严格）--------
        self._validate_inputs(
            gate=gate,
            structure=structure,
            watchlist=watchlist,
            conditions_runtime=conditions_runtime,
        )

        # -------- 行为裁决 --------
        action = self._decide_action(gate)

        # -------- Summary（制度映射）--------
        summary = SummaryMapper().map_gate_to_summary(gate)

        # -------- Execution Risk（新增，D1/D2/D3）--------
        execution = self._build_execution_risk(gate=gate)

        # -------- 中文裁决理由 --------
        reason = self._build_reason_text(gate=gate)

        # -------- 行为边界 --------
        allowed = self._build_allowed(action)
        forbidden = self._build_forbidden(action)
        limits = self._build_limits(action=action, gate=gate)

        # -------- 执行时点说明 --------
        conditions = self._build_conditions_text(conditions_runtime)

        hint: ActionHint = {
            "gate": gate,
            "action": action,
            "summary": summary,
            "execution": execution,     # ← 写入
            "reason": reason,
            "allowed": allowed,
            "forbidden": forbidden,
            "limits": limits,
            "conditions": conditions,
        }

        logger.info(
            "[ActionHint] gate=%s action=%s summary=%s execution=%s",
            gate,
            action,
            summary,
            execution["code"],
        )

        return hint

    # ===============================
    # 内部逻辑（冻结）
    # ===============================

    def _validate_inputs(
        self,
        *,
        gate: Any,
        structure: Any,
        watchlist: Any,
        conditions_runtime: Any,
    ) -> None:
        if not isinstance(gate, str):
            raise ValueError(
                f"ActionHintService expects gate as str, got {type(gate)}"
            )

        if gate not in ("NORMAL", "CAUTION", "FREEZE"):
            raise ValueError(f"Invalid gate level: {gate}")

    # -------------------------------
    # 行为裁决（最小规则，冻结）
    # -------------------------------
    def _decide_action(self, gate: GateLevel) -> Action:
        if gate == "NORMAL":
            return "HOLD"
        if gate == "CAUTION":
            return "HOLD"
        if gate == "FREEZE":
            return "FREEZE"
        return "HOLD"

    # -------------------------------
    # Execution Risk（D1/D2/D3）
    # -------------------------------
    def _build_execution_risk(self, *, gate: GateLevel) -> ExecutionRisk:
        """
        Execution 风险定义（冻结版）：

        - A  : 无显著短期执行风险
        - D1 : 存在轻度回撤风险，需谨慎
        - D2 : 存在显著回撤风险（-2%~-4%）
        - D3 : 存在高风险回撤（-4% 以上）
        """

        if gate == "NORMAL":
            return {
                "code": "A",
                "horizon": "2-5D",
                "meaning": "短期未观察到显著执行风险，可按既有结构计划执行。",
            }

        if gate == "CAUTION":
            return {
                "code": "D1",
                "horizon": "2-5D",
                "meaning": "短期存在一定回撤风险，建议避免主动加仓，优先控制风险敞口。",
            }

        if gate == "FREEZE":
            return {
                "code": "D3",
                "horizon": "2-5D",
                "meaning": "短期执行风险极高，制度上不支持任何风险敞口扩张行为。",
            }

        # 理论不可达
        return {
            "code": "A",
            "horizon": "2-5D",
            "meaning": "执行风险状态无法识别，采取保守处理。",
        }

    # -------------------------------
    # 中文裁决理由
    # -------------------------------
    def _build_reason_text(self, *, gate: GateLevel) -> str:
        if gate == "NORMAL":
            return "当前结构稳定，未触发风险限制，可正常持有。"
        if gate == "CAUTION":
            return "结构偏谨慎，未触发明确放行条件，不建议主动扩大风险敞口。"
        if gate == "FREEZE":
            return "风险结构恶化，进入防御状态，暂停风险敞口变更。"
        return "当前状态无法识别，采取保守处理。"

    # -------------------------------
    # 行为允许 / 禁止
    # -------------------------------
    def _build_allowed(self, action: Action) -> List[str]:
        if action == "HOLD":
            return ["维持现有风险敞口"]
        if action == "FREEZE":
            return ["仅允许被动持有，不允许任何主动调整"]
        return []

    def _build_forbidden(self, action: Action) -> List[str]:
        if action == "HOLD":
            return ["主动加仓", "情绪化追涨"]
        if action == "FREEZE":
            return ["任何新增风险敞口", "抄底式操作"]
        return []

    # -------------------------------
    # 行为边界说明
    # -------------------------------
    def _build_limits(self, *, action: Action, gate: GateLevel) -> str:
        return (
            f"当前 Gate={gate}，行为裁决为 {action}。"
            "该裁决用于限制风险敞口变更，不构成收益预测。"
        )

    # -------------------------------
    # 执行时点说明（中文）
    # -------------------------------
    def _build_conditions_text(self, conditions_runtime: Any) -> str:
        if conditions_runtime is None:
            return "当前未启用执行时点校验，仅依据结构性裁决给出行为边界。"

        return "执行时点校验功能已预留，当前尚未纳入强制判断。"
