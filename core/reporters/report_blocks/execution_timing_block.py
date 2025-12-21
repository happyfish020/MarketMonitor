from __future__ import annotations

from typing import Any, Dict, List

from core.reporters.report_context import ReportContext
from core.reporters.report_types import ReportBlock


class ExecutionTimingBlock:
    """
    风险敞口变更行为 · 执行说明块（冻结）

    职责：
    - 基于 Gate 输出执行层面的制度语境与注意事项
    - 不裁决行为是否允许/禁止
    - 不计算、不实时判断、不读取因子
    """

    block_alias = "execution.timing"
    title = "风险敞口变更行为"

    def render(
        self,
        context: ReportContext,
        doc_partial: Dict[str, Any],
    ) -> ReportBlock:
        warnings: List[str] = []

        gate = context.slots.get("gate")

        if gate is None:
            warnings.append("missing:gate")
            payload = self._build_empty_payload(
                note="未提供 Gate，无法生成风险敞口变更行为的执行说明。"
            )
            return self._build_block(payload, warnings)

        if not isinstance(gate, str):
            warnings.append("invalid:gate_type")
            payload = self._build_empty_payload(
                note="Gate 类型异常，风险敞口变更行为的执行说明不可用。"
            )
            return self._build_block(payload, warnings)

        gate = gate.upper()
        payload = self._build_payload_by_gate(gate)
        return self._build_block(payload, warnings)

    # ===============================
    # Payload builders（文案级对齐）
    # ===============================
    def _build_payload_by_gate(self, gate: str) -> Dict[str, Any]:
        if gate == "NORMAL":
            return {
                "intro": (
                    "本区块用于说明在当前 Gate 制度状态下，"
                    "风险敞口变更行为在执行层面的注意事项与制度语境。"
                    "行为是否允许或禁止，请以《下一交易日（T+1）风险敞口行为边界》区块为准。"
                ),
                "dominant": (
                    "当前制度主导语境：结构环境相对宽松，"
                    "但执行层仍需关注整体风险暴露与波动来源的集中度。"
                ),
                "behaviors": [
                    {
                        "label": "增加风险敞口",
                        "status": "执行关注点",
                        "meaning": (
                            "在扩张风险暴露时，应关注是否同步放大了账户整体波动性，"
                            "避免因集中加仓或单一风险来源过高而放大回撤风险。"
                        ),
                    },
                    {
                        "label": "减少风险敞口",
                        "status": "执行注意事项",
                        "meaning": (
                            "主动回收风险敞口可作为风险治理手段之一，"
                            "有助于在结构变化时降低账户波动。"
                        ),
                    },
                    {
                        "label": "切换风险敞口",
                        "status": "执行风险提示",
                        "meaning": (
                            "结构性切换时需防范“名义切换、实质加风险”的情况，"
                            "避免在调整过程中无意引入更高波动资产。"
                        ),
                    },
                ],
                "note": (
                    "本区块不构成任何具体交易建议或执行时点建议。"
                    "风险敞口变更行为的制度边界，"
                    "以《下一交易日（T+1）风险敞口行为边界》区块为唯一依据。"
                ),
            }

        if gate == "CAUTION":
            return {
                "intro": (
                    "本区块用于说明在当前 Gate 制度状态下，"
                    "风险敞口变更行为在执行层面的注意事项与制度语境。"
                    "行为是否允许或禁止，请以《下一交易日（T+1）风险敞口行为边界》区块为准。"
                ),
                "dominant": (
                    "当前制度主导语境：风险收敛与等待，"
                    "执行层应避免主动放大整体风险来源。"
                ),
                "behaviors": [
                    {
                        "label": "增加风险敞口",
                        "status": "执行风险提示",
                        "meaning": (
                            "在谨慎语境下，此类行为容易导致整体风险暴露上移，"
                            "执行层应优先回避并防止节奏性冲动操作。"
                        ),
                    },
                    {
                        "label": "减少风险敞口",
                        "status": "执行关注点",
                        "meaning": (
                            "通过回收风险敞口来控制波动，"
                            "有助于在结构未明朗阶段保持账户稳定性。"
                        ),
                    },
                    {
                        "label": "切换风险敞口",
                        "status": "执行约束",
                        "meaning": (
                            "结构性调整应以不放大整体风险为前提，"
                            "避免通过换仓方式引入更高不确定性。"
                        ),
                    },
                ],
                "note": (
                    "本区块不构成任何具体交易建议或执行时点建议。"
                    "风险敞口变更行为的制度边界，"
                    "以《下一交易日（T+1）风险敞口行为边界》区块为唯一依据。"
                ),
            }

        if gate == "FREEZE":
            return {
                "intro": (
                    "本区块用于说明在当前 Gate 制度状态下，"
                    "风险敞口变更行为在执行层面的注意事项与制度语境。"
                    "行为是否允许或禁止，请以《下一交易日（T+1）风险敞口行为边界》区块为准。"
                ),
                "dominant": (
                    "当前制度主导语境：风险防御优先，"
                    "执行层应以账户安全与回撤控制为核心目标。"
                ),
                "behaviors": [
                    {
                        "label": "增加风险敞口",
                        "status": "执行风险提示",
                        "meaning": (
                            "在冻结语境下，扩张风险暴露可能显著放大尾部风险，"
                            "执行层需严格回避相关操作。"
                        ),
                    },
                    {
                        "label": "减少风险敞口",
                        "status": "执行关注点",
                        "meaning": (
                            "通过主动回收风险敞口来降低整体波动，"
                            "有助于在高风险阶段保护账户安全。"
                        ),
                    },
                    {
                        "label": "切换风险敞口",
                        "status": "执行约束",
                        "meaning": (
                            "切换过程中需警惕“表面调整、实质加风险”的情况，"
                            "避免引入更高波动或流动性较差的风险来源。"
                        ),
                    },
                ],
                "note": (
                    "本区块不构成任何具体交易建议或执行时点建议。"
                    "风险敞口变更行为的制度边界，"
                    "以《下一交易日（T+1）风险敞口行为边界》区块为唯一依据。"
                ),
            }

        return self._build_empty_payload(
            note=f"未知 Gate 状态：{gate}"
        )

    def _build_empty_payload(self, note: str) -> Dict[str, Any]:
        return {
            "intro": "风险敞口变更行为的执行说明不可用。",
            "dominant": "",
            "behaviors": [],
            "note": note,
        }

    def _build_block(
        self,
        payload: Dict[str, Any],
        warnings: List[str],
    ) -> ReportBlock:
        return ReportBlock(
            block_alias=self.block_alias,
            title=self.title,
            payload=payload,
            warnings=warnings,
        )
