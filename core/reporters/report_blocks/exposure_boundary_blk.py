from __future__ import annotations

from typing import Any, Dict, List

from core.reporters.report_context import ReportContext
from core.reporters.report_types import ReportBlock


class ExposureBoundaryBlock:
    """
    风险敞口行为边界（冻结 · P0）

    职责：
    - 基于 Gate，明确声明下一交易日（T+1）的风险敞口变更制度边界
    - 只做制度裁决声明，不做执行说明
    - 不计算、不实时判断、不读取任何因子或行情数据
    """

    block_alias = "exposure.boundary"
    title = "下一交易日（T+1）风险敞口行为边界"

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
                note="未提供 Gate，无法生成 T+1 风险敞口行为边界。"
            )
            return self._build_block(payload, warnings)

        if not isinstance(gate, str):
            warnings.append("invalid:gate_type")
            payload = self._build_empty_payload(
                note="Gate 类型异常，T+1 风险敞口行为边界不可用。"
            )
            return self._build_block(payload, warnings)

        gate = gate.upper()
        payload = self._build_payload_by_gate(gate)
        return self._build_block(payload, warnings)

    # ===============================
    # Payload builders（制度裁决）
    # ===============================
    def _build_payload_by_gate(self, gate: str) -> Dict[str, Any]:
        if gate == "NORMAL":
            return {
                "intro": (
                    "本区块用于明确下一交易日（T+1）在当前制度状态下，"
                    "风险敞口变更行为的制度允许与禁止边界。"
                ),
                "dominant": "当前制度主导方向为：风险扩张在制度上不构成限制。",
                "behaviors": [
                    {
                        "label": "增加风险敞口",
                        "status": "允许",
                        "meaning": (
                            "任何会使账户整体风险暴露上升的行为，"
                            "在当前制度状态下不构成制度违规。"
                        ),
                    },
                    {
                        "label": "减少风险敞口",
                        "status": "允许",
                        "meaning": (
                            "通过回收仓位或降低高波动资产权重来降低风险，"
                            "始终不构成制度违规。"
                        ),
                    },
                    {
                        "label": "切换风险敞口",
                        "status": "允许",
                        "meaning": (
                            "在不显著放大整体风险的前提下，"
                            "允许进行结构性风险敞口调整。"
                        ),
                    },
                ],
                #"note": "Gate=NORMAL 表示制度层未对风险敞口变更设置额外限制。",
            }

        if gate == "CAUTION":
            return {
                "intro": (
                    "本区块用于明确下一交易日（T+1）在谨慎制度状态下，"
                    "风险敞口变更行为的制度允许与禁止边界。"
                ),
                "dominant": "当前制度主导方向为：风险收敛与等待。",
                "behaviors": [
                    {
                        "label": "增加风险敞口",
                        "status": "不被制度支持",
                        "meaning": (
                            "任何会使账户整体风险暴露上升的行为，"
                            "在当前结构环境下不具备制度条件。"
                        ),
                    },
                    {
                        "label": "减少风险敞口",
                        "status": "允许",
                        "meaning": (
                            "通过降低仓位或回收风险敞口来控制波动，"
                            "符合当前制度主导方向。"
                        ),
                    },
                    {
                        "label": "切换风险敞口",
                        "status": "有条件允许",
                        "meaning": (
                            "仅限于不放大整体风险、偏防守方向的结构性调整。"
                        ),
                    },
                ],
                #"note": (
                #    "Gate=CAUTION 表示当前结构尚未支持风险扩张，"
                #    "制度优先防守与等待。"
                #),
            }

        if gate == "FREEZE":
            return {
                "intro": (
                    "本区块用于明确下一交易日（T+1）在冻结制度状态下，"
                    "风险敞口变更行为的制度允许与禁止边界。"
                ),
                "dominant": "当前制度主导方向为：风险收缩。",
                "behaviors": [
                    {
                        "label": "增加风险敞口",
                        "status": "制度禁止",
                        "meaning": (
                            "任何会使账户整体风险暴露上升的行为，"
                            "在当前制度状态下均构成制度违规。"
                        ),
                    },
                    {
                        "label": "减少风险敞口",
                        "status": "制度鼓励",
                        "meaning": (
                            "通过主动回收风险敞口来保护账户安全，"
                            "符合当前制度目标。"
                        ),
                    },
                    {
                        "label": "切换风险敞口",
                        "status": "制度禁止",
                        "meaning": (
                            "不允许任何可能放大或重新分配整体风险的结构性调整。"
                        ),
                    },
                ],
                #"note": (
                #    "Gate=FREEZE 表示制度进入风险防御阶段，"
                #    "仅允许风险回收行为。"
                #),
            }

        return self._build_empty_payload(
            note=f"未知 Gate 状态：{gate}"
        )

    def _build_empty_payload(self, note: str) -> Dict[str, Any]:
        return {
            "intro": "T+1 风险敞口行为边界不可用。",
            "dominant": "",
            "behaviors": [],
            #"note": note,
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
