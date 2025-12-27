from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional

GateLevel = Literal["NORMAL", "CAUTION", "FREEZE"]


@dataclass(frozen=True)
class GateOverlayResult:
    gate_final: GateLevel
    reasons: List[str]
    evidence: Dict[str, Any]


class GateOverlay:
    """
    UnifiedRisk V12 · GateOverlay（冻结版）

    铁律：
    - 只允许降级：NORMAL -> CAUTION -> FREEZE
    - 不允许升级（任何情况下都不能把 CAUTION/FREEZE 升回 NORMAL）
    - 只读 slots（trend/drs/execution），不依赖外部实时数据
    """

    def apply(
        self,
        *,
        gate_pre: GateLevel,
        trend_state: Optional[str],
        drs_signal: Optional[str],
        execution_band: Optional[str],
    ) -> GateOverlayResult:
        reasons: List[str] = []
        evidence: Dict[str, Any] = {
            "gate_pre": gate_pre,
            "trend_state": trend_state,
            "drs_signal": drs_signal,
            "execution_band": execution_band,
        }

        # 先默认不变
        gate_final: GateLevel = gate_pre

        # ---- 冻结降级规则（可扩展，但只能降级） ----
        # 1) 趋势破坏 或 DRS=RED：至少 CAUTION；若已 CAUTION 则可到 FREEZE（更防守）
        if trend_state == "broken":
            gate_final = self._downgrade(gate_final, target="CAUTION")
            reasons.append("overlay:trend_broken=>downgrade")
        if drs_signal == "RED":
            # RED 更强：至少 CAUTION；若已有 CAUTION 则可进一步 FREEZE
            gate_final = self._downgrade(gate_final, target="CAUTION")
            reasons.append("overlay:drs_red=>downgrade")

        # 2) 执行分档 D3：进一步偏向 FREEZE（但仍遵循只降级）
        if execution_band == "D3":
            gate_final = self._downgrade(gate_final, target="FREEZE")
            reasons.append("overlay:execution_d3=>freeze")

        # 3) 执行分档 D2：至少 CAUTION（只降级）
        if execution_band == "D2":
            gate_final = self._downgrade(gate_final, target="CAUTION")
            reasons.append("overlay:execution_d2=>caution")

        return GateOverlayResult(gate_final=gate_final, reasons=reasons, evidence=evidence)

    def _downgrade(self, current: GateLevel, *, target: GateLevel) -> GateLevel:
        order = {"NORMAL": 0, "CAUTION": 1, "FREEZE": 2}
        # 只允许向更大（更防守）移动
        return current if order[current] >= order[target] else target
