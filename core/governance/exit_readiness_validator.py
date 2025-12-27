# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - ExitReadinessValidator (冻结 MVP)

目标：
- 提供“减仓/清仓是否应提前”的制度化准备度结论
- 只读 Phase-2 outputs（来自 ReportContext.slots）
- 不依赖盘中实时价格，不做收益预测
- 输出结构化证据：原因 + evidence（可审计）

输出 schema（冻结）：
{
  "level": "OK|WATCH|TRIM|EXIT",
  "action": "HOLD|TRIM|EXIT",
  "meaning": "中文说明",
  "reasons": [..],
  "evidence": {...},
  "meta": {"asof": "...", "status": "ok|empty|error"}
}
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional

from core.utils.logger import get_logger

LOG = get_logger("Governance.ExitReadiness")

ExitLevel = Literal["OK", "WATCH", "TRIM", "EXIT"]
ExitAction = Literal["HOLD", "TRIM", "EXIT"]
GateLevel = Literal["NORMAL", "CAUTION", "FREEZE"]


@dataclass(frozen=True)
class ExitReadiness:
    level: ExitLevel
    action: ExitAction
    meaning: str
    reasons: List[str]
    evidence: Dict[str, Any]
    meta: Dict[str, Any]


class ExitReadinessValidator:
    """
    冻结原则：
    - 与 Entry 对称：Exit 更强调“提前”（防止 D+RED+broken 才动作）
    - 只允许更保守（更接近 TRIM/EXIT），不允许因为“反弹”而放松
    - 以 Gate/DRS/Trend/Execution/Rebound-only 为核心输入
    """

    def evaluate(self, *, slots: Dict[str, Any], asof: str) -> Dict[str, Any]:
        try:
            res = self._eval_impl(slots=slots or {}, asof=asof)
            return {
                "level": res.level,
                "action": res.action,
                "meaning": res.meaning,
                "reasons": res.reasons,
                "evidence": res.evidence,
                "meta": res.meta,
            }
        except Exception as e:
            LOG.error("[ExitReadiness] evaluate failed: %s", e, exc_info=True)
            return {
                "level": "WATCH",
                "action": "HOLD",
                "meaning": "ExitReadiness 评估失败（不影响主流程），请查看日志。",
                "reasons": ["error:evaluate_failed"],
                "evidence": {"error": str(e)},
                "meta": {"asof": asof, "status": "error"},
            }

    # -------------------------------------------------
    # internal
    # -------------------------------------------------
    def _eval_impl(self, *, slots: Dict[str, Any], asof: str) -> ExitReadiness:
        reasons: List[str] = []
        evidence: Dict[str, Any] = {}

        # 1) Gate（优先使用 gate_final；没有则退回 gate）
        gate_final = slots.get("gate_final") or slots.get("gate")
        if gate_final not in ("NORMAL", "CAUTION", "FREEZE"):
            gate_final = "NORMAL"
            reasons.append("warn:gate_missing_or_invalid")
        evidence["gate_final"] = gate_final

        # 2) Trend-in-Force
        trend_state = None
        structure = slots.get("structure")
        if isinstance(structure, dict):
            trend = structure.get("trend_in_force")
            if isinstance(trend, dict):
                trend_state = trend.get("state")
        evidence["trend_state"] = trend_state

        # 3) DRS（标准化 slots['drs']）
        drs_signal = None
        drs = slots.get("drs")
        if isinstance(drs, dict):
            drs_signal = drs.get("signal")
        evidence["drs_signal"] = drs_signal

        # 4) ExecutionSummary（2–5D）
        execution_band = None
        execution_code = None
        execu = slots.get("execution_summary")
        if isinstance(execu, dict):
            execution_band = execu.get("band")
            execution_code = execu.get("code")
        evidence["execution"] = {"code": execution_code, "band": execution_band}

        # 5) Rebound-only（守门）
        ro_flag = None
        ro_sev = None
        ro = slots.get("rebound_only")
        if isinstance(ro, dict):
            ro_flag = ro.get("flag")
            ro_sev = ro.get("severity")
        evidence["rebound_only"] = {"flag": ro_flag, "severity": ro_sev}

        # -------------------------------------------------
        # 冻结判定逻辑（Exit 更强调“提前”）
        # -------------------------------------------------

        # A) EXIT：制度硬信号（最强）
        if gate_final == "FREEZE":
            reasons.append("hard:gate_freeze")
            return self._mk(
                level="EXIT",
                action="EXIT",
                meaning="制度进入 FREEZE：仅允许防守性操作，优先清理高β/非核心风险敞口。",
                reasons=reasons,
                evidence=evidence,
                asof=asof,
            )

        if trend_state == "broken" and drs_signal == "RED":
            reasons.append("hard:trend_broken+drs_red")
            return self._mk(
                level="EXIT",
                action="TRIM",
                meaning="趋势破坏 + DRS=RED：制度风险高位，优先减少风险敞口（先减高β/非核心）。",
                reasons=reasons,
                evidence=evidence,
                asof=asof,
            )

        # B) TRIM：提前减仓（强建议）
        # 条件：任一强风险 + 执行风险偏高
        if (drs_signal == "RED" and execution_band in ("D2", "D3")):
            reasons.append("strong:drs_red+execution_high")
            return self._mk(
                level="TRIM",
                action="TRIM",
                meaning="DRS=RED 且执行分档偏高：反弹阶段更适合减仓/控敞口，而非加仓追涨。",
                reasons=reasons,
                evidence=evidence,
                asof=asof,
            )

        if trend_state == "broken" and execution_band in ("D2", "D3"):
            reasons.append("strong:trend_broken+execution_high")
            return self._mk(
                level="TRIM",
                action="TRIM",
                meaning="趋势结构破坏且执行风险偏高：优先用反弹做降风险与再平衡。",
                reasons=reasons,
                evidence=evidence,
                asof=asof,
            )

        if gate_final == "CAUTION" and execution_band in ("D2", "D3"):
            reasons.append("strong:gate_caution+execution_high")
            return self._mk(
                level="TRIM",
                action="TRIM",
                meaning="Gate=CAUTION 且执行风险偏高：不支持扩敞口，优先降风险、避免追价。",
                reasons=reasons,
                evidence=evidence,
                asof=asof,
            )

        # C) WATCH：观察/准备（中等）
        if drs_signal == "RED":
            reasons.append("mid:drs_red")
            return self._mk(
                level="WATCH",
                action="HOLD",
                meaning="DRS=RED：制度风险上升，进入观察与减仓准备状态（等待确认/失败信号）。",
                reasons=reasons,
                evidence=evidence,
                asof=asof,
            )

        if trend_state == "broken":
            reasons.append("mid:trend_broken")
            return self._mk(
                level="WATCH",
                action="HOLD",
                meaning="趋势结构被破坏：即便反弹也偏噪声，进入观察与纪律执行阶段。",
                reasons=reasons,
                evidence=evidence,
                asof=asof,
            )

        if ro_flag is True and ro_sev in ("MEDIUM", "HIGH"):
            reasons.append("mid:rebound_only_guard")
            return self._mk(
                level="WATCH",
                action="HOLD",
                meaning="触发 Rebound-only 守门：反弹不等于可追，建议保持纪律与风险边界。",
                reasons=reasons,
                evidence=evidence,
                asof=asof,
            )

        # D) OK：无明显出场准备信号
        return self._mk(
            level="OK",
            action="HOLD",
            meaning="未触发明显出场准备信号：可按既定结构持有与执行，但仍需避免追涨。",
            reasons=reasons,
            evidence=evidence,
            asof=asof,
        )

    def _mk(
        self,
        *,
        level: ExitLevel,
        action: ExitAction,
        meaning: str,
        reasons: List[str],
        evidence: Dict[str, Any],
        asof: str,
    ) -> ExitReadiness:
        return ExitReadiness(
            level=level,
            action=action,
            meaning=meaning,
            reasons=reasons,
            evidence=evidence,
            meta={"asof": asof, "status": "ok"},
        )
