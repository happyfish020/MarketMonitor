# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - ExecutionSummaryBuilder

目标：
- 只读 Phase-2 outputs（factors / structure / observations）
- 输出 “执行摘要”（2-5D 维度）的 D1/D2/D3 分档 + 对应 A/N/D（仅解释，不做制度裁决）
- 永不返回 None（report slot 必须可用）
- 不影响 Gate / ActionHint（表达层专用）

约定输出 schema（冻结）：
{
  "code": "A|N|D",          # 执行摘要（2-5D）的建议色
  "band": "D1|D2|D3|NA",    # 分档（越大越危险）
  "meaning": "中文说明",
  "evidence": {...},        # 仅结构化证据，禁止拼报告长文
  "meta": {"asof": "...", "status": "ok|empty|error"}
}
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from core.utils.logger import get_logger
from core.regime.structure_distribution_evaluator import (
    StructureDistributionEvaluator
)
from typing import Dict, Any, Optional, List

 
LOG = get_logger("Governance.ExecutionSummary")


@dataclass(frozen=True)
class ExecutionSummary:
    code: str                 # A / N / D
    band: str                 # D1 / D2 / D3 / NA
    meaning: str
    evidence: Dict[str, Any]
    meta: Dict[str, Any]
   
    def to_dict(self) -> Dict[str, Any]:
            """
            Governance → Report / Overlay 的唯一合法序列化出口
            """
            return {
                "code": self.code,
                "band": self.band,
                "meaning": self.meaning,
                "evidence": self.evidence,
                "meta": self.meta,
            }
# -*- coding: utf-8 -*-
"""
ExecutionSummaryBuilder
-----------------------

职责（冻结）：
- 生成 ExecutionSummary（执行层解释）
- 不做 Gate 决策
- 不修改 structure / factors
- 明确区分：
  * Phase-2：当日结构质量
  * Phase-3：结构分布 / 成功率环境
  * Execution / DRS：2–5D 执行摩擦
"""


class ExecutionSummaryBuilder:
    """
    ExecutionSummary 构建器（只读）
    """

    def build(
        self,
        *,
        factors: Dict[str, Any],
        structure: Dict[str, Any],
        observations: Dict[str, Any],
        asof: str,
    ) -> ExecutionSummary:
        """
        Public entry.
        """
        return self._build_impl(
            factors=factors,
            structure=structure,
            observations=observations,
            asof=asof,
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _build_impl(
        self,
        *,
        factors: Dict[str, Any],
        structure: Dict[str, Any],
        observations: Dict[str, Any],
        asof: str,
    ) -> ExecutionSummary:
        """
        Build ExecutionSummary.

        规则（冻结）：
        - Phase-3 只作为“成功率环境”解释
        - D3 只能由：趋势破坏 / DRS=RED 触发
        - Phase-3 不直接触发 D3
        """

        # =========================
        # 0. 读取 Phase-2 结构事实
        # =========================
        trend = structure.get("trend_in_force")

         
        
        trend_state = trend.get("state") if isinstance(trend, dict) else None

        failure_rate = structure.get("failure_rate")
        failure_state = failure_rate.get("state") if isinstance(failure_rate, dict) else None

        breadth = structure.get("breadth")
        breadth_state = breadth.get("state") if isinstance(breadth, dict) else None

        # =========================
        # 1. 读取 DRS（执行观测）
        # =========================
        drs_signal = None
        drs = observations.get("drs")
        if isinstance(drs, dict):
            obs = drs.get("observation")
            payload = drs.get("payload")
            if isinstance(obs, dict):
                drs_signal = obs.get("signal")
                drs_meaning = obs.get("meaning")
            elif isinstance(payload, dict):
                drs_signal = payload.get("signal")
                drs_meaning = payload.get("meaning")

        # =========================
        # 2. Phase-3：结构分布（成功率环境）
        # =========================
        regime = structure.get("regime", {}) if isinstance(structure, dict) else {}
        dist = regime.get("structure_distribution")

        phase3_meaning: Optional[str] = None
        phase3_evidence: Optional[Dict[str, Any]] = None

        if isinstance(dist, dict) and dist.get("state") == "DISTRIBUTION_RISK":
            window = dist.get("window")
            count = dist.get("count")

            phase3_meaning = (
                f"【Phase-3｜结构性分布风险】"
                f"近{window}个交易日中出现{count}次结构恶化信号。"
                "这通常发生在上涨后期或反弹阶段，"
                "市场表面可能仍有强势表现，但结构同步与参与度反复走弱。"
                "这不是立即下跌信号，而是成功率下降环境，"
                "不适合主动追高或扩大风险敞口。"
            )

            phase3_evidence = {
                "state": dist.get("state"),
                "window": window,
                "count": count,
            }

        # =========================
        # 3. D3：高执行风险（严格条件）
        # =========================
        if trend_state == "broken" or drs_signal == "RED":
            meaning = (
                "短期（2–5D）执行风险高：趋势结构已被破坏或 DRS=RED，"
                "反弹容易失败或出现二次回落，制度上偏向防守执行。"
            )

            if phase3_meaning:
                meaning = f"{meaning}\n{phase3_meaning}"

            return ExecutionSummary(
                code="D",
                band="D3",
                meaning=meaning,
                evidence={
                    "trend_state": trend_state,
                    "drs_signal": drs_signal,
                    "phase3": phase3_evidence,
                },
                meta={"asof": asof, "status": "ok"},
            )

        # =========================
        # 4. D2：执行摩擦偏大
        # =========================
        d2_hits: List[str] = []

        if breadth_state in ("weak", "breakdown"):
            d2_hits.append("breadth")

        if failure_state in ("rising", "unstable"):
            d2_hits.append("failure_rate")

        if d2_hits:
            meaning = (
                "短期（2–5D）执行摩擦偏大：结构未必立刻失败，"
                "但参与度/广度/失败率显示操作难度上升，"
                "更适合轻仓或等待确认，避免追高。"
            )

            if phase3_meaning:
                meaning = f"{meaning}\n{phase3_meaning}"

            return ExecutionSummary(
                code="D",
                band="D2",
                meaning=meaning,
                evidence={
                    "hits": d2_hits,
                    "phase3": phase3_evidence,
                },
                meta={"asof": asof, "status": "ok"},
            )

        # =========================
        # 5. A / D1：执行环境尚可
        # =========================
        meaning = "短期（2–5D）未观察到显著执行风险，可在控制仓位的前提下按结构计划执行。"

        if phase3_meaning:
            meaning = f"{meaning}\n{phase3_meaning}"

        return ExecutionSummary(
            code="A",
            band="D1",
            meaning=meaning,
            evidence={
                "phase3": phase3_evidence,
            },
            meta={"asof": asof, "status": "ok"},
        )
