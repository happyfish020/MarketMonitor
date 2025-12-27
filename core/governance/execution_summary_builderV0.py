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

LOG = get_logger("Governance.ExecutionSummary")


@dataclass(frozen=True)
class ExecutionSummary:
    code: str                 # A / N / D
    band: str                 # D1 / D2 / D3 / NA
    meaning: str
    evidence: Dict[str, Any]
    meta: Dict[str, Any]


class ExecutionSummaryBuilder:
    """
    冻结设计：
    - 输入允许缺失：缺什么就降级为 NA/中性，并写 evidence + warning（不抛异常）
    - band 与 code 的映射：D3=>D, D2=>D, D1=>N, NA=>N, A=>A（但 A 需要满足“没看到执行风险”）
    """

    def build(
        self,
        *,
        factors: Optional[Dict[str, Any]],
        structure: Optional[Dict[str, Any]],
        observations: Optional[Dict[str, Any]],
        asof: str,
    ) -> Dict[str, Any]:
        try:
            res = self._build_impl(
                factors=factors or {},
                structure=structure or {},
                observations=observations or {},
                asof=asof,
            )
            return {
                "code": res.code,
                "band": res.band,
                "meaning": res.meaning,
                "evidence": res.evidence,
                "meta": res.meta,
            }
        except Exception as e:
            LOG.error("[ExecutionSummary] build failed: %s", e, exc_info=True)
            return {
                "code": "N",
                "band": "NA",
                "meaning": "ExecutionSummary 构建失败（不影响主流程），请查看日志。",
                "evidence": {"error": str(e)},
                "meta": {"asof": asof, "status": "error"},
            }

    # -------------------------
    # 内部实现（冻结）
    # -------------------------
    def _build_impl(
        self,
        *,
        factors: Dict[str, Any],
        structure: Dict[str, Any],
        observations: Dict[str, Any],
        asof: str,
    ) -> ExecutionSummary:
        
   
        regime = structure.get("regime", {}) if isinstance(structure, dict) else {}
        dist = regime.get("structure_distribution")


        # 关键输入抽取（允许缺失）
        trend = structure.get("trend_in_force") if isinstance(structure, dict) else None
        trend_state = trend.get("state") if isinstance(trend, dict) else None

        # 标准化 DRS（优先 observations["drs"]["observation"]）
        drs_signal = None
        drs_meaning = None
        drs = observations.get("drs") if isinstance(observations, dict) else None
        if isinstance(drs, dict):
            obs = drs.get("observation")
            payload = drs.get("payload")
            if isinstance(obs, dict):
                drs_signal = obs.get("signal")
                drs_meaning = obs.get("meaning")
            elif isinstance(payload, dict):
                drs_signal = payload.get("signal")
                drs_meaning = payload.get("meaning")

        # Phase-2 因子（只读）
        # 注意：这里不假设你的 FactorResult 结构一定一致，只做尽力解析
        breadth = factors.get("breadth") ########## todo check breadth is None
        participation = factors.get("participation")
        index_tech = factors.get("index_tech")   #todo check 
        frf = factors.get("failure_rate")            # frr is None

        def _score(fr: Any) -> Optional[float]:
            if isinstance(fr, dict):
                v = fr.get("score")
                return float(v) if isinstance(v, (int, float)) else None
            v = getattr(fr, "score", None)
            return float(v) if isinstance(v, (int, float)) else None

        def _level(fr: Any) -> Optional[str]:
            if isinstance(fr, dict):
                v = fr.get("level")
                return str(v) if isinstance(v, str) else None
            v = getattr(fr, "level", None)
            return str(v) if isinstance(v, str) else None

        b_score, b_lv = _score(breadth), _level(breadth)
        p_score, p_lv = _score(participation), _level(participation)
        t_score, t_lv = _score(index_tech), _level(index_tech)
        f_score, f_lv = _score(frf), _level(frf)

        # -------------------------
        # 分档规则（冻结 v0）
        # -------------------------
        # D3：制度级“反弹易失败/结构易变”高风险
        if trend_state == "broken" or drs_signal == "RED":
            band = "D3"
            code = "D"
            meaning = (
                "短期（2–5D）执行风险高：趋势结构已破坏或 DRS=RED，"
                "反弹易出现二次回落/剧烈波动，制度上偏向防守执行。"
                "结构性分布风险，反弹成功率偏低，避免追高"
            )
            evidence = {
                "trend_state": trend_state,
                "drs_signal": drs_signal,
                "drs_meaning": drs_meaning,
            }
            return ExecutionSummary(
                code=code,
                band=band,
                meaning=meaning,
                evidence=evidence,
                meta={"asof": asof, "status": "ok"},
            )

        # D2：结构未破，但“执行摩擦”偏大（常见：参与度/广度恶化、失败率升高）
        d2_hits = []
        if b_lv == "HIGH" or (isinstance(b_score, (int, float)) and b_score < 40):
            d2_hits.append("breadth_damage")
        if p_lv == "HIGH" or (isinstance(p_score, (int, float)) and p_score < 45):
            d2_hits.append("participation_weak")
        if f_lv == "HIGH" or (isinstance(f_score, (int, float)) and f_score > 60):
            d2_hits.append("frf_high")

        if d2_hits:
            band = "D2"
            code = "D"
            meaning = (
                "短期（2–5D）执行风险偏高：结构未必立刻失败，但执行摩擦增大。"
                "更适合“轻仓试错/等待确认/避免追高。"
                "结构性分布风险，反弹成功率偏低，避免追高。"
            )
            evidence = {
                "hits": d2_hits,
                "breadth": {"score": b_score, "level": b_lv},
                "participation": {"score": p_score, "level": p_lv},
                "frf": {"score": f_score, "level": f_lv},
                "drs_signal": drs_signal,
            }
            return ExecutionSummary(
                code=code,
                band=band,
                meaning=meaning,
                evidence=evidence,
                meta={"asof": asof, "status": "ok"},
            )

        # D1：轻微风险（震荡/回踩概率更高，但不是制度风险）
        d1_hits = []
        if t_lv == "HIGH" or (isinstance(t_score, (int, float)) and t_score < 48):
            d1_hits.append("index_tech_soft")
        if d1_hits:
            band = "D1"
            code = "N"
            meaning = "短期（2–5D）可能震荡/回踩：建议按计划分批、避免一次性追涨。"
            evidence = {
                "hits": d1_hits,
                "index_tech": {"score": t_score, "level": t_lv},
                "drs_signal": drs_signal,
            }
            return ExecutionSummary(
                code=code,
                band=band,
                meaning=meaning,
                evidence=evidence,
                meta={"asof": asof, "status": "ok"},
            )

        # A：没有看到明显“执行风险”
        return ExecutionSummary(
            code="A",
            band="NA",
            meaning="短期（2–5D）未观察到显著执行风险，可按既有结构计划执行。",
            evidence={
                "trend_state": trend_state,
                "drs_signal": drs_signal,
                "breadth": {"score": b_score, "level": b_lv},
                "participation": {"score": p_score, "level": p_lv},
                "index_tech": {"score": t_score, "level": t_lv},
                "frf": {"score": f_score, "level": f_lv},
            },
            meta={"asof": asof, "status": "ok"},
        )
