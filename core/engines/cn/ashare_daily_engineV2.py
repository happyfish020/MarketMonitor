# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Tuple

from core.utils.logger import get_logger
from core.adapters.fetchers.cn.ashare_fetcher import AshareDataFetcher

from core.factors.cn.unified_emotion_factor import UnifiedEmotionFactor
from core.factors.cn.participation_factor import ParticipationFactor
from core.factors.glo.global_macro_factor import GlobalMacroFactor
from core.factors.glo.index_global_factor import IndexGlobalFactor
from core.factors.glo.global_lead_factor import GlobalLeadFactor
from core.factors.cn.north_nps_factor import NorthNPSFactor
from core.factors.cn.north_nps_trend_factor import NorthNPSTrendFactor
from core.factors.cn.turnover_factor import TurnoverFactor
from core.factors.cn.margin_factor import MarginFactor
from core.factors.cn.sector_rotation_factor import SectorRotationFactor
from core.factors.cn.index_tech_factor import IndexTechFactor
from core.factors.cn.breadth_factor import BreadthFactor
from core.factors.cn.etf_index_sync_factor import ETFIndexSyncFactor
from core.factors.cn.etf_index_sync_daily_factor import ETFIndexSyncDailyFactor
from core.factors.cn.trend_in_force_factor import TrendInForceFactor
from core.factors.cn.frf_factor import FRFFactor
from core.factors.factor_result import FactorResult

from core.regime.ashares_gate_decider import ASharesGateDecider
from core.regime.observation.structure.structure_facts_builder import StructureFactsBuilder
from core.regime.observation.watchlist.watchlist_state_builder import WatchlistStateBuilder
from core.regime.observation.drs.drs_observation import DRSObservation
from core.regime.observation.drs_continuity import DRSContinuity

from core.governance.execution_summary_builder import ExecutionSummary, ExecutionSummaryBuilder

from core.reporters.report_context import ReportContext
from core.reporters.report_engine import ReportEngine
from core.reporters.renderers.markdown_renderer import MarkdownRenderer
from core.reporters.report_writer import ReportWriter

from core.actions.actionhint_service import ActionHintService

from core.reporters.report_blocks.structure_facts_blk import StructureFactsBlock
from core.reporters.report_blocks.summary_a_n_d_blk import SummaryANDBlock
from core.reporters.report_blocks.watchlist_sectors_blk import WatchlistSectorsBlock
from core.reporters.report_blocks.context_overnight_blk import ContextOvernightBlock
from core.reporters.report_blocks.execution_timing_block import ExecutionTimingBlock
from core.reporters.report_blocks.dev_evidence_blk import DevEvidenceBlock
from core.reporters.report_blocks.scenarios_forward_blk import ScenariosForwardBlock
from core.reporters.report_blocks.exposure_boundary_blk import ExposureBoundaryBlock
from core.reporters.report_blocks.execution_quick_reference_blk import ExecutionQuickReferenceBlock
from core.reporters.report_blocks.execution_summary_blk import ExecutionSummaryBlock
from core.reporters.report_blocks.exit_readiness_blk import ExitReadinessBlock

from core.predictors.prediction_engine import PredictionEngine
from core.adapters.policy_slot_binders.cn.ashares_policy_slot_binder import ASharesPolicySlotBinder

from core.regime.structure_distribution_evaluator import (
    StructureDistributionEvaluator
) 
from core.regime.structure_distribution_continuity import StructureDistributionContinuity

LOG = get_logger("Engine.AshareDaily")


def _normalize_trade_date(trade_date: str | None) -> str:
    if trade_date:
        s = str(trade_date).strip()
        if len(s) == 8 and s.isdigit():
            return f"{s[:4]}-{s[4:6]}-{s[6:]}"
        return s
    return datetime.now().strftime("%Y-%m-%d")


# =========================================================
# Phase-3 Report pipeline helpers
# =========================================================

def _extract_drs_slot(observations: Any) -> Dict[str, Any]:
    """
    标准化 slots["drs"]，供 Summary Block 等消费。
    永不抛异常，缺失返回 NA 占位。
    """
    if not isinstance(observations, dict):
        return {"signal": "NA", "meaning": "observations 缺失或非法", "status": "empty"}

    drs = observations.get("drs")
    if not isinstance(drs, dict):
        return {"signal": "NA", "meaning": "DRS observation 缺失", "status": "empty"}

    obs = drs.get("observation")
    payload = drs.get("payload")
    if isinstance(obs, dict):
        return {
            "signal": obs.get("signal", "NA"),
            "meaning": obs.get("meaning", "未提供风险说明"),
            "status": drs.get("meta", {}).get("status", "ok") if isinstance(drs.get("meta"), dict) else "ok",
        }
    if isinstance(payload, dict):
        return {
            "signal": payload.get("signal", "NA"),
            "meaning": payload.get("meaning", "未提供风险说明"),
            "status": drs.get("meta", {}).get("status", "ok") if isinstance(drs.get("meta"), dict) else "ok",
        }

    return {"signal": "NA", "meaning": "DRS 结构不符合预期", "status": "empty"}


def _prepare_report_slots(*, gate_decision, factors_bound: dict) -> dict:
    gate_pre = gate_decision.level

    # Phase-2 outputs（只读）
    structure = factors_bound.get("structure")
    watchlist = factors_bound.get("watchlist")
    observations = factors_bound.get("observations")

    # ★ 标准化 drs 为一等 slot
    drs_slot = _extract_drs_slot(observations)

    # ★ ExecutionSummary（必须由 Phase-2 build 阶段写入）
    execution_summary = factors_bound.get("execution_summary")
    if execution_summary is None:
        execution_summary = {
            "code": "N",
            "band": "NA",
            "meaning": "execution_summary 尚未接入或缺失。",
            "evidence": {"reason": "missing"},
            "meta": {"status": "empty"},
        }

    # 即时条件（占位）
    conditions_runtime = {
        "status": "not_enforced",
        "note": (
            "即时验证条件尚未启用强制校验。"
            "当前 ActionHint 仅受 Gate 与结构性因素约束。"
        ),
    }

    # 隔夜环境摘要：从 Phase-2 全局代理中抽关键字段（表达层）
    overnight = {}
    global_lead = factors_bound.get("global_lead", {})
    index_global = factors_bound.get("index_global", {})

    if isinstance(global_lead, dict):
        for k in ("a50", "hsi"):
            if k in global_lead:
                overnight[f"{k}_proxy"] = global_lead.get(k)

    if isinstance(index_global, dict):
        for k in ("spx", "ndx", "vix", "dxy"):
            if k in index_global:
                overnight[f"{k}_proxy"] = index_global.get(k)

    if not overnight:
        overnight = {
            "note": (
                "未能从 Phase-2 全局代理中提取隔夜环境摘要。"
                "该信息仅用于背景说明，不影响 Gate / ActionHint。"
            )
        }

    meta = factors_bound.get("_meta", {})
    phase = meta.get("phase", "PHASE_2")
    meta["phase"] = phase
    
    prediction = factors_bound.get("_prediction")

    return {
        "gate": gate_pre,
        "gate_pre": gate_pre,          # ★
        # gate_final 在 ReportEngine 内部由 ActionHint 决定；Summary block 可读 slots["gate_final"]
        "gate_final": gate_pre,        # ★ 默认先等于 pre，ReportEngine 内如需覆盖可扩展（当前先冻结）

        "structure": structure,
        "watchlist": watchlist,

        "observations": observations,  # 保留原样（审计证据链）
        "drs": drs_slot,              # ★ 标准化字段

        "execution_summary": execution_summary,  # ★

        "conditions_runtime": conditions_runtime,
        "overnight": overnight,

        "_meta": meta,
        "_prediction": prediction,
    }


def _build_report_context(*, trade_date: str, slots: dict, kind: str) -> ReportContext:
    return ReportContext(kind=kind, trade_date=trade_date, slots=slots)


def _build_report_engine() -> ReportEngine:
    return ReportEngine(
        market="CN",
        actionhint_service=ActionHintService(),
        block_builders={
            "structure.facts": StructureFactsBlock().render,
            "summary": SummaryANDBlock().render,
            "execution.summary": ExecutionSummaryBlock().render,
            "exit.readiness": ExitReadinessBlock().render,
            "context.overnight": ContextOvernightBlock().render,
            "watchlist.sectors": WatchlistSectorsBlock().render,
            "execution.timing": ExecutionTimingBlock().render,
            "exposure.boundary": ExposureBoundaryBlock().render,
            "scenarios.forward": ScenariosForwardBlock().render,
            "dev.evidence": DevEvidenceBlock().render,
            "execution_quick_reference": ExecutionQuickReferenceBlock().render,
        },
    )


def _execute_report_pipeline(*, trade_date: str, gate_decision, factors_bound: dict, kind: str) -> str:
    
    
    slots = _prepare_report_slots(gate_decision=gate_decision, factors_bound=factors_bound)
    context = _build_report_context(trade_date=trade_date, slots=slots, kind=kind)

    engine = _build_report_engine()
    report_doc = engine.build_report(context=context)

    renderer = MarkdownRenderer()
    text = renderer.render(report_doc)

    writer = ReportWriter()
    report_path = writer.write(doc=report_doc, text=text)

    from core.cases.case_validator import validate_case
    
    validate_case(
        case_path="docs/cases/CASE-CN-20251226.yaml",
        gate_final=gate_decision.level,
        summary_code=getattr(report_doc, "summary", "N"),
        structure=factors_bound["structure"] ,
        report_text=text,
    )    


    return report_path

 

# =========================================================
# Phase-1/2 pipeline helpers
# =========================================================

def _fetch_snapshot(trade_date_str: str, is_intraday: bool = False, refresh_mode: str = "none") -> Dict[str, Any]:
    fetcher = AshareDataFetcher(trade_date=trade_date_str, is_intraday=is_intraday, refresh_mode=refresh_mode)
    snapshot: Dict[str, Any] = fetcher.prepare_daily_market_snapshot()
    return snapshot


def _compute_factors(snapshot: Dict[str, Any], phase:str="PHASE_2") -> dict[str, Any]:
    factor_list = [
        #NorthNPSTrendFactor(),
        UnifiedEmotionFactor(),
        ParticipationFactor(),
        GlobalMacroFactor(),
        IndexGlobalFactor(),
        GlobalLeadFactor(),
        NorthNPSFactor(),
        TurnoverFactor(),
        MarginFactor(),
        SectorRotationFactor(),
        IndexTechFactor(),
        BreadthFactor(),
        ETFIndexSyncFactor(),
        ETFIndexSyncDailyFactor(),
        TrendInForceFactor(),
        FRFFactor(),
        
    ]

    factors: dict[str, Any] = {}

    for factor in factor_list:
        try:
            fr = factor.compute(snapshot)
            factors[getattr(fr, "name", factor.__class__.__name__)] = fr
            LOG.info("[Factor.%s] score=%s level=%s", getattr(fr, "name", "?"), getattr(fr, "score", "?"), getattr(fr, "level", "?"))
            fr_raw_data  = getattr(fr, "details")["_raw_data"]
            #LOG.info(f"Factor raw data: {fr_raw_data}")
            assert fr_raw_data, "_raw_data is empty"
        except Exception as e:
            LOG.error("[Factor.%s] compute failed: %s", factor.__class__.__name__, e, exc_info=True)

    return factors


def _bind_policy_slots(factors: dict[str, Any]) -> dict:
    binder = ASharesPolicySlotBinder()
    factors_bound = binder.bind(factors)
    assert factors_bound.get("watchlist"), 'factors_bound["watchlist"] missing'
    return factors_bound


def _build_phase2_structures(
    factors: Dict[str, Any],
    factors_bound: Dict[str, Any],
    trade_date_str: str,
) -> Dict[str, Any]:
    structure = StructureFactsBuilder().build(factors=factors)
    factors_bound["structure"] = structure

    watchlist_state = WatchlistStateBuilder().build(
        factors=factors,
        structure=structure,
        watchlist_config=factors_bound.get("watchlist"),
    )
    factors_bound["watchlist"] = watchlist_state

    observations = {}
    try:
        drs = DRSObservation().build(inputs=structure, asof=trade_date_str)
        drs = DRSContinuity.apply(
            drs_obs=drs,
            asof=trade_date_str,
            fallback_state_path="state/drs_persistence.json",
        )
        observations["drs"] = drs
    except Exception as e:
        LOG.error("DRS failed: %s", e)

    factors_bound["observations"] = observations

    ###factors_bound["execution_summary"] = ExecutionSummaryBuilder().build(
    ###    factors=factors,
    ###    structure=structure,
    ###    observations=observations,
    ###    asof=trade_date_str,
    ###)

    return factors_bound

def _build_execution_summary(
    *,
    factors: Dict[str, Any],
    structure: Dict[str, Any],
    observations: Dict[str, Any],
    asof: str,
) -> ExecutionSummary:
    
    execution_summary = ExecutionSummaryBuilder().build(
        factors=factors,
        structure=structure,
        observations=observations,
        asof=asof,
    )
    return  execution_summary.to_dict()

def _build_phase3_structure_distribution(
    *,
    structure: Dict[str, Any],
    factors: Dict[str, Any],
    asof: str,
) -> Dict[str, Any]:
    """
    Phase-3: Structure Distribution Continuity
    - 输入：Phase-2 structure
    - 输出：增强后的 structure
    """

    dist = StructureDistributionContinuity.apply(
        factors=factors,
        asof=asof,
        state_path="state/structure_distribution.json",
    )

    if not dist:
        return structure

    new_structure = dict(structure)
    regime = new_structure.setdefault("regime", {})
    regime["structure_distribution"] = dist

    return new_structure

def _make_gate_decision(snapshot: Dict[str, Any], slots: Dict[str, Any], factors: Dict[str, FactorResult]):
    decider = ASharesGateDecider()
    gate = decider.decide(snapshot=snapshot, slots=slots, factors=factors)
    snapshot["gate"] = {"level": gate.level, "reasons": gate.reasons}
    return gate, snapshot


def _generate_prediction_and_action(
    factors_bound: dict,
    snapshot: Dict[str, Any],
    trade_date_str: str,
) -> Tuple[Any, Dict[str, Any], Dict[str, Any]]:
    prediction_engine = PredictionEngine()
    prediction = prediction_engine.predict(factors_bound)

    meta = {"market": "cn", "trade_date": trade_date_str}

    factors_bound["_prediction"] = prediction
    factors_bound["_meta"] = meta
    action_hint = None
    return prediction, action_hint, meta


def _generate_phase3_report(trade_date_str: str, gate_decision, factors_bound: dict) -> str:
    report_path = _execute_report_pipeline(
        trade_date=trade_date_str,
        gate_decision=gate_decision,
        factors_bound=factors_bound,
        kind="PRE_OPEN",
    )

    LOG.info("[Engine.AshareDaily] Phase-3 report generated: %s", report_path)
    if not report_path:
        LOG.error("[Engine.AshareDaily] report_path is EMPTY")



    return report_path

 
 
def run_cn_ashare_daily(
    trade_date: str | None = None,
    is_intraday: bool = False,
    refresh_mode: str = "none",
) -> None:
    trade_date_str = _normalize_trade_date(trade_date)
    LOG.info("Run CN AShare Daily | trade_date=%s refresh=%s", trade_date_str, refresh_mode)

    snapshot = _fetch_snapshot(trade_date_str, is_intraday=is_intraday, refresh_mode=refresh_mode)
    factors = _compute_factors(snapshot)
    factors_bound = _bind_policy_slots(factors)
    factors_bound = _build_phase2_structures(factors, factors_bound, trade_date_str)

 
    # Phase-3: enhance structure (注入 structure["regime"]["structure_distribution"])
    factors_bound["structure"] = _build_phase3_structure_distribution(
        structure=factors_bound["structure"],
        factors=factors,
        asof=trade_date_str,
    )
    
    # ✅ 现在才 build execution_summary（能看到 phase-3）
    factors_bound["execution_summary"] = _build_execution_summary(
        factors=factors,
        structure=factors_bound["structure"],
        observations=factors_bound.get("observations", {}),
        asof=trade_date_str,
    )
    


    gate_decision, snapshot = _make_gate_decision(
        snapshot, slots=factors_bound, factors=factors
    )

    _generate_prediction_and_action(factors_bound, snapshot, trade_date_str)
    _generate_phase3_report(trade_date_str, gate_decision, factors_bound)


    ### 


    ####
    LOG.info("CN AShare Daily finished successfully.")
