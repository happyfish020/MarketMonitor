# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - CN AShare Daily Engine

职责（V12 终态）：
- 作为系统 orchestration 层
- 组织 Fetcher → Snapshot → Factors → Prediction → Reporter
- 不解析业务结果
- 不输出人类可读文本
- 不向 main.py 返回业务数据
"""

from __future__ import annotations

from typing import Dict, Any, Tuple
from datetime import datetime

from core.utils.logger import get_logger
from core.adapters.fetchers.cn.ashare_fetcher import AshareDataFetcher

from core.factors.cn.unified_emotion_factor import UnifiedEmotionFactor
from core.factors.cn.margin_factor import MarginFactor
from core.factors.cn.north_nps_factor import NorthNPSFactor
from core.factors.cn.turnover_factor import TurnoverFactor
from core.factors.cn.sector_rotation_factor import SectorRotationFactor
from core.factors.cn.index_tech_factor import IndexTechFactor
from core.factors.cn.etf_index_sync_factor import ETFIndexSyncFactor
from core.factors.cn.participation_factor import ParticipationFactor
from core.factors.cn.trend_in_force_factor import TrendInForceFactor
from core.factors.glo.global_macro_factor import GlobalMacroFactor
from core.factors.glo.global_lead_factor import GlobalLeadFactor
from core.factors.glo.index_global_factor import IndexGlobalFactor
from core.factors.cn.breadth_factor import BreadthFactor
from core.factors.cn.frf_factor import FRFFactor

from core.factors.factor_result import FactorResult
from core.adapters.policy_slot_binders.cn.ashares_policy_slot_binder import ASharesPolicySlotBinder
from core.predictors.prediction_engine import PredictionEngine

from core.regime.ashares_gate_decider import ASharesGateDecider
from core.regime.observation.structure.structure_facts_builder import StructureFactsBuilder
from core.regime.observation.watchlist.watchlist_state_builder import WatchlistStateBuilder

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
from core.reporters.report_blocks.exposure_boundary_blk import  ExposureBoundaryBlock

#from core.actions.action_hint_builder import build_action_hint

LOG = get_logger("Engine.AshareDaily")


def _normalize_trade_date(trade_date: str | None) -> str:
    if trade_date:
        s = str(trade_date).strip()
        if len(s) == 8 and s.isdigit():
            return f"{s[:4]}-{s[4:6]}-{s[6:]}"
        return s
    return datetime.now().strftime("%Y-%m-%d")


# =========================================================
# Phase-3 Report pipeline helpers (保留你原来的架构)
# =========================================================

def _prepare_report_slots(*, gate_decision, factors_bound: dict) -> dict:
    gate = gate_decision.level

    # Phase-2 结构事实：必须来自 factors_bound["structure"]
    structure = factors_bound.get("structure")

    observations = factors_bound.get("observations")


    # 观察对象：Phase-2 生成后的 watchlist_state
    watchlist = factors_bound.get("watchlist")

    # 即时条件（占位，不强制）
    conditions_runtime = {
        "status": "not_enforced",
        "note": (
            "即时验证条件尚未启用强制校验。"
            "当前 ActionHint 仅受 Gate 与结构性因素约束。"
        ),
    }

    # 隔夜环境摘要：从 Phase-2 全局代理中抽关键字段
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

    # ---- 额外：把 meta/prediction/action_hint 放入 slots（只做表达，不改变制度计算）
    meta = factors_bound.get("_meta", {})
    prediction = factors_bound.get("_prediction")
    #action_hint = factors_bound.get("_action_hint")

    return {
        "gate": gate,
        "structure": structure,
        "watchlist": watchlist,
        "observations": observations,   # ★★★ 关键新增
        "conditions_runtime": conditions_runtime,
        "overnight": overnight,
        "_meta": meta,
        "_prediction": prediction,
        #"_action_hint": action_hint,
    }


def _build_report_context(*, trade_date: str, slots: dict, kind: str) -> ReportContext:
    return ReportContext(
        kind=kind,
        trade_date=trade_date,
        slots=slots,
    )


def _build_report_engine() -> ReportEngine:
    return ReportEngine(
        market="CN",
        actionhint_service=ActionHintService(),
        block_builders={
            "structure.facts": StructureFactsBlock().render,
            "summary": SummaryANDBlock().render,
            "context.overnight": ContextOvernightBlock().render,
            "watchlist.sectors": WatchlistSectorsBlock().render,
            #"conditions.runtime": ConditionsRuntimeBlock().render,
            "execution.timing": ExecutionTimingBlock().render,
            "exposure.boundary": ExposureBoundaryBlock().render,
            "scenarios.forward": ScenariosForwardBlock().render,
            "dev.evidence": DevEvidenceBlock().render,
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
    return report_path


# ========================== 拆分后的小函数（保持原数据流） ==========================

def _fetch_snapshot(trade_date_str: str,  is_intraday:bool=False, refresh_mode: str="none") -> Dict[str, Any]:
    fetcher = AshareDataFetcher(trade_date=trade_date_str, is_intraday =is_intraday,  refresh_mode=refresh_mode)
    snapshot: Dict[str, Any] = fetcher.prepare_daily_market_snapshot()
    return snapshot


def _compute_factors(snapshot: Dict[str, Any]) -> dict[str, FactorResult]:
    factor_list = [
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
        TrendInForceFactor(),
        FRFFactor(),  # <-- Step-2A: 接入 FRF（Failure-Rate Factor）
    ]

    factors: dict[str, FactorResult] = {}

    for factor in factor_list:
        try:
            fr = factor.compute(snapshot)
            factors[fr.name] = fr

            assert factors[fr.name], f"{fr.name} is missing"
            LOG.info("[Factor.%s] score=%.2f level=%s", fr.name, fr.score, fr.level)
        except Exception as e:
            LOG.error("[Factor.%s] compute failed: %s", factor.__class__.__name__, e, exc_info=True)

    return factors


def _bind_policy_slots(factors: dict[str, FactorResult]) -> dict:
    binder = ASharesPolicySlotBinder()
    factors_bound = binder.bind(factors)

    # 保持你原来的硬校验（如果你希望“缺 watchlist 允许跑”，这里再改）
    assert factors_bound.get("watchlist"), 'factors_bound["watchlist"] missing'
    return factors_bound


def _build_phase2_structures(factors: dict[str, FactorResult], factors_bound: dict) -> dict:
    # -------------------------------------------------
    # Phase-2: Structure facts（冻结）
    # -------------------------------------------------
    structure_builder = StructureFactsBuilder()
    structure_facts = structure_builder.build(factors=factors)
    factors_bound["structure"] = structure_facts

    # -------------------------------------------------
    # Phase-2: Watchlist state（冻结）
    # -------------------------------------------------
    watchlist_config = factors_bound.get("watchlist")
    watchlist_builder = WatchlistStateBuilder()
    watchlist_state = watchlist_builder.build(
        factors=factors,
        structure=structure_facts,
        watchlist_config=watchlist_config,
    )
    factors_bound["watchlist"] = watchlist_state

    # -------------------------------------------------
    # Phase-2: Observations（新增 · 冻结合规）
    # -------------------------------------------------
    observations = factors_bound.get("observations")
    if not isinstance(observations, dict):
        observations = {}

    try:
        from core.regime.observation.drs.drs_observation import DRSObservation

        drs_observation = DRSObservation().build(
            inputs=structure_facts,
            asof=factors_bound.get("_meta", {}).get("trade_date", "NA"),
        )
        observations["drs"] = drs_observation

    except Exception as e :
        # 冻结铁律：Observation 构建失败不影响主流程
        raise e

    factors_bound["observations"] = observations

    return factors_bound
 

def _make_gate_decision(snapshot: Dict[str, Any], factors_bound: dict):
    decider = ASharesGateDecider()
    gate_decision = decider.decide(snapshot, factors_bound)

    snapshot["gate"] = {
        "level": gate_decision.level,
        "reasons": gate_decision.reasons,
        "evidence": gate_decision.evidence,
    }

    LOG.info(
        "[ASharesEngine] Gate | level=%s | reasons=%s | evidence=%s",
        gate_decision.level,
        gate_decision.reasons,
        gate_decision.evidence,
    )

    return gate_decision, snapshot


def _generate_prediction_and_action(
    factors_bound: dict,
    snapshot: Dict[str, Any],
    trade_date_str: str,
) -> Tuple[Any, Dict[str, Any], Dict[str, Any]]:
    """
    Phase-3：Prediction + ActionHint
    - 不改变制度逻辑
    - 只把结果写回 snapshot / factors_bound 供 report 表达层读取
    """
    prediction_engine = PredictionEngine()
    prediction = prediction_engine.predict(factors_bound)

    #action_hint = build_action_hint(snapshot)
    #snapshot["action_hint"] = action_hint

    meta = {
        "market": "cn",
        "trade_date": trade_date_str,
    }

    # 只作为 report 输入（表达层），不影响 gate/structure
    factors_bound["_prediction"] = prediction
    #factors_bound["_action_hint"] = action_hint
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


# ========================== 主函数（保持你要的清晰） ==========================

def run_cn_ashare_daily(trade_date: str | None = None, is_intraday: bool = False, refresh_mode: str = "auto") -> None:
    trade_date_str = _normalize_trade_date(trade_date)
    #
    #trade_date_str = "2025-12-18"
    LOG.info("Run CN AShare Daily | trade_date=%s refresh=%s", trade_date_str, refresh_mode)

    # Phase-1: Fetch
    snapshot = _fetch_snapshot(trade_date_str, refresh_mode)

    # Phase-1 → Phase-2: Factors
    factors = _compute_factors(snapshot)

    # Phase-2: Policy binding
    factors_bound = _bind_policy_slots(factors)

    # Phase-2: Structure & Watchlist
    factors_bound = _build_phase2_structures(factors, factors_bound)

    # Phase-2: Gate decision
    gate_decision, snapshot = _make_gate_decision(snapshot, factors_bound)

    # Phase-3: Action & Prediction
    _generate_prediction_and_action(factors_bound, snapshot, trade_date_str)

    # Phase-3: Report
    _generate_phase3_report(trade_date_str, gate_decision, factors_bound)

    LOG.info("CN AShare Daily finished successfully.")
