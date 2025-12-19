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

from typing import Dict, Any
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

from core.factors.glo.global_macro_factor import GlobalMacroFactor
from core.factors.glo.global_lead_factor import GlobalLeadFactor
from core.factors.glo.index_global_factor import IndexGlobalFactor
from core.factors.cn.breadth_factor import BreadthFactor

from core.factors.factor_result import FactorResult
from core.adapters.policy_slot_binders.cn.ashares_policy_slot_binder import ASharesPolicySlotBinder
from core.predictors.prediction_engine import PredictionEngine


from core.predictors.prediction_engine import PredictionEngine
from core.reporters.cn.ashare_daily_reporter import build_daily_report_text, save_daily_report
from core.regime.ashares_gate_decider import ASharesGateDecider

from core.regime.observation.structure.structure_facts_builder import (
    StructureFactsBuilder
)

from core.regime.observation.watchlist.watchlist_state_builder import (
    WatchlistStateBuilder
)

LOG = get_logger("Engine.AshareDaily")


def _normalize_trade_date(trade_date: str | None) -> str:
    if trade_date:
        s = str(trade_date).strip()
        if len(s) == 8 and s.isdigit():
            return f"{s[:4]}-{s[4:6]}-{s[6:]}"
        return s
    return datetime.now().strftime("%Y-%m-%d")


# =========================================================
# Report helpers (naming frozen)
# =========================================================

from core.reporters.report_context import ReportContext
from core.reporters.report_engine import ReportEngine
from core.reporters.renderers.markdown_renderer import MarkdownRenderer
from core.reporters.report_writer import ReportWriter

from core.actions.actionhint_service import ActionHintService
from core.actions.summary_mapper import SummaryMapper

from core.reporters.report_blocks.structure_facts_blk import StructureFactsBlock
from core.reporters.report_blocks.summary_a_n_d_blk import SummaryANDBlock
from core.reporters.report_blocks.watchlist_sectors_blk import WatchlistSectorsBlock
from core.reporters.report_blocks.context_overnight_blk import ContextOvernightBlock
from core.reporters.report_blocks.conditions_runtime_blk import ConditionsRuntimeBlock
from core.reporters.report_blocks.dev_evidence_blk import DevEvidenceBlock
from core.reporters.report_blocks.scenarios_forward_blk import ScenariosForwardBlock


####

def _prepare_report_slots(*, gate_decision, factors_bound) -> dict:
    """
    Prepare Phase-3 report slots.

    原则：
    - 不引入新的因子或评分
    - 仅对 Phase-2 结果做解释性聚合
    - slot 缺失允许，但尽量提供可解释内容
    """

    # 1️⃣ Gate（必须）
    gate = gate_decision.level

    # 2️⃣ Structure（Phase-2 冻结结构）
    # 若 Phase-2 已提供 structure，则直接使用
    # 否则 fallback 到 factors_bound（兼容旧结构）
    structure = factors_bound.get("structure", factors_bound)

    # 3️⃣ Watchlist（结构来源说明，而非推荐）
    watchlist = factors_bound.get("watchlist")
    if watchlist is None:
        watchlist = {
            "note": (
                "watchlist 未在 Phase-2 显式提供。"
                "当前报告未基于 sector_rotation 形成观察对象。"
            )
        }

    # 4️⃣ Conditions Runtime（执行前校验条件）
    # 当前版本不引入强制校验，仅提供占位说明
    conditions_runtime = {
        "status": "not_enforced",
        "note": (
            "即时验证条件尚未启用强制校验。"
            "当前 ActionHint 仅受 Gate 与结构性因素约束。"
        ),
    }

    # 5️⃣ Overnight（隔夜环境：解释性，不参与裁决）
    # 从 Phase-2 已有全局因子中提取“可读摘要”
    overnight = {}

    # 常见全局代理（存在才取）
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

    return {
        "gate": gate,
        "structure": structure,
        "watchlist": watchlist,
        "conditions_runtime": conditions_runtime,
        "overnight": overnight,
        # scenarios.forward / dev.evidence 由 block 内部基于现有 slot 生成
    }

###??
 
def _build_report_context(
    *,
    trade_date: str,
    slots: dict,
    kind: str,
) -> ReportContext:
    """
    构造只读 ReportContext（防污染边界）。
    """
    return ReportContext(
        kind=kind,
        trade_date=trade_date,
        slots=slots,
    )
 
def _build_report_engine() -> ReportEngine:
    """
    构造 ReportEngine（只做 wiring，不接数据）。
    """
    return ReportEngine(
        market="CN",
        actionhint_service=ActionHintService(),
        
        block_builders={
            # 🚨 只能用 block_alias
            "structure.facts": StructureFactsBlock().render,
            "summary": SummaryANDBlock().render,
            "context.overnight": ContextOvernightBlock().render,
            "watchlist.sectors": WatchlistSectorsBlock().render,
            "conditions.runtime": ConditionsRuntimeBlock().render,
            "scenarios.forward": ScenariosForwardBlock().render,
            "dev.evidence": DevEvidenceBlock().render,
        },
    )
##### report section to  be added above  


def _execute_report_pipeline(
    *,
    trade_date: str,
    gate_decision,
    factors_bound,
    kind: str,
) -> str:
    """
    执行完整 Report pipeline：
    slots → context → engine → document → render → write
    """
    slots = _prepare_report_slots(
        gate_decision=gate_decision,
        factors_bound=factors_bound,
    )

    context = _build_report_context(
        trade_date=trade_date,
        slots=slots,
        kind=kind,
    )

    engine = _build_report_engine()

    # 1️⃣ build document
    report_doc = engine.build_report(context=context)

    # 2️⃣ render
    renderer = MarkdownRenderer()
    text = renderer.render(report_doc)

    # 3️⃣ write（🚨 base_dir 必须来自 paths.yaml）
    writer = ReportWriter()
    report_path = writer.write(doc=report_doc, text=text)

    return report_path


def run_cn_ashare_daily(trade_date: str | None = None, refresh_mode: str = "auto") -> None:
    trade_date_str = _normalize_trade_date(trade_date)

    LOG.info(
        "Run CN AShare Daily | trade_date=%s refresh=%s",
        trade_date_str,
        refresh_mode,
    )

    # 1️⃣ Fetch snapshot
    fetcher = AshareDataFetcher(trade_date=trade_date_str, refresh_mode=refresh_mode)
    snapshot: Dict[str, Any] = fetcher.prepare_daily_market_snapshot()

    # 2️⃣ Factors
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
    ]

    #factors: Dict[str, Any] = {}
    ###
    # 1️⃣ 计算所有 Factor（raw）
    factors: dict[str, FactorResult] = {}
    
    for factor in factor_list:
        try:
            fr = factor.compute(snapshot)
            factors[fr.name] = fr
    
            assert factors[fr.name], f"{fr.name} is missing"
            LOG.info("[Factor.%s] score=%.2f level=%s", fr.name, fr.score, fr.level)
        except Exception as e:
            LOG.error("[Factor.%s] compute failed: %s", fr.name, e, exc_info=True)
    
    
    # 2️⃣ PolicySlotBinder（raw → 制度槽位）
    binder = ASharesPolicySlotBinder()
    factors_bound = binder.bind(factors)
    
    assert factors_bound.get("watchlist"), 'factors_bound["watchlist"] missing'
    
    
    # 3️⃣ Phase-2 · Structure Facts（Observation 层）
    from core.regime.observation.structure.structure_facts_builder import (
        StructureFactsBuilder
    )
    
    structure_builder = StructureFactsBuilder()
    structure_facts = structure_builder.build(factors=factors)
    
    # 写入 Phase-2 制度槽位

    factors_bound["structure"] = structure_facts




    watchlist_config = factors_bound.get("watchlist")
    
    watchlist_builder = WatchlistStateBuilder()
    watchlist_state = watchlist_builder.build(
        factors=factors,
        structure=structure_facts,
        watchlist_config=watchlist_config,
    )
    
    # 覆盖 / 丰富 watchlist 槽位（只读给 Phase-3）
    factors_bound["watchlist"] = watchlist_state
    
    ################ above is phase-2 ################
    
    
    # 4️⃣ Gate 决策（只读 structure / watchlist）
    decider = ASharesGateDecider()
    gate_decision = decider.decide(snapshot, factors_bound)
    
    snapshot["gate"] = {
        "level": gate_decision.level,
        "reasons": gate_decision.reasons,
        "evidence": gate_decision.evidence,
    }
    



    ###
    LOG.info(
        "[ASharesEngine] Gate | level=%s | reasons=%s | evidence=%s",
        gate_decision.level,
        gate_decision.reasons,
        gate_decision.evidence,
    )


# Phase-2 已完成
 

    # Phase-3 Action Governance
    from core.actions.action_hint_builder import build_action_hint
    action_hint = build_action_hint(snapshot)
    snapshot["action_hint"] = action_hint



    # 3️⃣ Prediction（只吃制度槽位）
    prediction_engine = PredictionEngine()
    prediction = prediction_engine.predict(factors_bound)

    # meta
    meta = {
        "market": "cn",
        "trade_date": trade_date_str,
    }

    # 4️⃣ Reporter

    ##### report ############
    # 4️⃣ Phase-3 Report (NEW)

    report_path = _execute_report_pipeline(
        trade_date=trade_date_str,
        gate_decision=gate_decision,
        factors_bound=factors_bound,
        kind="PRE_OPEN",
    )

    LOG.info("[Engine.AshareDaily] Phase-3 report generated: %s", report_path)
    if not report_path:
        LOG.error("[Engine.AshareDaily] report_text is EMPTY, skip saving")
        LOG.info("CN AShare Daily finished successfully.")
        return
 
    LOG.info("CN AShare Daily finished successfully.")
