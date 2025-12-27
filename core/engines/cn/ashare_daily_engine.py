# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime, timedelta, time
from typing import Any, Dict, Optional

import baostock as bs
import pandas as pd
import pytz

from core.utils.logger import get_logger
from core.adapters.fetchers.cn.ashare_fetcher import AshareDataFetcher

# ===== Factors =====
from core.factors.cn.unified_emotion_factor import UnifiedEmotionFactor
from core.factors.cn.participation_factor import ParticipationFactor
from core.factors.glo.global_macro_factor import GlobalMacroFactor
from core.factors.glo.index_global_factor import IndexGlobalFactor
from core.factors.glo.global_lead_factor import GlobalLeadFactor
from core.factors.cn.north_nps_factor import NorthNPSFactor
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

# ===== Regime / Governance =====
from core.regime.ashares_gate_decider import ASharesGateDecider
from core.regime.structure_distribution_continuity import StructureDistributionContinuity
from core.regime.observation.structure.structure_facts_builder import StructureFactsBuilder
from core.regime.observation.watchlist.watchlist_state_builder import WatchlistStateBuilder
from core.regime.observation.drs.drs_observation import DRSObservation
from core.regime.observation.drs_continuity import DRSContinuity
from core.governance.execution_summary_builder import ExecutionSummaryBuilder

# ===== Report =====
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
from core.cases.case_validator import validate_case

LOG = get_logger("Engine.AshareDaily")

A_SHARE_OPEN = time(9, 30)
A_SHARE_CLOSE = time(15, 0)


class AShareDailyEngine:
    """
    UnifiedRisk V12 · CN A-Share Daily Engine（严格 OOP 封装版）

    设计铁律：
    - 制度信号只在 Engine 内生成
    - Builder / Block / Case 不允许推断制度
    """

    def __init__(self, refresh_mode: str = "none") -> None:
        self.refresh_mode = refresh_mode
        self.is_intraday, self.trade_date = self._resolve_trade_time()

        # === 核心制度状态（只在 Engine 内部存在）===
        self._distribution_risk_active: bool = False

    # ==================================================
    # Public API
    # ==================================================
    def run(self) -> None:
        LOG.info("[AShareDailyEngine] run start: %s", self.trade_date)
        self._execute_pipeline()
        LOG.info("[AShareDailyEngine] run finished: %s", self.trade_date)

    # ==================================================
    # Core Pipeline（唯一入口）
    # ==================================================
    def _execute_pipeline(self) -> None:
        snapshot = self._fetch_snapshot()
        factors = self._compute_factors(snapshot)

        factors_bound = self._bind_policy_slots(factors)
        factors_bound = self._build_phase2(factors, factors_bound)

        # ===== Phase-3 / 制度状态（封装）=====
        self._distribution_risk_active = self._compute_distribution_risk(
            structure=factors_bound["structure"],
            factors=factors,
        )

        # ===== Structure（消费制度状态）=====
        factors_bound["structure"] = StructureFactsBuilder().build(
            factors=factors,
            distribution_risk_active=self._distribution_risk_active,
            drs_signal=self._extract_drs_signal(factors_bound),
        )

        factors_bound["execution_summary"] = self._build_execution_summary(
            factors=factors,
            structure=factors_bound["structure"],
            observations=factors_bound.get("observations", {}),
        )

        gate = self._make_gate_decision(factors_bound, factors)
        self._generate_prediction(factors_bound)
        self._generate_report(gate, factors_bound)

    # ==================================================
    # Institution Logic（不外溢）
    # ==================================================
    def _compute_distribution_risk(
        self,
        *,
        structure: Dict[str, Any],
        factors: Dict[str, FactorResult],
    ) -> bool:
        """
        冻结制度定义：
        - 连续结构恶化（由 continuity 模块维护）
        """
        dist = StructureDistributionContinuity.apply(
            factors=factors,
            asof=self.trade_date,
            state_path="state/structure_distribution.json",
        )
        return bool(dist)

    # ==================================================
    # Helpers（其余逻辑保持原语义）
    # ==================================================
    def _resolve_trade_time(self) -> tuple[bool, str]:
        tz = pytz.timezone("Asia/Shanghai")
        now = datetime.now(tz)
        trade_date = now.strftime("%Y-%m-%d")
        is_intraday = A_SHARE_OPEN <= now.time() < A_SHARE_CLOSE
        return is_intraday, trade_date

    def _fetch_snapshot(self) -> Dict[str, Any]:
        return AshareDataFetcher(
            trade_date=self.trade_date,
            is_intraday=self.is_intraday,
            refresh_mode=self.refresh_mode,
        ).prepare_daily_market_snapshot()

    def _compute_factors(self, snapshot: Dict[str, Any]) -> Dict[str, FactorResult]:
        factors = {}
        for factor in [
            UnifiedEmotionFactor(), ParticipationFactor(),
            GlobalMacroFactor(), IndexGlobalFactor(), GlobalLeadFactor(),
            NorthNPSFactor(), TurnoverFactor(), MarginFactor(),
            SectorRotationFactor(), IndexTechFactor(), BreadthFactor(),
            ETFIndexSyncFactor(), ETFIndexSyncDailyFactor(),
            TrendInForceFactor(), FRFFactor(),
        ]:
            fr = factor.compute(snapshot)
            factors[fr.name] = fr
        return factors

    def _bind_policy_slots(self, factors: Dict[str, FactorResult]) -> Dict[str, Any]:
        return ASharesPolicySlotBinder().bind(factors)

    def _build_phase2(self, factors, bound):
        structure = StructureFactsBuilder().build(factors=factors)
        bound["structure"] = structure
        bound["watchlist"] = WatchlistStateBuilder().build(
            factors=factors, structure=structure, watchlist_config=bound.get("watchlist")
        )
        drs = DRSObservation().build(inputs=structure, asof=self.trade_date)
        bound["observations"] = {
                                    "drs": DRSContinuity.apply(
                                        drs_obs=drs,
                                        asof=self.trade_date,
                                        fallback_state_path="state/drs_persistence.json",
                                    )
                                }
        return bound

    def _extract_drs_signal(self, bound: Dict[str, Any]) -> Optional[str]:
        return bound.get("observations", {}).get("drs", {}).get("observation", {}).get("signal")

    def _build_execution_summary(self, *, factors, structure, observations):
        return ExecutionSummaryBuilder().build(
            factors=factors, structure=structure, observations=observations, asof=self.trade_date
        ).to_dict()

    def _make_gate_decision(self, bound, factors):
        return ASharesGateDecider().decide(slots=bound, factors=factors)

    def _generate_prediction(self, bound):
        bound["_prediction"] = PredictionEngine().predict(bound)

    def _generate_report1(self, gate, bound):
        context = ReportContext(
            kind="PRE_OPEN",
            trade_date=self.trade_date,
            slots={
                "gate": gate.level,
                "structure": bound["structure"],
                "observations": bound.get("observations"),
                "execution_summary": bound.get("execution_summary"),
            },
        )
        engine = ReportEngine(
            market="CN",
            actionhint_service=ActionHintService(),
            block_builders={
                "structure.facts": StructureFactsBlock().render,
                "summary": SummaryANDBlock().render,
                "execution.summary": ExecutionSummaryBlock().render,
                "exit.readiness": ExitReadinessBlock().render,
            },
        )
        doc = engine.build_report(context)
        text = MarkdownRenderer().render(doc)
        ReportWriter().write(doc, text)

        validate_case(
            case_path="docs/cases/CASE-CN-20251226.yaml",
            gate_final=gate.level,
            summary_code=getattr(doc, "summary", "N"),
            structure=bound["structure"],
            report_text=text,
        )

    def _generate_report(self, gate, bound):
        context = ReportContext(
            kind="PRE_OPEN",
            trade_date=self.trade_date,
            slots={
                "gate": gate.level,
                "structure": bound["structure"],
                "observations": bound.get("observations"),
                "execution_summary": bound.get("execution_summary"),
            },
        )
    
        engine = ReportEngine(
            market="CN",
            actionhint_service=ActionHintService(),
            block_builders={
                "structure.facts": StructureFactsBlock().render,
                "summary": SummaryANDBlock().render,
                "execution.summary": ExecutionSummaryBlock().render,
                "exit.readiness": ExitReadinessBlock().render,
            },
        )
    
        doc = engine.build_report(context)
        text = MarkdownRenderer().render(doc)
        ReportWriter().write(doc, text)
    
        # ❗ 不做任何 case 校验
        # Case 校验不属于 Engine 的职责
    