# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Tuple, Optional

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

import baostock as bs
import pandas as pd
import pytz
from datetime import datetime, timedelta, time
A_SHARE_OPEN = time(9, 30)   # 开盘时间
A_SHARE_CLOSE = time(15, 0)  # 收盘时间


# =========================================================
# Engine Class
# =========================================================
class AShareDailyEngine:
    """
    UnifiedRisk V12 · CN A-Share Daily Engine (Class-based)

    职责：
    - 完整 orchestrate CN A 股日级制度流程
    - 显式生成制度信号（distribution_risk_active）
    - 统一状态流转，支持 Case 校验
    """
    def __init__(self, refresh_mode:str="none") -> None:
        self.refresh_mode=refresh_mode
        self.is_intraday, self.trade_date = self.get_intraday_status_and_last_trade_date()
    
    def get_intraday_status_and_last_trade_date(self) -> tuple[bool, str]:
        """
        判断当前是否为交易日盘中时间，并返回最近一个交易日
    
        Returns:
            (is_intraday: bool, last_trade_date: str)
                - is_intraday: True 表示当前是交易日且在 9:30~15:00 之间
                - last_trade_date: 最近的交易日（'YYYY-MM-DD'）
        """
        tz = pytz.timezone('Asia/Shanghai')
        now = datetime.now(tz)
    
        # 1. 确定参考日期（盘前8点前算前一天）
        if now.hour < 8:
            reference_date = (now - timedelta(days=1)).date()
        else:
            reference_date = now.date()
    
        # 2. 登录 baostock
        lg = bs.login()
        if lg.error_code != '0':
            print(f"baostock login failed: {lg.error_msg}")
            # 降级处理：仅判断周末 + 时间段（不考虑节假日）
            is_weekend = reference_date.weekday() >= 5
            in_session = A_SHARE_OPEN <= now.time() < A_SHARE_CLOSE
            fallback_last_date = reference_date - timedelta(days=(reference_date.weekday() + 2) % 7 if is_weekend else 0)
            return (not is_weekend and in_session, fallback_last_date.strftime('%Y-%m-%d'))
    
        try:
            # 3. 查询最近60天的交易日历（足够覆盖节假日）
            start_date = (reference_date - timedelta(days=60)).strftime('%Y-%m-%d')
            end_date = reference_date.strftime('%Y-%m-%d')
    
            rs = bs.query_trade_dates(start_date=start_date, end_date=end_date)
            if rs.error_code != '0':
                raise Exception(f"query_trade_dates failed: {rs.error_msg}")
    
            data_list = []
            while rs.next():
                data_list.append(rs.get_row_data())
    
            trade_df = pd.DataFrame(data_list, columns=rs.fields)
            trade_df['calendar_date'] = pd.to_datetime(trade_df['calendar_date'])
            trade_df['is_trading_day'] = trade_df['is_trading_day'].astype(int)
    
            # 提取所有交易日并降序排列
            trading_days = trade_df[trade_df['is_trading_day'] == 1]['calendar_date'].dt.date.values
    
            if len(trading_days) == 0:
                raise Exception("No trading days found in the past 60 days")
    
            # 4. 找到最近的交易日（从 reference_date 往前找）
            last_trade_date_obj = max(d for d in trading_days if d <= reference_date)
            last_trade_date_str = last_trade_date_obj.strftime('%Y-%m-%d')
    
            # 5. 判断是否为盘中时间
            # 只有当今天就是交易日，且当前时间在开盘后收盘前，才算盘中
            is_today_trading_day = last_trade_date_obj == now.date()
            in_trading_hours = A_SHARE_OPEN <= now.time() < A_SHARE_CLOSE
            is_intraday = is_today_trading_day and in_trading_hours
            

            trade_date_str = self._normalize_trade_date(last_trade_date_str)

            return is_intraday, trade_date_str
    
        except Exception as e:
            print(f"Error in get_intraday_status_and_last_trade_date: {e}")
            # 降级：仅判断周末和时间段
            is_weekend = reference_date.weekday() >= 5
            in_session = A_SHARE_OPEN <= now.time() < A_SHARE_CLOSE
            fallback_last = reference_date - timedelta(days=(reference_date.weekday() - 4) % 7)
            return (not is_weekend and in_session, fallback_last.strftime('%Y-%m-%d'))
    
        finally:
            bs.logout()
    
    
    # =========================
    # Public API
    # =========================

    # ==================================================
    # Public API
    # ==================================================
    def run(self, ) -> None:
        """
        运行 CN A 股日级制度引擎（EOD / Pre-open 共用）

        参数：
        - trade_date: YYYY-MM-DD
        """
        LOG.info("[ASharesDailyEngine] run start: %s", self.trade_date)

        self._execute_report_pipeline()

        LOG.info("[ASharesDailyEngine] run finished: %s", self.trade_date)


    def _execute_report_pipeline(self) -> None:
        
        LOG.info("Run CN AShare Daily | trade_date=%s refresh=%s", self.trade_date, self.refresh_mode)

        snapshot = self._fetch_snapshot( )
        # =========================
        # 2. Factors
        # =========================
        factors = self._compute_factors(snapshot)

        factors_bound = self._bind_policy_slots(factors)
        factors_bound = self._build_phase2_structures(factors, factors_bound)
    
     
        # Phase-3: enhance structure (注入 structure["regime"]["structure_distribution"])
        factors_bound["structure"] = self._build_phase3_structure_distribution(
            structure=factors_bound["structure"],
            factors=factors,
            
        )
        
        # ✅ 现在才 build execution_summary（能看到 phase-3）
        factors_bound["execution_summary"] = self._build_execution_summary(
            factors=factors,
            structure=factors_bound["structure"],
            observations=factors_bound.get("observations", {}),
           
        )
        
    
    
        gate_decision, snapshot = self._make_gate_decision(
            snapshot = snapshot, slots=factors_bound, factors=factors
        )
        # snapshot no needed any more ?
        self._generate_prediction_and_action(factors_bound = factors_bound, )
        self._generate_phase3_report( gate_decision, factors_bound)
          

        LOG.info("CN AShare Daily finished successfully.")

    # ==================================================
    # Helpers
    # ==================================================
    def _normalize_trade_date(self, trade_date: Optional[str]) -> str:
        if trade_date:
            s = str(trade_date).strip()
            if len(s) == 8 and s.isdigit():
                return f"{s[:4]}-{s[4:6]}-{s[6:]}"
            return s
        return datetime.now().strftime("%Y-%m-%d")

    def _fetch_snapshot(self ) -> Dict[str, Any]:
        fetcher = AshareDataFetcher(
            trade_date=self.trade_date,
            is_intraday= self.is_intraday,
            refresh_mode=self.refresh_mode,
        )
        return fetcher.prepare_daily_market_snapshot()

    def _compute_factors(self, snapshot: Dict[str, Any]) -> Dict[str, FactorResult]:
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
            ETFIndexSyncDailyFactor(),
            TrendInForceFactor(),
            FRFFactor(),
        ]

        factors: Dict[str, FactorResult] = {}
        for factor in factor_list:
            try:
                fr = factor.compute(snapshot)
                factors[fr.name] = fr
                LOG.info("[Factor.%s] score=%s level=%s", fr.name, fr.score, fr.level)
                assert fr.details.get("_raw_data"), "_raw_data missing"
            except Exception as e:
                LOG.error("[Factor.%s] compute failed: %s", factor.__class__.__name__, e, exc_info=True)

        return factors

    def _bind_policy_slots(self, factors: Dict[str, FactorResult]) -> Dict[str, Any]:
        binder = ASharesPolicySlotBinder()
        bound = binder.bind(factors)
        assert bound.get("watchlist"), "watchlist missing"
        return bound

    def _build_phase2_structures(
            self,
            factors: Dict[str, FactorResult], 
            factors_bound: Dict[str, Any] ) -> Dict[str, Any]:
        
        structure = StructureFactsBuilder().build(factors=factors)
        factors_bound["structure"] = structure

        factors_bound["watchlist"] = WatchlistStateBuilder().build(
            factors=factors,
            structure=structure,
            watchlist_config=factors_bound.get("watchlist"),
        )

        observations = {}
        try:
            drs = DRSObservation().build(inputs=structure, asof=self.trade_date)
            drs = DRSContinuity.apply(
                drs_obs=drs,
                asof=self.trade_date,
                fallback_state_path="state/drs_persistence.json",
            )
            observations["drs"] = drs
        except Exception as e:
            LOG.error("DRS failed: %s", e)

        factors_bound["observations"] = observations
        return factors_bound

    def _build_execution_summary(
        self,
        *,
        factors: Dict[str, FactorResult],
        structure: Dict[str, Any],
        observations: Dict[str, Any],
         
    ) -> Dict[str, Any]:
        return ExecutionSummaryBuilder().build(
            factors=factors,
            structure=structure,
            observations=observations,
            asof=self.trade_date,
        ).to_dict()

    def _build_phase3_structure_distribution(
        self,
        *,
        structure: Dict[str, Any],
        factors: Dict[str, FactorResult],
         
    ) -> Dict[str, Any]:
        
        asof=self.trade_date
        dist = StructureDistributionContinuity.apply(
            factors=factors,
            asof=self.trade_date,
            state_path="state/structure_distribution.json",
        )
        if not dist:
            return structure

        new_structure = dict(structure)
        new_structure.setdefault("regime", {})["structure_distribution"] = dist
        return new_structure

    def _make_gate_decision(
        self,
        snapshot: Dict[str, Any],
        slots: Dict[str, Any],
        factors: Dict[str, FactorResult],
    ):
        decider = ASharesGateDecider()
        gate = decider.decide(  slots=slots, factors=factors)
        snapshot["gate"] = {"level": gate.level, "reasons": gate.reasons}
        return gate, snapshot

    def _generate_prediction_and_action(
        self,
        *,
        factors_bound: Dict[str, Any],
         
    ) -> None:
        prediction = PredictionEngine().predict(factors_bound)
        factors_bound["_prediction"] = prediction
        factors_bound["_meta"] = {"market": "cn", "trade_date": self.trade_date}

    # ==================================================
    # Report
    # ==================================================
    def _generate_phase3_report(
        self,
        gate_decision,
        factors_bound: Dict[str, Any],
    ) -> str:

        slots = self._prepare_report_slots(gate_decision, factors_bound)
        context = ReportContext(kind="PRE_OPEN", trade_date=self.trade_date, slots=slots)

        engine = self._build_report_engine()
        report_doc = engine.build_report(context=context)

        text = MarkdownRenderer().render(report_doc)
        path = ReportWriter().write(doc=report_doc, text=text)

        validate_case(
            case_path="docs/cases/CASE-CN-20251226.yaml",
            gate_final=gate_decision.level,
            summary_code=getattr(report_doc, "summary", "N"),
            structure=factors_bound["structure"],
            report_text=text,
        )

        LOG.info("[Engine.AshareDaily] report generated: %s", path)
        return path

    # ==================================================
    # Report helpers
    # ==================================================
    def _prepare_report_slots(self, gate_decision, factors_bound: Dict[str, Any]) -> Dict[str, Any]:
        structure = factors_bound.get("structure")
        watchlist = factors_bound.get("watchlist")
        observations = factors_bound.get("observations")

        drs_slot = self._extract_drs_slot(observations)

        execution_summary = factors_bound.get("execution_summary") or {
            "code": "N",
            "band": "NA",
            "meaning": "execution_summary missing",
            "evidence": {},
            "meta": {"status": "empty"},
        }

        overnight = {}

        return {
            "gate": gate_decision.level,
            "gate_pre": gate_decision.level,
            "gate_final": gate_decision.level,
            "structure": structure,
            "watchlist": watchlist,
            "observations": observations,
            "drs": drs_slot,
            "execution_summary": execution_summary,
            "overnight": overnight,
            "_meta": factors_bound.get("_meta", {}),
            "_prediction": factors_bound.get("_prediction"),
        }

    def _extract_drs_slot(self, observations: Any) -> Dict[str, Any]:
        if not isinstance(observations, dict):
            return {"signal": "NA", "meaning": "observations missing", "status": "empty"}
        drs = observations.get("drs")
        if not isinstance(drs, dict):
            return {"signal": "NA", "meaning": "DRS missing", "status": "empty"}
        obs = drs.get("observation") or drs.get("payload") or {}
        return {
            "signal": obs.get("signal", "NA"),
            "meaning": obs.get("meaning", "NA"),
            "status": drs.get("meta", {}).get("status", "ok"),
        }

    def _build_report_engine(self) -> ReportEngine:
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
    
 
    # ==================================================
    # Institution Signal Detection
    # ==================================================
    def _detect_distribution_risk(
        self,
        *,
        snapshot,
        trade_date: str,
    ) -> bool:
        """
        结构性分布风险检测（制度信号）

        定义（冻结）：
        - 最近 N 日内，结构恶化信号出现次数 ≥ K
        """

        # ⚠️ 制度参数（冻结，不写入 Builder）
        WINDOW_DAYS = 3
        THRESHOLD = 3

        tracker = snapshot.get("governance_tracker")
        if tracker is None:
            return False

        deterioration_count = tracker.count_recent(
            key="structure_deterioration",
            window=WINDOW_DAYS,
            trade_date=trade_date,
        )

        return deterioration_count >= THRESHOLD    
