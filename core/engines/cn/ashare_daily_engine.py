# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime, timedelta, time
from typing import Any, Dict, Optional, Tuple

import baostock as bs
import pandas as pd
import pytz
import os, json 
import sqlite3

from core.persistence.sqlite.sqlite_l2_publisher import SqliteL2Publisher
from core.reporters.report_blocks.etf_spot_sync_explain_blk import EtfSpotSyncExplainBlock
from core.reporters.report_blocks.market_overview_blk import MarketOverviewBlock
from core.utils.logger import get_logger
from core.adapters.fetchers.cn.ashare_fetcher import AshareDataFetcher

# ===== Factors =====
from core.factors.cn.unified_emotion_factor import UnifiedEmotionFactor
from core.factors.cn.participation_factor import ParticipationFactor
from core.factors.glo.global_macro_factor import GlobalMacroFactor
from core.factors.glo.index_global_factor import IndexGlobalFactor
from core.factors.glo.global_lead_factor import GlobalLeadFactor
#from core.factors.cn.north_nps_factor import NorthNPSFactor
from core.factors.cn.north_proxy_pressure_factor import NorthProxyPressureFactor
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
from core.regime.ashares_gate_decider import ASharesGateDecider, GateDecision
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
def connect_sqlite(db_path: str, timeout: float = 30.0) -> sqlite3.Connection:
    """Create a configured SQLite connection for UnifiedRisk persistence."""
    conn = sqlite3.connect(db_path, timeout=timeout)
    conn.row_factory = sqlite3.Row
    return conn


def _wrap_factor(fr: Any) -> Dict[str, Any]:
    """FactorResult -> dict wrapper (for blocks expecting dict)."""
    if fr is None:
        return {}
    return {
        "score": getattr(fr, "score", None),
        "level": getattr(fr, "level", None),
        "details": getattr(fr, "details", None),
    }


class AShareDailyEngine:
    """
    UnifiedRisk V12 · CN A-Share Daily Engine（严格 OOP 封装版）

    设计铁律：
    - 制度信号只在 Engine 内生成
    - Builder / Block / Case 不允许推断制度
    """

    def __init__(self, refresh_mode: str = "none") -> None:
        self.refresh_mode = refresh_mode
        self.snapshot = None
        self.factors = None
        self.gate = None

        # === 核心制度状态（只在 Engine 内部存在）===
        self._distribution_risk_active: bool = False
        
        self._resolve_trade_time()
        #test 
        #self.trade_date="2025-12-29"
        #self.report_kind ="EOD"
        #self.is_intraday = False
         




    def _resolve_trade_time(self) -> tuple[bool, str]:
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


            self.is_intraday = is_today_trading_day and in_trading_hours
            

            #self.trade_date = self._normalize_trade_date(last_trade_date_str)
            self.trade_date = last_trade_date_str
            if self.is_intraday: 
                self.report_kind = "Intraday"
            elif is_today_trading_day and now.time() < time(9,30) and now.time() > time(7,30):
               self.report_kind = "PRE_OPEN"
            else:
               self.report_kind = "EOD"

            #### test
        
    
        except Exception as e:
            print(f"Error in get_intraday_status_and_last_trade_date: {e}")
            # 降级：仅判断周末和时间段
            is_weekend = reference_date.weekday() >= 5
            in_session = A_SHARE_OPEN <= now.time() < A_SHARE_CLOSE
            fallback_last = reference_date - timedelta(days=(reference_date.weekday() - 4) % 7)
            self.report_kind = "EOD"
            self.is_intraday = False
            self.trade_date =  fallback_last.strftime('%Y-%m-%d')
            #return (not is_weekend and in_session, fallback_last.strftime('%Y-%m-%d'))
    
        finally:
            bs.logout()
        



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
        
        
        self.snapshot = self._fetch_snapshot()
        
        
        self.factors = self._compute_factors(self.snapshot)

        factors_bound = self._bind_policy_slots()
        factors_bound = self._build_phase2( factors_bound)

        # ===== Phase-3 / 制度状态（封装）=====
        self._distribution_risk_active = self._compute_distribution_risk(
            structure=factors_bound["structure"],
            
        )

        # ===== Structure（消费制度状态）=====
        factors_bound["structure"] = StructureFactsBuilder().build(
            factors = self.factors,
            distribution_risk_active=self._distribution_risk_active,
            drs_signal=self._extract_drs_signal(factors_bound),
        )

        factors_bound["execution_summary"] = self._build_execution_summary(
            
            structure=factors_bound["structure"],
            observations=factors_bound.get("observations", {}),
        )

        self.gate = self._make_gate_decision(factors_bound)
        self._generate_prediction(factors_bound)
        report_text , des_payload = self._generate_report( factors_bound)
     
        ########## presiste  ###########
        self.presiste_data(report_text=report_text, des_payload= des_payload)

    def presiste_data(self, report_text: str, des_payload:dict):

        from core.persistence.sqlite.sqlite_run_persistence import SqliteRunPersistence
        from pathlib import Path
        UNIFIEDRISK_DB_PATH = r"./data/persistent/unifiedrisk.db"
        db_path = Path(UNIFIEDRISK_DB_PATH)
        #if db_path.exists():
        #   db_path.unlink()

        conn = connect_sqlite(str(db_path))

        run_persist= SqliteRunPersistence(conn)
        publisher = SqliteL2Publisher(conn)         
        
        now = datetime.now()
        engine_version = "V_"+ now.strftime("%Y-%m-%d_%H:%M:%S")
         
        format_string = "%Y-%m-%d"

        run_id = run_persist.start_run(
                trade_date=self.trade_date,
                report_kind = self.report_kind,
                engine_version=engine_version,
            )
        
        run_persist.record_snapshot(run_id, "internal_snapshot", self.snapshot)
        
        run_persist.record_gate(run_id = run_id,payload = des_payload)

        for factor_name, fr in self.factors.items():
            run_persist.record_factor(
                run_id,
                factor_name,
                fr.to_dict(),
                factor_version="" # factor_result.get("version"),
            )


        publisher.publish(
            trade_date=self.trade_date,
            report_kind = self.report_kind,
            report_text = report_text, des_payload=des_payload,
            engine_version=engine_version,
            meta={"run_id": run_id},
        )

        
        #run_persist.record_factor(run_id, "breadth", breadth_result)
        #run_persist.record_factor(run_id, "new_top50_lows", ntl_result)
    # end def 

 
        
        
    


    ###################################################### 
        ################# 对账 ， 
        #self._dump_factors(factors)
        #from core.recon.reconciliation_engine import ReconciliationEngine
        #import os , json
        #json_path = os.path.join( "runs/recon", f"snapshot_{self.trade_date}.json")
        #with open(json_path, "w", encoding="utf-8") as f:
        #    json.dump(snapshot, f, ensure_ascii=False, indent=2)

        #recon = ReconciliationEngine()
        #recon.run(
        #    trade_date=self.trade_date,
        #    snapshot=snapshot,
        #    factors=factors,
        #    structure=factors_bound["structure"],
        #    gate_level=gate.level,    
        #)


    def _dump_factors(
        self,
        *,
        
        output_dir: str = "runs/factors",
    ) -> None:
        os.makedirs(output_dir, exist_ok=True)
    
        payload = {}
        for name, fr in self.factors.items():
            payload[name] = {
                "score": fr.score,
                "level": fr.level,
                "details": fr.details,
            }
    
        path = os.path.join(output_dir, f"factors_{self.trade_date}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    
#      

    # ==================================================
    # Institution Logic（不外溢）
    # ==================================================
    def _compute_distribution_risk(
        self,
        *,
        structure: Dict[str, Any],
        
    ) -> bool:
        """
        冻结制度定义：
        - 连续结构恶化（由 continuity 模块维护）
        """
        dist = StructureDistributionContinuity.apply(
            factors=self.factors,
            asof=self.trade_date,
            state_path="state/structure_distribution.json",
        )
        return bool(dist)





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
            NorthProxyPressureFactor(), TurnoverFactor(), MarginFactor(),
            SectorRotationFactor(), IndexTechFactor(), BreadthFactor(),
            ETFIndexSyncFactor(), ETFIndexSyncDailyFactor(),
            TrendInForceFactor(), FRFFactor(),
        ]:
            fr = factor.compute(snapshot)
            factors[fr.name] = fr
        return factors

    def _bind_policy_slots(self, ) -> Dict[str, Any]:
        return ASharesPolicySlotBinder().bind(self.factors)

    def _build_phase2(self,  bound):
        structure = StructureFactsBuilder().build(factors=self.factors)
        bound["structure"] = structure
        bound["watchlist"] = WatchlistStateBuilder().build(
            factors=self.factors, structure=structure, watchlist_config=bound.get("watchlist")
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

    def _build_execution_summary(self, *,  structure, observations):
        return ExecutionSummaryBuilder().build(
            factors=self.factors, structure=structure, observations=observations, asof=self.trade_date
        ).to_dict()

    def _make_gate_decision(self, bound,):
        return ASharesGateDecider().decide(slots=bound, factors=self.factors)

    def _generate_prediction(self, bound):
        bound["_prediction"] = PredictionEngine().predict(bound)


    
    def _generate_report(self, bound: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        # --- governance extract (你已有) ---
        drs = self._extract_drs_signal(bound)
    
        frf_obj = bound.get("failure_rate")
        frf = getattr(frf_obj, "level", "") if frf_obj is not None else ""
    
        # --- slots（事实层）---
        slots: Dict[str, Any] = {
            #"gate": self.gate.level,
            # V12 preferred
            "governance": {
                "gate": {"raw_gate": self.gate.level, "final_gate": self.gate.level},
                "drs": {"signal": drs},
            },

            "drs": {"signal": drs},
            "structure": bound.get("structure") or {},
            "observations": bound.get("observations") or {},
            "execution_summary": bound.get("execution_summary"),
            # 供 market.overview block 使用（存在什么就透传什么，不在此处“编造事实”）
            "market_overview": (
                bound.get("market_overview")
                or bound.get("market_close_facts")
                or bound.get("close_facts")
                or {}
            ),
            # 供 etf_spot_sync.explain block 使用（允许 intraday_overlay 作为备用）
            "intraday_overlay": bound.get("intraday_overlay") or {},
        }
    
        # 将 etf_index_sync / etf_spot_sync factor 统一挂到 slots["etf_spot_sync"]
        #（你的 explain block 优先读 slots["etf_spot_sync"]）
        for k in ("etf_spot_sync", "etf_index_sync", "etf_index_sync_daily", "etf_spot_sync_raw"):
            fr = self.factors.get(k) if isinstance(getattr(self, "factors", None), dict) else None
            if fr is not None:
                slots["etf_spot_sync"] = _wrap_factor(fr)  # 内含 details
                break
    
        context = ReportContext(
            kind=self.report_kind,
            trade_date=self.trade_date,
            slots=slots,
        )
    
        # --- persistence payload（你已有，保持兼容）---
        des_payload = {
            "context": {
                "trade_date": self.trade_date,
                "report_kind": self.report_kind,
                "engine_version": "",  # init
            },
            "factors": {
                name: {"score": fr.score, "level": fr.level, "details": fr.details}
                for name, fr in self.factors.items()
            },
            "structure": bound.get("structure") or {},
            "governance": {
                "gate": self.gate.level,
                "drs": drs,
                "frf": frf,
            },
            "rule_trace": "",  # init
        }
    
        engine = ReportEngine(
            market="CN",
            actionhint_service=ActionHintService(),
            block_builders={
                #"market.overview": MarketOverviewBlock().render,
                "structure.facts": StructureFactsBlock().render,
                #"etf_spot_sync.explain": EtfSpotSyncExplainBlock().render,
                "summary": SummaryANDBlock().render,
                "execution.summary": ExecutionSummaryBlock().render,
                "exit.readiness": ExitReadinessBlock().render,
            },
        )
    
        doc = engine.build_report(context)

        path = os.path.join(r"run\reports", f"doc_{self.trade_date}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(des_payload, f, ensure_ascii=False, indent=2)

        text = MarkdownRenderer().render(doc)
        ReportWriter().write(doc, text)
        return text, des_payload
        