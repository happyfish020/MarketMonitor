# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime, timedelta, time
import importlib
import inspect
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
from core.factors.cn.amount_factor import AmountFactor
from core.factors.cn.margin_factor import MarginFactor
from core.factors.cn.index_tech_factor import IndexTechFactor
from core.factors.cn.breadth_factor import BreadthFactor
from core.factors.cn.etf_index_sync_factor import ETFIndexSyncFactor
from core.factors.cn.etf_index_sync_daily_factor import ETFIndexSyncDailyFactor
from core.factors.cn.trend_in_force_factor import TrendInForceFactor
from core.factors.cn.frf_factor import FRFFactor
from core.factors.cn.sector_proxy_factor import SectorProxyFactor
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
from core.regime.observation.watchlist.watchlist_ob_builder import WatchlistObservationBuilder
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
        #self.trade_date="2025-12-30"
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
        


    def _load_yaml_file(self, path: str) -> Dict[str, Any]:
        """Load a YAML file as dict (fail-safe)."""
        from pathlib import Path
        import yaml

        if not isinstance(path, str) or not path.strip():
            return {}

        raw = path.strip()

        def _try(p: Path):
            if not p.exists() or not p.is_file():
                return None
            try:
                data = yaml.safe_load(p.read_text(encoding="utf-8"))
            except Exception:
                return None
            return data if isinstance(data, dict) else None

        p = Path(raw)
        if p.is_absolute():
            d = _try(p)
            return d if d is not None else {}

        d = _try(Path.cwd() / raw)
        if d is not None:
            return d

        here = Path(__file__).resolve()
        for parent in [here.parent] + list(here.parents)[:6]:
            d = _try(parent / raw)
            if d is not None:
                return d

        d = _try(p)
        return d if d is not None else {}


    def _load_weights_cfg(self) -> Dict[str, Any]:
        """Load weights.yaml (single source for config paths)."""
        cfg = self._load_yaml_file("config/weights.yaml")
        if isinstance(cfg, dict) and cfg:
            return cfg
        cfg = self._load_yaml_file("weights.yaml")
        return cfg if isinstance(cfg, dict) else {}
    


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
        # weights.yaml is the single source of truth for pipeline config
        self.weights_cfg = self._load_weights_cfg()
        fp = self.weights_cfg.get("factor_pipeline", {}) if isinstance(self.weights_cfg, dict) else {}
        structure_keys = fp.get("structure_factors", []) if isinstance(fp, dict) else []
        if not isinstance(structure_keys, list):
            structure_keys = []

        
        
        self.snapshot = self._fetch_snapshot()
        
        
        self.factors = self._compute_factors(self.snapshot)

        # policy slot binder 已弃用：Phase-2/Report 统一从 YAML 读取结构与语义；此处不再做额外绑定
        factors_bound: Dict[str, Any] = {}
        factors_bound = self._build_phase2( factors_bound)

        # ===== Phase-3 / 制度状态（封装）=====
        self._distribution_risk_active = self._compute_distribution_risk(
            structure=factors_bound["structure"],
            
        )

        # ===== Structure（消费制度状态）=====
        factors_bound["structure"] = StructureFactsBuilder().build(
            factors = self.factors,
            structure_keys=structure_keys,
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
        #self.presiste_data(report_text=report_text, des_payload= des_payload)

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

    def _load_factor_pipeline_cfg(self) -> Tuple[list, dict, dict]:
        """Load factor pipeline config from weights.yaml (single source of truth).
    
        Returns:
            enabled: list[str] - compute order
            registry: dict[str,str|dict] - factor name -> import spec
            params: dict[str,dict] - factor name -> init kwargs (optional)
        """
        cfg = self.weights_cfg if isinstance(getattr(self, "weights_cfg", None), dict) else {}
        fp = cfg.get("factor_pipeline", {}) if isinstance(cfg, dict) else {}
    
        enabled = fp.get("enabled", []) if isinstance(fp, dict) else []
        if not isinstance(enabled, list):
            enabled = []
    
        registry = fp.get("registry", {}) if isinstance(fp, dict) else {}
        if not isinstance(registry, dict):
            registry = {}
    
        params = fp.get("params", {}) if isinstance(fp, dict) else {}
        if not isinstance(params, dict):
            params = {}
    
        return enabled, registry, params
    
    def _import_obj(self, spec: str) -> Any:
        """Import an object by spec.
    
        Supported:
        - 'package.module:ClassName'
        - 'package.module.ClassName'
        """
        spec = (spec or "").strip()
        if not spec:
            raise ValueError("empty import spec")
    
        if ":" in spec:
            mod_path, attr = spec.split(":", 1)
        else:
            mod_path, _, attr = spec.rpartition(".")
            if not mod_path:
                raise ValueError(f"invalid import spec: {spec}")
    
        module = importlib.import_module(mod_path)
        obj = getattr(module, attr, None)
        if obj is None:
            raise ValueError(f"import failed: {spec} (attr not found)")
        return obj
    
    def _guess_factor_import_spec(self, name: str) -> Optional[str]:
        """Best-effort import spec guess when YAML registry is not provided.
    
        Note: This is only a fallback. Recommended is to define
        factor_pipeline.registry in config/weights.yaml.
        """
        # explicit exceptions (only for legacy compatibility)
        special = {
            "failure_rate": "core.factors.cn.frf_factor:FRFFactor",
            "etf_index_sync": "core.factors.cn.etf_index_sync_factor:ETFIndexSyncFactor",
            "etf_index_sync_daily": "core.factors.cn.etf_index_sync_daily_factor:ETFIndexSyncDailyFactor",
        }
        if name in special:
            return special[name]
    
        # try CN first, then GLO
        candidates = [
            f"core.factors.cn.{name}_factor",
            f"core.factors.glo.{name}_factor",
        ]
    
        # build class name: tokens -> Camel + Factor, with common acronym handling
        token_map = {"etf": "ETF", "ai": "AI", "nps": "NPS", "drs": "DRS", "dos": "DOS"}
        toks = [t for t in name.split("_") if t]
        camel = "".join(token_map.get(t, t[:1].upper() + t[1:]) for t in toks)
        cls_name = f"{camel}Factor"
    
        for mod in candidates:
            try:
                module = importlib.import_module(mod)
                if hasattr(module, cls_name):
                    return f"{mod}:{cls_name}"
            except Exception:
                continue
        return None
    
    def _instantiate_factor(self, cls: Any, params: Dict[str, Any]) -> Any:
        """Instantiate factor class with best-effort filtered kwargs."""
        if not isinstance(params, dict):
            params = {}
        try:
            sig = inspect.signature(cls.__init__)
            kwargs = {}
            for k, v in params.items():
                if k in sig.parameters and k != "self":
                    kwargs[k] = v
            return cls(**kwargs)
        except Exception:
            return cls()
    
    def _compute_factors(self, snapshot: Dict[str, Any]) -> Dict[str, FactorResult]:
        """Compute factors dynamically based on config/weights.yaml.
    
        Source of truth:
          weights.yaml -> factor_pipeline.enabled (+ optional registry/params)
    
        Behavior:
        - Each enabled factor is instantiated then compute(snapshot) is called.
        - Missing/failed factor will NOT crash the engine in UAT; instead it yields
          a NEUTRAL FactorResult with details.data_status = ERROR/MISSING.
        """
        enabled, registry, params_cfg = self._load_factor_pipeline_cfg()
        factors: Dict[str, FactorResult] = {}
    
        for name in enabled:
            if not isinstance(name, str) or not name.strip():
                continue
            key = name.strip()
    
            try:
                spec = None
                if key in registry:
                    raw = registry.get(key)
                    if isinstance(raw, str):
                        spec = raw.strip()
                    elif isinstance(raw, dict):
                        spec = str(raw.get("import") or raw.get("path") or "").strip()
                if not spec:
                    spec = self._guess_factor_import_spec(key)
    
                if not spec:
                    raise KeyError(f"factor not registered: {key} (set factor_pipeline.registry)")
    
                cls = self._import_obj(spec)
                factor = self._instantiate_factor(cls, params_cfg.get(key, {}))
                fr = factor.compute(snapshot)
    
                if not isinstance(fr, FactorResult):
                    raise TypeError(f"factor.compute() must return FactorResult, got={type(fr)}")
    
                # store under config key to keep YAML/structure alignment stable
                factors[key] = fr
                if fr.name != key:
                    LOG.warning("FactorResult.name mismatch: cfg=%s result=%s", key, fr.name)
    
            except Exception as e:
                LOG.exception("factor compute failed: %s", key)
                factors[key] = FactorResult(
                    name=key,
                    score=50.0,
                    level="NEUTRAL",
                    details={
                        "data_status": "ERROR" if not isinstance(e, KeyError) else "MISSING",
                        "error": str(e),
                    },
                )
    
        return factors
    def _bind_policy_slots(self) -> Dict[str, Any]:
        """Bind FactorResult dict into slots using YAML config (no hard-coded SLOT_MAP).

        Rules:
        - default: identity binding (slot name == factor key)
        - optional rename: weights.yaml -> factor_pipeline.rename {factor_key: slot_key}
        - inject watchlist observation (append-only, may raise if coverage missing)
        """
        cfg = self.weights_cfg if isinstance(getattr(self, "weights_cfg", None), dict) else {}
        rename = cfg.get("factor_pipeline", {}).get("rename", {})
        if not isinstance(rename, dict):
            rename = {}

        bound: Dict[str, Any] = {}
        for k, fr in (self.factors or {}).items():
            if not isinstance(k, str):
                continue
            slot = rename.get(k, k)
            bound[slot] = fr

        # watchlist injection (kept for backward compatibility)
        asof = getattr(self, "trade_date", None) or "unknown"
        try:
            watchlist = WatchlistObservationBuilder().build(slots=bound, asof=str(asof))
            bound["watchlist"] = watchlist
        except FileNotFoundError:
            # coverage 缺失属于配置错误：明确失败
            raise
        return bound

    def _build_phase2(self,  bound):
        cfg = self.weights_cfg if isinstance(getattr(self, "weights_cfg", None), dict) else {}
        fp = cfg.get("factor_pipeline", {}) if isinstance(cfg, dict) else {}
        structure_keys = fp.get("structure_factors", []) if isinstance(fp, dict) else []
        if not isinstance(structure_keys, list):
            structure_keys = []
        structure = StructureFactsBuilder().build(factors=self.factors or {}, structure_keys=structure_keys)
        bound["structure"] = structure
        #bound["watchlist"] = WatchlistStateBuilder().build(
        #    factors=self.factors, structure=structure, watchlist_config=bound.get("watchlist")
        #)
        drs = DRSObservation().build(inputs=structure, asof=self.trade_date)
        bound["observations"] = {
                                    "drs": DRSContinuity.apply(
                                        drs_obs=drs,
                                        asof=self.trade_date,
                                        fallback_state_path="state/drs_persistence.json",
                                    )
                                }
        bound["market_overview"] = self._build_market_overview_slot()
        return bound


    def _build_market_overview_slot(self) -> Dict[str, Any]:
        """
        Build report-only *factual* market overview for MarketOverviewBlock.

        Contract:
        - Read-only snapshot facts (no scoring / no gate implications).
        - Best-effort. Never raises: on failure return {} with error logged.
        - Shape strictly follows core/reporters/report_blocks/market_overview_blk.py expectation.
        """
        try:
            snapshot: Dict[str, Any] = self.snapshot if isinstance(self.snapshot, dict) else {}
            factors: Dict[str, Any] = self.factors if isinstance(self.factors, dict) else {}

            # 1) indices
            indices_raw = snapshot.get("index_core_raw")
            indices: Dict[str, Any] = {}
            if isinstance(indices_raw, dict):
                # keep all raw keys for fallback display
                indices.update(indices_raw)
                # report key alias (kcb50 expected by block, while DS uses kc50)
                if "kc50" in indices_raw and "kcb50" not in indices:
                    indices["kcb50"] = indices_raw.get("kc50")
                if "kcb50" in indices_raw and "kc50" not in indices:
                    indices["kc50"] = indices_raw.get("kcb50")

            # 2) amount (成交额)
            amount_total: Optional[float] = None
            fr_amount = factors.get("amount")
            if isinstance(fr_amount, FactorResult) and isinstance(fr_amount.details, dict):
                v = fr_amount.details.get("amount_total")
                if isinstance(v, (int, float)):
                    amount_total = float(v)

            amount_raw = snapshot.get("amount_raw") if isinstance(snapshot.get("amount_raw"), dict) else {}
            if amount_total is None:
                v = amount_raw.get("total_amount")
                if isinstance(v, (int, float)):
                    amount_total = float(v)

            delta: Optional[float] = None
            window = amount_raw.get("window")
            if isinstance(window, list) and len(window) >= 2:
                a0 = window[0].get("total_amount") if isinstance(window[0], dict) else None
                a1 = window[1].get("total_amount") if isinstance(window[1], dict) else None
                if isinstance(a0, (int, float)) and isinstance(a1, (int, float)):
                    delta = float(a0) - float(a1)

            top20_ratio: Optional[float] = None
            fr_sync = factors.get("etf_index_sync_daily")
            if isinstance(fr_sync, FactorResult) and isinstance(fr_sync.details, dict):
                v = fr_sync.details.get("top20_amount_ratio")
                if isinstance(v, (int, float)):
                    top20_ratio = float(v)

            amount_slot: Dict[str, Any] = {}
            if isinstance(amount_total, float):
                amount_slot = {
                    # MarketOverviewBlock expects src["amount"] to be dict, with a nested "amount" dict.
                    "amount": {"amount": amount_total, "delta": delta, "unit": "亿元"},
                }
                if isinstance(top20_ratio, float):
                    amount_slot["top20_amount_ratio"] = round(top20_ratio, 4)

            # 3) breadth (涨跌/扩散)
            breadth_raw = snapshot.get("market_sentiment_raw")
            breadth: Dict[str, Any] = breadth_raw if isinstance(breadth_raw, dict) else {}

            if not breadth:
                # fallback: adv_ratio from etf_index_sync_daily
                adv_ratio: Optional[float] = None
                if isinstance(fr_sync, FactorResult) and isinstance(fr_sync.details, dict):
                    v = fr_sync.details.get("adv_ratio")
                    if isinstance(v, (int, float)):
                        adv_ratio = float(v)
                if isinstance(adv_ratio, float):
                    breadth = {"adv_ratio": round(adv_ratio, 4)}

            out: Dict[str, Any] = {}
            if indices:
                out["indices"] = indices
            if amount_slot:
                out["amount"] = amount_slot
            if breadth:
                out["breadth"] = breadth

            return out
        except Exception as e:
            LOG.error("[AShareDailyEngine] market_overview slot build failed: %s", e, exc_info=True)
            return {}


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
        # -----------------------------------------------------------
        # Expose FactorResults to report blocks (read-only)
        # - SectorProxyBlock expects slots["factors"]["sector_proxy"]
        # - Keep best-effort compatibility: if bound carries sector_proxy, also attach it.
        # -----------------------------------------------------------
        factors_slot: Dict[str, Any] = {}
        if isinstance(getattr(self, "factors", None), dict):
            for _k, _fr in self.factors.items():
                if _fr is None:
                    continue
                try:
                    factors_slot[_k] = _wrap_factor(_fr)
                except Exception:
                    # fallback: keep raw object
                    factors_slot[_k] = _fr
        
        # If binder/output placed sector_proxy on bound but it is not present in self.factors
        if "sector_proxy" not in factors_slot:
            _sp = bound.get("sector_proxy")
            if _sp is not None:
                try:
                    factors_slot["sector_proxy"] = _wrap_factor(_sp)  # type: ignore[arg-type]
                except Exception:
                    factors_slot["sector_proxy"] = _sp
        
        slots["factors"] = factors_slot


    
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
    

        # --- report blocks config (single source of truth: YAML) ---
        weights_cfg = self._load_weights_cfg()
        report_blocks_path = None
        if isinstance(weights_cfg, dict):
            v = weights_cfg.get("report_blocks_path")
            if isinstance(v, str) and v.strip():
                report_blocks_path = v.strip()
        if not report_blocks_path:
            report_blocks_path = "config/report_blocks.yaml"

        # expose path into slots for audit/debug
        gov = slots.get("governance")
        if not isinstance(gov, dict):
            gov = {}
            slots["governance"] = gov
        cfg = gov.get("config")
        if not isinstance(cfg, dict):
            cfg = {}
            gov["config"] = cfg
        cfg["report_blocks_path"] = report_blocks_path

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
            block_specs_path=report_blocks_path,
        )
    
        doc = engine.build_report(context)

        path = os.path.join(r"run\reports", f"doc_{self.trade_date}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(des_payload, f, ensure_ascii=False, indent=2)

        text = MarkdownRenderer().render(doc)
        ReportWriter().write(doc, text)
        return text, des_payload
        
