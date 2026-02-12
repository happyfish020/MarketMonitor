# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime, timedelta, time
import importlib
import inspect
from typing import Any, Dict, List, Optional, Tuple

import baostock as bs
import pandas as pd
import pytz
import os, json 
import sqlite3

# --- UnifiedRisk persistence helpers (SRP/OOP-friendly) ---
from core.persistence.sqlite.sqlite_connection import connect_sqlite as ur_connect_sqlite
from core.persistence.sqlite.sqlite_schema_l1 import ensure_schema_l1
from core.persistence.sqlite.sqlite_schema import ensure_schema_l2


def _ur_open_persist_conn(db_path):
    """Open SQLite connection for UnifiedRisk persistence and ensure schemas exist."""
    conn = ur_connect_sqlite(str(db_path))
    ensure_schema_l1(conn)
    ensure_schema_l2(conn)
    return conn


def _ur_purge_l2_for_rerun(conn, trade_date: str, report_kind: str):
    """Best-effort purge L2 artifacts for same (trade_date, report_kind) to allow reruns."""
    try:
        with conn:
            conn.execute(
                "DELETE FROM ur_report_des_link WHERE trade_date=? AND report_kind=?",
                (trade_date, report_kind),
            )
            conn.execute(
                "DELETE FROM ur_decision_evidence_snapshot WHERE trade_date=? AND report_kind=?",
                (trade_date, report_kind),
            )
            conn.execute(
                "DELETE FROM ur_report_artifact WHERE trade_date=? AND report_kind=?",
                (trade_date, report_kind),
            )
    except Exception:
        # Best-effort only.
        pass


def _ur_publish_l2(publisher, trade_date: str, report_kind: str, report_text: str, des_payload: dict, engine_version: str, run_id: str):
    """Publish report + decision evidence into L2 tables."""
    publisher.publish(
        trade_date=trade_date,
        report_kind=report_kind,
        report_text=report_text,
        des_payload=des_payload,
        engine_version=engine_version,
        meta={"run_id": run_id},
    )
from core.persistence.contracts.errors import PersistenceError
from core.persistence.sqlite.sqlite_l2_publisher import SqliteL2Publisher
from core.persistence.regime_shift_auditor import RegimeShiftAuditor
from core.services.regime_history_service import RegimeHistoryService
from core.utils.data_freshness import compute_data_freshness, inject_asof_fields
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
#from core.factors.cn.etf_index_sync_factor import CrowdingConcentrationFactor
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
from core.regime.observation.rew.rew_observation import REWObservation
from core.governance.execution_summary_builder import ExecutionSummaryBuilder
from core.governance.rotation_switch_builder import build_rotation_switch

# ===== Report =====
from core.reporters.report_context import ReportContext
from core.reporters.report_engine import ReportEngine
from core.reporters.renderers.markdown_renderer import MarkdownRenderer
from core.reporters.report_writer import ReportWriter
from core.actions.actionhint_service import ActionHintService
from core.rules.attack_window_rule import AttackWindowEvaluator

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
    UnifiedRisk V12 · CN A-Share Daily Engine（严�?OOP 封装版）

    设计铁律�?
    - 制度信号只在 Engine 内生�?
    - Builder / Block / Case 不允许推断制�?
    """

    def __init__(self, refresh_mode: str = "none") -> None:
        self.refresh_mode = refresh_mode
        self.snapshot = None
        self.factors = None
        self.gate = None

        # === 核心制度状态（只在 Engine 内部存在�?==
        self._distribution_risk_active: bool = False
        
        self._resolve_trade_time()
        #test 
        #self.trade_date="2026-02-11"
        self.is_intraday = False
         
        self.report_kind ="EOD"



    def _resolve_trade_time(self) -> tuple[bool, str]:
        """
        判断当前是否为交易日盘中时间，并返回最近一个交易日
    
        Returns:
            (is_intraday: bool, last_trade_date: str)
                - is_intraday: True 表示当前是交易日且在 9:30~15:00 之间
                - last_trade_date: 最近的交易日（'YYYY-MM-DD'�?
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
            # 3. 查询最�?0天的交易日历（足够覆盖节假日�?
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
    
            # 提取所有交易日并降序排�?
            trading_days = trade_df[trade_df['is_trading_day'] == 1]['calendar_date'].dt.date.values
    
            if len(trading_days) == 0:
                raise Exception("No trading days found in the past 60 days")
    
            # 4. 找到最近的交易日（�?reference_date 往前找�?
            last_trade_date_obj = max(d for d in trading_days if d <= reference_date)
            last_trade_date_str = last_trade_date_obj.strftime('%Y-%m-%d')
    
            # 5. 判断是否为盘中时�?
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
    # Core Pipeline（唯一入口�?
    # ==================================================
    def _execute_pipeline(self) -> None:
        # weights.yaml is the single source of truth for pipeline config

        run_persist, engine_version = self._init_persistence()
        run_id = None
        stage = "START"

        try:

            # 1) start_run 先落�?
            run_id = run_persist.start_run(
                trade_date=self.trade_date,
                report_kind=self.report_kind,
                engine_version=engine_version,
            )
          
            self.weights_cfg = self._load_weights_cfg()
            fp = self.weights_cfg.get("factor_pipeline", {}) if isinstance(self.weights_cfg, dict) else {}
            structure_keys = fp.get("structure_factors", []) if isinstance(fp, dict) else []
            if not isinstance(structure_keys, list):
                structure_keys = []
    
              
            
            self.snapshot = self._fetch_snapshot()
            
            
            self.factors = self._compute_factors(self.snapshot)
    
            # policy slot binder 已弃用：Phase-2/Report 统一�?YAML 读取结构与语义；此处不再做额外绑�?
            factors_bound: Dict[str, Any] = {}
            factors_bound = self._build_phase2( factors_bound)
    
            # ===== Phase-3 / 制度状态（封装�?====
            self._distribution_risk_active = self._compute_distribution_risk(
                structure=factors_bound["structure"],
                
            )
    
            # ===== Structure（消费制度状态）=====
            factors_bound["structure"] = StructureFactsBuilder(spec=self._load_structure_facts_cfg()).build(
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
            
            with open(r"c:\temp\des_payload.json", "w", encoding="utf-8") as f:
                json.dump(des_payload, f, ensure_ascii=False, indent=2)


            self.presiste_data(report_text=report_text, des_payload= des_payload)
        except Exception as e:
        # 失败态也必须落库
            if run_id:
                run_persist.finish_run(
                    run_id,
                    status="FAILED",
                    error_type=type(e).__name__,
                    error_message=f"{stage} :: {str(e)}"[:1000],
                )
            raise
        finally:
            try:
                if getattr(self, "_conn", None):
                    self._conn.close()
            except Exception:
                pass 
    
    def presiste_data(self, report_text: str, des_payload:dict):

        from core.persistence.sqlite.sqlite_run_persistence import SqliteRunPersistence
        from pathlib import Path
        UNIFIEDRISK_DB_PATH = r"./data/persistent/unifiedrisk.db"
        db_path = Path(UNIFIEDRISK_DB_PATH)
        #if db_path.exists():
        #   db_path.unlink()

        self._conn = _ur_open_persist_conn(db_path)

        run_persist= SqliteRunPersistence(self._conn)
        publisher = SqliteL2Publisher(self._conn)
        _ur_purge_l2_for_rerun(self._conn, self.trade_date, self.report_kind)
        
        now = datetime.now()
        engine_version = "V_"+ now.strftime("%Y-%m-%d_%H:%M:%S")
         
        format_string = "%Y-%m-%d"
        try:
            run_id = run_persist.start_run(
                    trade_date=self.trade_date,
                    report_kind = self.report_kind,
                    engine_version=engine_version,
                )
            
            run_persist.record_snapshot(run_id, "internal_snapshot", self.snapshot)
            
            gate = des_payload["governance"]["gate"]
            drs = des_payload["governance"]["drs"]
            frf = des_payload["governance"]["frf"]
    
            run_persist.record_gate(run_id = run_id,
                                    gate=gate, 
                                    drs=drs,
                                    frf = frf, 
                                    action_hint=None, 
                                    rule_hits=None
                                    )
    
            for factor_name, fr in self.factors.items():
                run_persist.record_factor(
                    run_id,
                    factor_name,
                    fr.to_dict(),
                    factor_version="" # factor_result.get("version"),
                )
      
    
            _ur_publish_l2(
                publisher,
                trade_date=self.trade_date,
                report_kind=self.report_kind,
                report_text=report_text,
                des_payload=des_payload,
                engine_version=engine_version,
                run_id=run_id,
                )
            run_persist.finish_run(run_id, status="COMPLETED")
            #run_persist.finish_run(run_id, status="COMPLETED")     
            #run_persist.record_factor(run_id, "breadth", breadth_result)
            #run_persist.record_factor(run_id, "new_top50_lows", ntl_result)
        except Exception as e:
            run_persist.finish_run(
                run_id,
                status="FAILED",
                error_type=type(e).__name__,
                error_message=str(e)[:1000],
            )
            raise
    # end def 

 
        
        
    


    ###################################################### 
        ################# 对账 �?
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
    # Institution Logic（不外溢�?
    # ==================================================
    def _compute_distribution_risk(
        self,
        *,
        structure: Dict[str, Any],
        
    ) -> bool:
        """
        冻结制度定义�?
        - 连续结构恶化（由 continuity 模块维护�?
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
            "etf_index_sync": "core.factors.cn.etf_index_sync_factor:CrowdingConcentrationFactor",
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
            # coverage 缺失属于配置错误：明确失�?
            raise
        return bound

    def _build_phase2(self,  bound):
        cfg = self.weights_cfg if isinstance(getattr(self, "weights_cfg", None), dict) else {}
        fp = cfg.get("factor_pipeline", {}) if isinstance(cfg, dict) else {}
        structure_keys = fp.get("structure_factors", []) if isinstance(fp, dict) else []
        if not isinstance(structure_keys, list):
            structure_keys = []
        structure = StructureFactsBuilder(spec=self._load_structure_facts_cfg()).build(factors=self.factors or {}, structure_keys=structure_keys)
        bound["structure"] = structure
        #bound["watchlist"] = WatchlistStateBuilder().build(
        #    factors=self.factors, structure=structure, watchlist_config=bound.get("watchlist")
        #)
        drs_obs = DRSObservation().build(inputs=structure, asof=self.trade_date)
        drs = DRSContinuity.apply(
            drs_obs=drs_obs,
            asof=self.trade_date,
            fallback_state_path="state/drs_persistence.json",
        )

        # REW: Regime Early Warning (observation-only)
        # - best-effort, never throws
        # - MUST NOT affect Gate/DRS/scoring
        rew = REWObservation().build(
            inputs={"structure": structure, "factors": self.factors or {}},
            asof=self.trade_date,
        )

        bound["observations"] = {
            "drs": drs,
            "rew": rew,
            # alias for compatibility
            "regime_early_warning": rew,
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
        # --- governance extract (你已�? ---
        drs = self._extract_drs_signal(bound)
    
        frf_obj = bound.get("failure_rate")
        frf = getattr(frf_obj, "level", "") if frf_obj is not None else ""

        # Regime Early Warning (REW) summary (observation-only)
        rew_summary: Dict[str, Any] = {}
        try:
            _obs = bound.get("observations", {}) if isinstance(bound.get("observations"), dict) else {}
            rew_node = _obs.get("rew") or _obs.get("regime_early_warning")
            if isinstance(rew_node, dict):
                ob = rew_node.get("observation") if isinstance(rew_node.get("observation"), dict) else {}
                lvl = ob.get("level")
                scp = ob.get("scope")
                rs = ob.get("reasons") if isinstance(ob.get("reasons"), list) else []
                if isinstance(lvl, str) and lvl.strip():
                    rew_summary = {
                        "level": lvl.strip().upper(),
                        "scope": (scp or "LOCAL"),
                        "reasons": rs,
                    }
                    if isinstance(rew_node.get("warnings"), list):
                        rew_summary["warnings"] = rew_node.get("warnings")
                    if isinstance(rew_node.get("evidence"), dict):
                        rew_summary["evidence"] = rew_node.get("evidence")
        except Exception:
            rew_summary = {}
    
        # --- slots（事实层�?--
        slots: Dict[str, Any] = {
            #"gate": self.gate.level,
            # V12 preferred
            "governance": {
                "gate": {
                    "raw_gate": self.gate.level,
                    "final_gate": self.gate.level,
                    "reasons": getattr(self.gate, "reasons", []) if hasattr(self.gate, "reasons") else [],
                    "hits": [
                        {
                            "rule_id": f"AUTO:{r}",
                            "title": str(r),
                            "severity": "INFO",
                        }
                        for r in (getattr(self.gate, "reasons", []) if hasattr(self.gate, "reasons") else [])
                    ],
                    "gate_rules_path": "config/gate_rules.yaml",
                },
                "drs": {"signal": drs},
                "rew": rew_summary,
            },

            "drs": {"signal": drs},
            "rew": rew_summary,
            "structure": bound.get("structure") or {},
            "observations": bound.get("observations") or {},
            "execution_summary": bound.get("execution_summary"),
            # �?market.overview block 使用（存在什么就透传什么，不在此处“编造事实”）
            "market_overview": (
                bound.get("market_overview")
                or bound.get("market_close_facts")
                or bound.get("close_facts")
                or {}
            ),
            # �?etf_spot_sync.explain block 使用（允�?intraday_overlay 作为备用�?
            "intraday_overlay": bound.get("intraday_overlay") or {},
        }

        # -----------------------------------------------------------
        # Rotation Snapshot (Sector Rotation)
        # - Source: Fetcher snapshot["rotation_snapshot_raw"]
        # - Frozen: report layer must only read DB snapshot tables (already done in DS)
        # -----------------------------------------------------------
        try:
            _rs = None
            if isinstance(getattr(self, "snapshot", None), dict):
                _rs = self.snapshot.get("rotation_snapshot_raw")
            if isinstance(_rs, dict) and _rs:
                slots["rotation_snapshot"] = _rs
        except Exception:
            # fail-open: rotation snapshot is an additive report block
            pass
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

        # -----------------------------------------------------------
        # -----------------------------------------------------------
        # Attack Window (Offense Permission Layer) - canonical input slots
        #
        # Problem fixed (2026-02-05 audit):
        # - Do NOT assume factors named breadth/trend_in_force/failure_rate exist.
        # - Prefer structure facts + watchlist_lead lead_panels when available.
        # - Emit canonical top-level slots expected by AttackWindowEvaluator:
        #   breadth / trend_in_force / failure_rate / north_proxy_pressure / leverage_constraints / options_risk
        # - Each carries meta.asof for Data Freshness checks.
        #
        # Read-only: does NOT affect Gate/DRS/Execution.
        # -----------------------------------------------------------
        def _details_from_factor(name: str) -> Dict[str, Any]:
            frw = factors_slot.get(name)
            if isinstance(frw, dict):
                det = frw.get("details")
                if isinstance(det, dict) and det:
                    return det
                # fallback: pack level/score if details missing
                return {"level": frw.get("level"), "score": frw.get("score"), "meta": {"asof": self.trade_date}}
            return {"meta": {"asof": self.trade_date}}

        # Prefer explicit watchlist_lead slot; fallback to factor.details if present.
        if "watchlist_lead" not in slots:
            _wl = _details_from_factor("watchlist_lead")
            if isinstance(_wl, dict) and _wl:
                slots["watchlist_lead"] = _wl

        def _pick_structure_fact(key: str) -> Dict[str, Any]:
            sf = slots.get("structure") if isinstance(slots.get("structure"), dict) else {}
            obj = sf.get(key)
            return obj if isinstance(obj, dict) else {}

        def _pick_lead_panel(panel_key: str) -> Dict[str, Any]:
            wl = slots.get("watchlist_lead") if isinstance(slots.get("watchlist_lead"), dict) else {}
            lead_panels = wl.get("lead_panels") if isinstance(wl.get("lead_panels"), dict) else {}
            p = lead_panels.get(panel_key)
            return p if isinstance(p, dict) else {}

        # trend_in_force: prefer structure facts (stable schema: {"state": ...})
        if "trend_in_force" not in slots or not isinstance(slots.get("trend_in_force"), dict) or not slots.get("trend_in_force"):
            _tif = _pick_structure_fact("trend_in_force")
            if _tif:
                # normalize minimal contract
                slots["trend_in_force"] = {
                    "state": _tif.get("state") or _tif.get("status") or _tif.get("状态") or _tif.get("trend_state"),
                    "meta": {"asof": self.trade_date},
                    **{k: v for k, v in _tif.items() if k not in ("meta",)},
                }
            else:
                slots["trend_in_force"] = _details_from_factor("trend_in_force")

        # failure_rate: prefer structure facts; keep original state string for evaluator mapping
        if "failure_rate" not in slots or not isinstance(slots.get("failure_rate"), dict) or not slots.get("failure_rate"):
            _fr = _pick_structure_fact("failure_rate") or _pick_structure_fact("frf") or _pick_structure_fact("failure_rate_facts")
            if _fr:
                slots["failure_rate"] = {
                    "state": _fr.get("state") or _fr.get("status") or _fr.get("状态"),
                    "level": _fr.get("level"),  # if exists
                    "improve_days": _fr.get("improve_days") or _fr.get("improvement_days"),
                    "meta": {"asof": self.trade_date},
                    **{k: v for k, v in _fr.items() if k not in ("meta",)},
                }
            else:
                slots["failure_rate"] = _details_from_factor("failure_rate")

        # north_proxy_pressure: prefer structure facts (usually carries pressure_score/level)
        if "north_proxy_pressure" not in slots or not isinstance(slots.get("north_proxy_pressure"), dict) or not slots.get("north_proxy_pressure"):
            _np = _pick_structure_fact("north_proxy_pressure") or _pick_structure_fact("north_proxy")
            if _np:
                slots["north_proxy_pressure"] = {
                    "level": _np.get("level") or _np.get("pressure_level") or _np.get("状态"),
                    "score": _np.get("score") or _np.get("pressure_score"),
                    "meta": {"asof": self.trade_date},
                    **{k: v for k, v in _np.items() if k not in ("meta",)},
                }
            else:
                slots["north_proxy_pressure"] = _details_from_factor("north_proxy_pressure")

        # breadth: prefer watchlist_lead breadth_plus panel key_metrics; fallback to factor
        if "breadth" not in slots or not isinstance(slots.get("breadth"), dict) or not slots.get("breadth"):
            bp = _pick_lead_panel("breadth_plus")
            km = bp.get("key_metrics") if isinstance(bp.get("key_metrics"), dict) else {}
            slots["breadth"] = {
                "adv_ratio": None,  # filled below if available
                "pct_above_ma20": km.get("pct_above_ma20"),
                "pct_above_ma50": km.get("pct_above_ma50"),
                "new_low_ratio_pct": km.get("new_low_ratio_pct"),
                "new_high_low_ratio": km.get("new_high_low_ratio"),
                "ad_slope_10d": km.get("ad_slope_10d"),
                "meta": {"asof": self.trade_date},
            }

        # Fill adv_ratio from watchlist_lead market_sentiment panel if available
        ms = _pick_lead_panel("market_sentiment")
        ms_km = ms.get("key_metrics") if isinstance(ms.get("key_metrics"), dict) else {}
        if isinstance(slots.get("breadth"), dict):
            if slots["breadth"].get("adv_ratio") is None:
                adv_pct = ms_km.get("adv_ratio_pct")
                if isinstance(adv_pct, (int, float)):
                    slots["breadth"]["adv_ratio"] = float(adv_pct) / 100.0

        # leverage_constraints/options_risk: prefer watchlist_lead panels levels (G/E) for governance hints
        if "leverage_constraints" not in slots or not isinstance(slots.get("leverage_constraints"), dict) or not slots.get("leverage_constraints"):
            mi = _pick_lead_panel("margin_intensity")
            slots["leverage_constraints"] = {"level": mi.get("level"), "meta": {"asof": self.trade_date}}

        if "options_risk" not in slots or not isinstance(slots.get("options_risk"), dict) or not slots.get("options_risk"):
            op = _pick_lead_panel("options_risk")
            slots["options_risk"] = {"level": op.get("level"), "meta": {"asof": self.trade_date}}
        # NOTE (Frozen): do NOT write back partial fields into slots['market_overview'].
        # MarketOverviewBlock relies on empty/nonexistent market_overview to trigger its fallback assembly.
        # Attack Window reads required evidence directly from structure facts / lead panels.
# Provide a canonical gate_decision slot for rule layers expecting it.
        if "gate_decision" not in slots:
            slots["gate_decision"] = {"gate": getattr(self.gate, "level", None), "meta": {"asof": self.trade_date}}



    
        # �?etf_index_sync / etf_spot_sync factor 统一挂到 slots["etf_spot_sync"]
        #（你�?explain block 优先�?slots["etf_spot_sync"]�?
        for k in ("etf_spot_sync", "etf_index_sync", "crowding_concentration", "etf_spot_sync_raw"):
            fr = self.factors.get(k) if isinstance(getattr(self, "factors", None), dict) else None
            if fr is not None:
                slots["etf_spot_sync"] = _wrap_factor(fr)  # 内含 details
                break
    
        
        # -----------------------------------------------------------
        # Data Freshness (asof vs trade_date)
        # - Collect any embedded 'asof' fields across slots and render a compact summary.
        # - Enforcement is handled elsewhere; here we expose facts for report/audit.
        # -----------------------------------------------------------
        try:
            slots["data_freshness_inject"] = inject_asof_fields(trade_date=self.trade_date, slots=slots)
            slots["data_freshness"] = compute_data_freshness(trade_date=self.trade_date, slots=slots)
        except Exception as _e:
            slots["data_freshness"] = {
                "meta": {"trade_date": self.trade_date, "stale_count": None},
                "stale": [],
                "hard_required_missing_paths": [],
                "render_lines": [f"> ⚠️ exception:data_freshness_compute: {_e}"],
            }

        # -----------------------------------------------------------
        # Rotation Switch (板块轮动开关)
        # - Read-only governance helper: ON / OFF / PARTIAL + reasons
        # - Uses config/rotation_enable.yaml as single source of truth.
        # -----------------------------------------------------------
        try:
            rot_cfg_all = self._load_yaml_file("config/rotation_enable.yaml")
            rot_cfg = rot_cfg_all.get("rotation_enable") if isinstance(rot_cfg_all, dict) else {}
            if not isinstance(rot_cfg, dict):
                rot_cfg = {}
            slots["rotation_switch"] = build_rotation_switch(slots=slots, cfg=rot_cfg, trade_date=self.trade_date)
        except Exception as _e:
            # Never fail report because of this helper block
            slots["rotation_switch"] = {
                "asof": self.trade_date,
                "mode": "OFF",
                "confidence": 0.0,
                "verdict": "不适合板块轮动（异常降级）",
                "reasons": [{"code": "ROT_EXCEPTION", "level": "WARN", "msg": f"exception:{_e}"}],
                "data_status": {"coverage": "PARTIAL", "missing": []},
            }
        

        # -----------------------------------------------------------
        # Attack Window (Offense Permission Layer) - evaluate and attach
        # - Must run after gate_decision is available, before report build.
        # - Fail-closed: on any exception, state=OFF, permission=FORBID.
        # -----------------------------------------------------------
        try:
            if not hasattr(self, "_attack_window_evaluator") or self._attack_window_evaluator is None:
                self._attack_window_evaluator = AttackWindowEvaluator("config/rules/attack_window.yaml")
            slots["attack_window"] = self._attack_window_evaluator.evaluate(slots)
            # ---- FINAL: normalize AttackWindow asof for reporter compatibility ----
            try:
                _aw = slots.get("attack_window")
                if isinstance(_aw, dict):
                    _aw.setdefault("asof", self.trade_date)
                    _aw.setdefault("trade_date", self.trade_date)
                    _m = _aw.get("meta")
                    if not isinstance(_m, dict):
                        _m = {}
                    _m.setdefault("asof", self.trade_date)
                    _m.setdefault("trade_date", self.trade_date)
                    _aw["meta"] = _m
            except Exception:
                pass

        except Exception as _e:
            slots["attack_window"] = {
                "asof": self.trade_date,
                "trade_date": self.trade_date,
                "meta": {"asof": self.trade_date, "schema": "AW_V1"},
                "state": "OFF",
                "gate": getattr(self.gate, "level", "UNKNOWN"),
                "offense_permission": "FORBID",
                "reasons_yes": [],
                "reasons_no": [f"exception:attack_window:{_e}"],
                "evidence": {},
                "data_freshness": {"asof_ok": False, "notes": ["exception"]},
            }
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
                "rew": rew_summary,
            },
            "rule_trace": "",  # init
        }
        # --- Attack Window snapshot (read-only) ---
        des_payload["attack_window"] = slots.get("attack_window")
        # -----------------------------------------------------------
        # Attack Window Case Log (制度案例日志) - fail-safe append-only
        # - Always writes a DAY snapshot (even OFF) so activation is visible.
        # - Writes a CASE snapshot when state in {VERIFY_ONLY, LIGHT_ON, ON}.
        # - Does NOT affect any decision logic.
        # -----------------------------------------------------------
        try:
            from core.audit.attack_window_case_logger import maybe_log_attack_window_case

            maybe_log_attack_window_case(
                trade_date=self.trade_date,
                slots=slots,
                report_kind=getattr(self, "report_kind", "EOD"),
                des_payload=des_payload,
                logger=LOG,
            )
        except Exception as _e:
            # Never break daily run because of audit log.
            LOG.warning("attack_window case log failed: %s", _e)



    

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
            block_specs_path=report_blocks_path,
        )
    
        RegimeHistoryService.inject(context, conn=self._conn, report_kind=self.report_kind, n=10)
    
        doc = engine.build_report(context)

        path = os.path.join(r"run\reports", f"doc_{self.trade_date}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(des_payload, f, ensure_ascii=False, indent=2)

        text = MarkdownRenderer().render(doc)
        ReportWriter().write(doc, text)
        return text, des_payload
        

    def _load_structure_facts_cfg(self) -> Dict[str, Any]:
        """Load structure facts semantic configuration (YAML).

        Default path: config/structure_facts.yaml
        Override via weights.yaml key: structure_facts_path
        """
        weights_cfg = self._load_weights_cfg()
        path = None
        if isinstance(weights_cfg, dict):
            path = weights_cfg.get("structure_facts_path")
        if not path:
            path = "config/structure_facts.yaml"
        cfg = self._load_yaml_file(path)
        return cfg if isinstance(cfg, dict) else {}



    def finish_run(
        self,
        run_id: str,
        status: str,
        error_type: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
        try:
            now = datetime.utcnow().isoformat(timespec="seconds")
    
            cols = {
                row[1] for row in self._conn.execute("PRAGMA table_info(ur_run_meta)").fetchall()
            }
    
            sets = ["status = ?"]
            vals = [status]
    
            if "error_type" in cols:
                sets.append("error_type = ?")
                vals.append(error_type)
            if "error_message" in cols:
                sets.append("error_message = ?")
                vals.append(error_message)
    
            # 支持 finished_at / finished_at_utc 二选一（或两者都有）
            if "finished_at_utc" in cols:
                sets.append("finished_at_utc = ?")
                vals.append(now)
            elif "finished_at" in cols:
                sets.append("finished_at = ?")
                vals.append(now)
    
            sql = f"UPDATE ur_run_meta SET {', '.join(sets)} WHERE run_id = ?"
            vals.append(run_id)
    
            with self._conn:
                self._conn.execute(sql, tuple(vals))
        except Exception as e:
            raise e
            #raise PersistenceError("Failed to finish_run ") from e

    def _init_persistence(self):
        from core.persistence.sqlite.sqlite_run_persistence import SqliteRunPersistence
        from pathlib import Path
    
        UNIFIEDRISK_DB_PATH = r"./data/persistent/unifiedrisk.db"
        db_path = Path(UNIFIEDRISK_DB_PATH)
        self._conn = connect_sqlite(str(db_path))
    
        run_persist = SqliteRunPersistence(self._conn)
    
        now = datetime.now()
        engine_version = "V_" + now.strftime("%Y-%m-%d_%H:%M:%S")
        return run_persist, engine_version
    
##### end class
