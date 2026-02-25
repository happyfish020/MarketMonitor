#-*- coding: utf-8 -*-
"""UnifiedRisk V12 - Rotation Market Facts DataSource (Frozen)

职责（冻结）：
- 只读 Oracle 事实层轮动信号表（Market Facts Overlay）：
    - SECOPR.CN_SECTOR_ROTATION_MKT_SIGNAL_T
- 不做业务判断/计算，不改写任何策略/回测相关表
- 输出给 Fetcher -> snapshot["rotation_market_signal_raw"] -> slots["rotation_market_signal"]

注意：
- 该事实层信号与策略信号（CN_SECTOR_ROTATION_SIGNAL_T）必须严格隔离。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from datetime import date, datetime
from decimal import Decimal

from core.utils.logger import get_logger
from core.datasources.datasource_base import DataSourceBase, DataSourceConfig
from core.adapters.providers.db_provider_router import get_db_provider

LOG = get_logger("DS.RotationMarketSignal")


class RotationMarketSignalDataSource(DataSourceBase):
    """Rotation Market Facts (DB -> raw block)."""

    def __init__(self, config: DataSourceConfig):
        super().__init__(config)
        self.config = config
        self.market = config.market
        self.ds_name = config.ds_name

        LOG.info(
            "[DS.RotationMarketSignal] Init ok. market=%s ds=%s",
            self.market,
            self.ds_name,
        )

    def build_block(self, trade_date: str, refresh_mode: str = "none") -> Dict[str, Any]:
        """Return market rotation facts raw block.

        refresh_mode is kept for interface consistency; this DS is DB-only read.
        """
        LOG.info("[DS.RotationMarketSignal] build_block trade_date=%s mode=%s", trade_date, refresh_mode)

        run_id = self._resolve_run_id_default()
        rows = self._fetch(run_id=run_id, trade_date=trade_date)

        # DB provider might return keys in different cases; normalize reads.
        def _get_action(x: Dict[str, Any]) -> str:
            v = x.get("action")
            if v is None:
                v = x.get("ACTION")
            return str(v or "").upper()

        enter = [r for r in rows if _get_action(r) == "ENTER"]
        watch = [r for r in rows if _get_action(r) == "WATCH"]
        exit_ = [r for r in rows if _get_action(r) == "EXIT"]

        # candidates: prefer ENTER then WATCH; sort by SIGNAL_SCORE desc (best-effort)
        def _score(x: Dict[str, Any]) -> float:
            v = x.get("signal_score")
            if v is None:
                v = x.get("SIGNAL_SCORE")
            try:
                return float(v) if v is not None else -1e18
            except Exception:
                return -1e18

        candidates = sorted(enter, key=_score, reverse=True)
        if len(candidates) < 3:
            candidates += sorted(watch, key=_score, reverse=True)
        candidates = candidates[:3]

        return {
            "meta": {
                "trade_date": str(trade_date),
                "run_id": str(run_id) if run_id is not None else None,
                "data_status": "OK" if rows else "EMPTY",
            },
            "enter": enter,
            "watch": watch,
            "exit": exit_,
            "candidates": candidates,
        }

    # ---------------------------------------------------------
    def _resolve_run_id_default(self) -> Optional[str]:
        """Resolve baseline run_id from CN_BASELINE_REGISTRY_T (DEFAULT_BASELINE)."""
        db = get_db_provider()
        sql = """
        SELECT run_id
          FROM CN_BASELINE_REGISTRY_T
         WHERE baseline_key = 'DEFAULT_BASELINE'
           AND is_active = 1
         LIMIT 1
        """
        try:
            rows = db.execute(sql)
            if rows:
                r0 = rows[0]
                try:
                    m = getattr(r0, "_mapping", None)
                    if m and "RUN_ID" in m:
                        return str(m["RUN_ID"])
                except Exception:
                    pass
                return str(r0[0])
        except Exception as e:
            LOG.warning("[DS.RotationMarketSignal] resolve_run_id_default failed: %s", e)

        return "SR_BASE_V535_EP90_XP55_XC2_MH5_RF5_K2_COST5BPS"

    def _fetch(self, *, run_id: str, trade_date: str) -> List[Dict[str, Any]]:
        db = get_db_provider()
        sql = """
        SELECT
            RUN_ID,
            SIGNAL_DATE,
            TRADE_DATE,
            SECTOR_TYPE,
            SECTOR_ID,
            SECTOR_NAME,
            ACTION,
            SIGNAL_SCORE,
            STRENGTH_TAG,
            REASONS_JSON,
            CREATED_AT,
            ENTER_DAYS_5D,
            TOP3_DAYS_5D,
            SCORE_MA3
        FROM CN_SECTOR_ROTATION_MKT_PERSIST_V
        WHERE RUN_ID = :run_id
          AND SIGNAL_DATE >= :trade_date
          AND SIGNAL_DATE <  DATE_ADD(:trade_date, INTERVAL 1 DAY)
        ORDER BY
            CASE WHEN UPPER(ACTION)='ENTER' THEN 0 WHEN UPPER(ACTION)='WATCH' THEN 1 ELSE 2 END,
            (SIGNAL_SCORE IS NULL) ASC,
            SIGNAL_SCORE DESC
        """
        rows = db.execute(sql, {"run_id": run_id, "trade_date": trade_date})
        return self._rows_to_dicts(rows)

    # ---------------------------------------------------------
    @staticmethod
    def _jsonable(v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, (datetime, date)):
            try:
                return v.isoformat()
            except Exception:
                return str(v)
        if isinstance(v, Decimal):
            try:
                return float(v)
            except Exception:
                return str(v)
        return v

    def _rows_to_dicts(self, rows: Any) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        if not rows:
            return out
        for r in rows:
            m = getattr(r, "_mapping", None)
            if m:
                out.append({k: self._jsonable(v) for k, v in m.items()})
                continue
            try:
                out.append({str(i): self._jsonable(v) for i, v in enumerate(r)})
            except Exception:
                out.append({"_raw": str(r)})
        return out
