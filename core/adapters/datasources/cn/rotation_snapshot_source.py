# -*- coding: utf-8 -*-
"""UnifiedRisk V12 - Rotation Snapshot DataSource (Frozen)

职责（冻结）：
- 只读 Oracle snapshot 表（报告唯一数据源）：
    - SECOPR.CN_ROTATION_ENTRY_SNAP_T
    - SECOPR.CN_ROTATION_HOLDING_SNAP_T
    - SECOPR.CN_ROTATION_EXIT_SNAP_T
- 不做业务判断/计算，不产生新信号
- 只做轻量整形：分离明细（SECTOR_ID!=-1）与 summary（SECTOR_ID=-1）

铁律提醒：报告层只读 snapshot，任何复杂计算一律禁止。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from datetime import date, datetime
from decimal import Decimal

from core.utils.logger import get_logger
from core.datasources.datasource_base import DataSourceBase, DataSourceConfig
from core.adapters.providers.db_provider_router import get_db_provider

LOG = get_logger("DS.RotationSnapshot")


class RotationSnapshotDataSource(DataSourceBase):
    """Rotation Snapshot DS (DB -> raw block)."""

    def __init__(self, config: DataSourceConfig):
        super().__init__(config)
        self.config = config
        self.market = config.market
        self.ds_name = config.ds_name

        LOG.info(
            "[DS.RotationSnapshot] Init ok. market=%s ds=%s",
            self.market,
            self.ds_name,
        )

    # ---------------------------------------------------------
    def build_block(self, trade_date: str, refresh_mode: str = "none") -> Dict[str, Any]:
        """Return rotation snapshot raw block.

        refresh_mode is kept for interface consistency; this DS is DB-only read.
        """
        LOG.info("[DS.RotationSnapshot] build_block trade_date=%s mode=%s", trade_date, refresh_mode)

        run_id = self._resolve_run_id_default()
        entry_rows = self._fetch_entry(run_id=run_id, trade_date=trade_date)
        holding_rows = self._fetch_holding(run_id=run_id, trade_date=trade_date)
        exit_rows = self._fetch_exit(run_id=run_id, trade_date=trade_date)

        entry_detail, entry_summary = self._split_detail_summary(entry_rows)
        holding_detail, holding_summary = self._split_detail_summary(holding_rows)
        exit_detail, exit_summary = self._split_detail_summary(exit_rows)

        # EntryAllowed 仅根据是否存在明细（SECTOR_ID!=-1）。
        entry_allowed = bool(entry_detail)

        return {
            "meta": {
                "trade_date": str(trade_date),
                "run_id": str(run_id) if run_id is not None else None,
                "data_status": "OK" if (entry_rows or holding_rows or exit_rows) else "EMPTY",
            },
            "entry": {
                "allowed": entry_allowed,
                "rows": entry_detail,
                "summary": entry_summary,
            },
            "holding": {
                "rows": holding_detail,
                "summary": holding_summary,
            },
            "exit": {
                "rows": exit_detail,
                "summary": exit_summary,
            },
        }

    # ---------------------------------------------------------
    def _resolve_run_id_default(self) -> Optional[str]:
        """Resolve baseline run_id from CN_BASELINE_REGISTRY_T (DEFAULT_BASELINE).

        Frozen behavior:
        - Prefer DEFAULT_BASELINE=1 (or truthy) row.
        - If no row found, return the frozen baseline id as a last-resort fallback.
        """
        db = get_db_provider()

        # Best-effort query; keep it simple and Oracle-friendly.
        sql = """
        SELECT run_id
          FROM SECOPR.CN_BASELINE_REGISTRY_T
         WHERE baseline_key = 'DEFAULT_BASELINE'
           AND is_active = 1
           AND ROWNUM = 1
        """
        try:
            rows = db.execute(sql)
            if rows:
                # SQLAlchemy Row -> mapping
                r0 = rows[0]
                try:
                    m = getattr(r0, "_mapping", None)
                    if m and "RUN_ID" in m:
                        return str(m["RUN_ID"])
                except Exception:
                    pass
                # tuple fallback
                return str(r0[0])
        except Exception as e:
            LOG.warning("[DS.RotationSnapshot] resolve_run_id_default failed: %s", e)

        # Hard fallback to frozen baseline (per user spec).
        return "SR_BASE_V535_EP90_XP55_XC2_MH5_RF5_K2_COST5BPS"

    # ---------------------------------------------------------
    def _fetch_entry(self, *, run_id: str, trade_date: str) -> List[Dict[str, Any]]:
        db = get_db_provider()
        sql = """
        SELECT
            RUN_ID,
            TRADE_DATE,
            SECTOR_TYPE,
            SECTOR_ID,
            SECTOR_NAME,
            ENTRY_RANK,
            ENTRY_CNT,
            WEIGHT_SUGGESTED,
            SIGNAL_SCORE,
            ENERGY_SCORE,
            ENERGY_PCT,
            ENERGY_TIER,
            STATE,
            TRANSITION,
            SOURCE_JSON,
            CREATED_AT
        FROM SECOPR.CN_ROTATION_ENTRY_SNAP_T
        WHERE RUN_ID = :run_id
          AND TRADE_DATE = TO_DATE(:trade_date, 'YYYY-MM-DD')
        ORDER BY ENTRY_RANK NULLS LAST
        """
        rows = db.execute(sql, {"run_id": run_id, "trade_date": trade_date})
        return self._rows_to_dicts(rows)

    def _fetch_holding(self, *, run_id: str, trade_date: str) -> List[Dict[str, Any]]:
        db = get_db_provider()
        sql = """
        SELECT
            RUN_ID,
            TRADE_DATE,
            SECTOR_TYPE,
            SECTOR_ID,
            SECTOR_NAME,
            ENTER_SIGNAL_DATE,
            EXEC_ENTER_DATE,
            HOLD_DAYS,
            MIN_HOLD_DAYS,
            EXIT_SIGNAL_TODAY,
            EXIT_TRANSITION,
            EXIT_EXEC_STATUS,
            NEXT_EXIT_ELIGIBLE_DATE,
            SOURCE_JSON,
            CREATED_AT
        FROM SECOPR.CN_ROTATION_HOLDING_SNAP_T
        WHERE RUN_ID = :run_id
          AND TRADE_DATE = TO_DATE(:trade_date, 'YYYY-MM-DD')
        ORDER BY SECTOR_NAME
        """
        rows = db.execute(sql, {"run_id": run_id, "trade_date": trade_date})
        return self._rows_to_dicts(rows)

    def _fetch_exit(self, *, run_id: str, trade_date: str) -> List[Dict[str, Any]]:
        db = get_db_provider()
        sql = """
        SELECT
            RUN_ID,
            TRADE_DATE,
            EXEC_EXIT_DATE,
            SECTOR_TYPE,
            SECTOR_ID,
            SECTOR_NAME,
            STATE,
            TRANSITION,
            ENTRY_RANK,
            SIGNAL_SCORE,
            ENTER_SIGNAL_DATE,
            EXEC_ENTER_DATE,
            HOLD_DAYS,
            MIN_HOLD_DAYS,
            EXIT_EXEC_STATUS,
            SOURCE_JSON,
            CREATED_AT
        FROM SECOPR.CN_ROTATION_EXIT_SNAP_T
        WHERE RUN_ID = :run_id
          AND TRADE_DATE = TO_DATE(:trade_date, 'YYYY-MM-DD')
        ORDER BY SECTOR_NAME
        """
        rows = db.execute(sql, {"run_id": run_id, "trade_date": trade_date})
        return self._rows_to_dicts(rows)

    # ---------------------------------------------------------
    @staticmethod
    def _jsonable(v: Any) -> Any:
        """Convert DB scalar types into JSON-serializable values.

        Reason:
        - Some callers (e.g. debug dump / snapshot export) may json.dump the raw block.
        - SQLAlchemy/Oracle rows may contain datetime/date/Decimal which are not JSON-serializable.
        """
        if v is None:
            return None
        # datetime/date
        if isinstance(v, (datetime, date)):
            try:
                return v.isoformat()
            except Exception:
                return str(v)
        # Decimal -> float (safe for display/debug)
        if isinstance(v, Decimal):
            try:
                return float(v)
            except Exception:
                return str(v)
        # bytes -> utf-8 str (best-effort)
        if isinstance(v, (bytes, bytearray)):
            try:
                return v.decode("utf-8", errors="replace")
            except Exception:
                return str(v)
        return v

    @staticmethod
    def _rows_to_dicts(rows: Any) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        if not rows:
            return out
        for r in rows:
            try:
                m = getattr(r, "_mapping", None)
                if m:
                    # Normalize keys to UPPERCASE to match frozen snapshot schema
                    # (Oracle columns are uppercase; downstream report blocks expect uppercase)
                    out.append({str(k).upper(): RotationSnapshotDataSource._jsonable(m[k]) for k in m.keys()})
                else:
                    # tuple fallback (should not happen for SQLAlchemy Row)
                    out.append({"_row": list(r)})
            except Exception:
                out.append({"_row": str(r)})
        return out

    @staticmethod
    def _split_detail_summary(rows: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
        detail: List[Dict[str, Any]] = []
        summary: Optional[Dict[str, Any]] = None
        for r in rows or []:
            sid = r.get("SECTOR_ID")
            try:
                sid_int = int(sid) if sid is not None else None
            except Exception:
                sid_int = None
            if sid_int == -1:
                summary = r
            else:
                detail.append(r)
        return detail, summary
