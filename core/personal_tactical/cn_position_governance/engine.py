from __future__ import annotations

import datetime as _dt
import uuid
from dataclasses import dataclass
from typing import Dict, List, Tuple

from .config import PgRunContext, RISK_HIGH, SQLITE_PATH
from .oracle_facts import OracleFacts
from .sqlite_store import SqliteStore, PositionStateRow, EventRow
from .state_machine import RiskStateMachine
from .reporting import ReportRow


@dataclass(frozen=True)
class EvalResult:
    symbol: str
    theme: str
    risk_level: str
    add_permission: int
    trim_required: int
    reason_add: str
    reason_trim: str
    lots_held: int
    avg_cost: float
    exposure_pct: float


class CnPositionGovernanceEngine:
    """Independent personal tactical subsystem: position governance + add/trim control."""

    def __init__(self, store: SqliteStore) -> None:
        self.store = store
        self.oracle = OracleFacts()

    @staticmethod
    def _now_iso() -> str:
        return _dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    @staticmethod
    def _parse_trade_date(s: str) -> _dt.date:
        return _dt.date.fromisoformat(s)

    def run_eod(self, trade_date: str, run_id: str) -> List[EvalResult]:
        self.store.init_schema()
        cfgs = self.store.list_position_configs()
        symbols = [c.symbol for c in cfgs]
        if not symbols:
            return []

        bars_map = self.oracle.load_bars(symbols=symbols, end_trade_date=trade_date, lookback_days=35)

        # theme exposure uses existing state rows for this trade_date (if rerun, it will be overwritten later)
        theme_exp = self.store.get_theme_exposure_pct(trade_date)

        results: List[EvalResult] = []
        for c in cfgs:
            facts = bars_map.get(c.symbol)
            metrics = RiskStateMachine.compute_metrics(facts.bars if facts else [])
            if metrics is None:
                risk_level = "NORMAL"
                add_permission = 0
                trim_required = 0
                reason_add = "insufficient_bars(<21)"
                reason_trim = "insufficient_bars(<21)"
            else:
                risk_level = RiskStateMachine.classify(metrics)

                # A. Add permission (frozen rules)
                add_reasons = []
                add_permission = 1

                # risk != HIGH
                if risk_level == RISK_HIGH:
                    add_permission = 0
                    add_reasons.append("risk=HIGH")

                # drawdown <= -8% (only allow low-price add)
                if metrics.dd_10d > -0.08:
                    add_permission = 0
                    add_reasons.append(f"dd_10d({metrics.dd_10d:.4f})>-0.08")

                # max_lots constraint (lots_held to be provided by user; default 0)
                lots_held = 0
                if lots_held >= int(c.max_lots):
                    add_permission = 0
                    add_reasons.append(f"lots_held({lots_held})>=max_lots({c.max_lots})")

                # theme cap
                cur_theme_exp = float(theme_exp.get(c.theme, 0.0))
                if cur_theme_exp >= float(c.theme_cap_pct):
                    add_permission = 0
                    add_reasons.append(f"theme_exp({cur_theme_exp:.4f})>=cap({c.theme_cap_pct:.4f})")

                # config enable_add
                if int(c.enable_add) != 1:
                    add_permission = 0
                    add_reasons.append("enable_add=0")

                reason_add = ";".join(add_reasons) if add_reasons else "ok"

                # B. Trim required (frozen rules)
                trim_reasons = []
                trim_required = 0

                # 10d drawdown > -15%  => interpret as dd_10d < -0.15 (worse than -15%)
                if metrics.dd_10d < -0.15:
                    trim_required = 1
                    trim_reasons.append(f"dd_10d({metrics.dd_10d:.4f})<-0.15")

                # 3d close below MA20
                if metrics.below_ma20_3d:
                    trim_required = 1
                    trim_reasons.append("3d_close_below_ma20")

                # theme overall risk HIGH => if any symbol in theme is HIGH on this run, mark theme_high later
                # We'll compute theme_high after first pass.
                reason_trim = ";".join(trim_reasons) if trim_reasons else "no_trigger"

            results.append(
                EvalResult(
                    symbol=c.symbol,
                    theme=c.theme,
                    risk_level=risk_level,
                    add_permission=add_permission,
                    trim_required=trim_required,
                    reason_add=reason_add,
                    reason_trim=reason_trim,
                    lots_held=0,
                    avg_cost=0.0,
                    exposure_pct=0.0,
                )
            )

        # Theme HIGH: if any symbol in theme has risk HIGH, force trim_required=1 for theme members with enable_trim=1
        theme_high = {}
        for r in results:
            theme_high.setdefault(r.theme, False)
            if r.risk_level == RISK_HIGH:
                theme_high[r.theme] = True

        adjusted: List[EvalResult] = []
        for r in results:
            # find cfg
            c = next(cc for cc in cfgs if cc.symbol == r.symbol)
            trim_required = r.trim_required
            reason_trim = r.reason_trim
            if theme_high.get(r.theme, False) and int(c.enable_trim) == 1:
                if trim_required != 1:
                    trim_required = 1
                    reason_trim = "theme_risk=HIGH"
                else:
                    reason_trim = (reason_trim + ";theme_risk=HIGH") if reason_trim else "theme_risk=HIGH"
            adjusted.append(
                EvalResult(
                    symbol=r.symbol,
                    theme=r.theme,
                    risk_level=r.risk_level,
                    add_permission=r.add_permission,
                    trim_required=trim_required,
                    reason_add=r.reason_add,
                    reason_trim=reason_trim,
                    lots_held=r.lots_held,
                    avg_cost=r.avg_cost,
                    exposure_pct=r.exposure_pct,
                )
            )

        # Persist state + events (idempotent)
        ts = self._now_iso()
        for r in adjusted:
            self.store.upsert_position_state(
                PositionStateRow(
                    trade_date=trade_date,
                    symbol=r.symbol,
                    lots_held=r.lots_held,
                    avg_cost=r.avg_cost,
                    exposure_pct=r.exposure_pct,
                    risk_level=r.risk_level,
                    add_permission=r.add_permission,
                    trim_required=r.trim_required,
                    run_id=run_id,
                    created_at=ts,
                )
            )

            if r.add_permission == 1:
                self.store.insert_event_dedup(
                    EventRow(
                        trade_date=trade_date,
                        symbol=r.symbol,
                        event_type="EOD_ADD_ALLOWED",
                        reason=r.reason_add,
                        run_id=run_id,
                        created_at=ts,
                    )
                )
            else:
                self.store.insert_event_dedup(
                    EventRow(
                        trade_date=trade_date,
                        symbol=r.symbol,
                        event_type="EOD_ADD_BLOCKED",
                        reason=r.reason_add,
                        run_id=run_id,
                        created_at=ts,
                    )
                )

            if r.trim_required == 1:
                self.store.insert_event_dedup(
                    EventRow(
                        trade_date=trade_date,
                        symbol=r.symbol,
                        event_type="EOD_TRIM_REQUIRED",
                        reason=r.reason_trim,
                        run_id=run_id,
                        created_at=ts,
                    )
                )

        return adjusted

    def run_t1(self, trade_date: str, run_id: str) -> List[Dict[str, str]]:
        """Generate T+1 execution signals from the EOD state (written to event_log)."""
        self.store.init_schema()
        rows = self.store.get_state_rows(trade_date)
        ts = self._now_iso()
        out: List[Dict[str, str]] = []
        for r in rows:
            symbol = r["symbol"]
            add_permission = int(r["add_permission"])
            trim_required = int(r["trim_required"])
            risk_level = r["risk_level"]

            if trim_required == 1:
                signal = "TRIM_REQUIRED"
                reason = f"trim_required=1;risk={risk_level}"
            elif add_permission == 1:
                signal = "ADD_ALLOWED"
                reason = f"add_permission=1;risk={risk_level}"
            else:
                signal = "HOLD_ONLY"
                reason = f"add_permission=0;risk={risk_level}"

            self.store.insert_event_dedup(
                EventRow(
                    trade_date=trade_date,
                    symbol=symbol,
                    event_type="T1_SIGNAL",
                    reason=f"{signal}::{reason}",
                    run_id=run_id,
                    created_at=ts,
                )
            )
            out.append({"symbol": symbol, "signal": signal, "reason": reason})
        return out
