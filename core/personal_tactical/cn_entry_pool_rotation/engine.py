#-*- coding: utf-8 -*-
"""
engine.py  (CN_ENTRY_POOL_ROTATION_V1)

Public API (frozen):
- run_eod(trade_date: str) -> str
- run_t1(trade_date: str) -> str
"""
from __future__ import annotations

from datetime import datetime, date
from typing import List, Dict

from .config import EPRConfig, load_config, default_state_row_from_db
from .oracle_facts import make_oracle_engine, get_facts_for_symbols
from .sqlite_store import SQLiteStore
from .state_machine import evaluate_eod, evaluate_t1
from .reporting import format_eod_summary, format_t1_summary


class CNEntryPoolRotationEngine:
    def __init__(self, cfg: EPRConfig):
        self.cfg = cfg
        self.oracle_engine = make_oracle_engine(cfg.oracle_dsn)
        self.store = SQLiteStore(cfg.sqlite_path, cfg.sqlite_schema_path)

    def run_eod(self, trade_date: str) -> str:
        self.store.ensure_schema()
        self.store.upsert_entry_pool(self.cfg.entry_pool)

        prior_states = self.store.get_latest_state_before(trade_date)

        td = self._parse_trade_date(trade_date)
        facts_map = get_facts_for_symbols(
            self.oracle_engine,
            td,
            self.cfg.symbol_map_internal_to_oracle(),
            lookback_high=self.cfg.lookback_high,
            lookback_vol_ma=self.cfg.lookback_vol_ma,
        )

        transitions: List[dict] = []
        snaps: Dict[str, dict] = {}
        asof = datetime.now().isoformat(timespec="seconds")

        for symbol in self.cfg.entry_pool_symbols():
            prior = default_state_row_from_db(symbol, prior_states.get(symbol))
            f = facts_map[symbol]

            # breakout_level fallback (spec): use prior.snap if present; else use high_60d
            breakout_level = prior.breakout_level if prior.breakout_level is not None else f.high_60d

            # IMPORTANT: state_machine.evaluate_eod signature is frozen without extra kwargs.
            # It should derive breakout_level internally (or accept via prior.breakout_level).
            tr = evaluate_eod(self.cfg, trade_date, symbol, prior, f)

            if tr is not None:
                self.store.insert_state_event(
                    trade_date=trade_date,
                    symbol=symbol,
                    event_kind=tr.event_kind,
                    from_state=tr.from_state,
                    to_state=tr.to_state,
                    reason_code=tr.reason_code,
                    reason_text=tr.reason_text,
                    payload_json=tr.payload_json,
                )
                transitions.append(tr.as_dict())
                snap = tr.snap_after
            else:
                snap = prior.to_snap_dict(trade_date, asof, breakout_level)

            self.store.upsert_state_snap(
                trade_date=snap["trade_date"],
                symbol=snap["symbol"],
                state=snap["state"],
                breakout_level=snap.get("breakout_level"),
                confirm_ok_streak=int(snap.get("confirm_ok_streak") or 0),
                fail_streak=int(snap.get("fail_streak") or 0),
                cooldown_days_left=int(snap.get("cooldown_days_left") or 0),
                asof=snap["asof"],
            )
            snaps[symbol] = snap

        return format_eod_summary(trade_date, snaps, transitions)

    def run_t1(self, trade_date: str) -> str:
        self.store.ensure_schema()
        self.store.upsert_entry_pool(self.cfg.entry_pool)

        prior_states = self.store.get_latest_state_before(trade_date)
        prior_positions = self.store.get_latest_position_before(trade_date)

        self.store.clear_execution_on(trade_date)

        transitions: List[dict] = []
        executions: List[dict] = []
        asof = datetime.now().isoformat(timespec="seconds")

        for symbol in self.cfg.entry_pool_symbols():
            prior = default_state_row_from_db(symbol, prior_states.get(symbol))
            pos = prior_positions.get(symbol, {})
            prior_lots = int(pos.get("position_lots") or 0)

            tr, exec_row = evaluate_t1(
                self.cfg,
                trade_date,
                symbol,
                prior,
                prior_position_lots=prior_lots,
                max_lots_2026=self.cfg.entry_pool[symbol].max_lots_2026,
            )

            if tr is not None:
                self.store.insert_state_event(
                    trade_date=trade_date,
                    symbol=symbol,
                    event_kind=tr.event_kind,
                    from_state=tr.from_state,
                    to_state=tr.to_state,
                    reason_code=tr.reason_code,
                    reason_text=tr.reason_text,
                    payload_json=tr.payload_json,
                )
                transitions.append(tr.as_dict())
                snap = tr.snap_after
            else:
                snap = prior.to_snap_dict(trade_date, asof, prior.breakout_level)

            self.store.upsert_state_snap(
                trade_date=snap["trade_date"],
                symbol=snap["symbol"],
                state=snap["state"],
                breakout_level=snap.get("breakout_level"),
                confirm_ok_streak=int(snap.get("confirm_ok_streak") or 0),
                fail_streak=int(snap.get("fail_streak") or 0),
                cooldown_days_left=int(snap.get("cooldown_days_left") or 0),
                asof=snap["asof"],
            )

            if exec_row is not None:
                self.store.upsert_execution(
                    trade_date=trade_date,
                    symbol=symbol,
                    action=exec_row["action"],
                    lots=int(exec_row["lots"]),
                    limit_price=exec_row.get("limit_price"),
                    note=exec_row["note"],
                    payload_json=exec_row["payload_json"],
                )
                executions.append(exec_row)

                new_lots = exec_row.get("post_position_lots")
                if new_lots is not None:
                    self.store.upsert_position_snap(
                        trade_date=trade_date,
                        symbol=symbol,
                        position_lots=int(new_lots),
                        avg_cost=None,
                        asof=asof,
                    )

        return format_t1_summary(trade_date, executions, transitions)

    @staticmethod
    def _parse_trade_date(s: str) -> date:
        try:
            return datetime.strptime(s, "%Y-%m-%d").date()
        except Exception as e:
            raise RuntimeError(f"Invalid --trade-date '{s}', expected YYYY-MM-DD") from e


def build_engine() -> CNEntryPoolRotationEngine:
    cfg = load_config()
    return CNEntryPoolRotationEngine(cfg)
