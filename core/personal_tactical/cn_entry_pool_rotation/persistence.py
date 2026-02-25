from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy import text


def load_position_lots_for_date(sqlite_engine, trade_date: str, symbols: List[str], default_lots: int = 0) -> Dict[str, int]:
    if not symbols:
        return {}
    q = text(
        """
        SELECT symbol, holding_lots
        FROM cn_epr_position_snap
        WHERE trade_date = :trade_date
          AND symbol IN ({in_list})
        """.format(in_list=",".join([f":s{i}" for i in range(len(symbols))]))
    )
    params: Dict[str, Any] = {"trade_date": trade_date}
    for i, s in enumerate(symbols):
        params[f"s{i}"] = s
    lots: Dict[str, int] = {s: int(default_lots) for s in symbols}
    with sqlite_engine.begin() as conn:
        rows = conn.execute(q, params).fetchall()
    for sym, hl in rows:
        lots[str(sym)] = int(hl or 0)
    return lots


def load_latest_state_snap(sqlite_engine, trade_date: Optional[str], symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    if not trade_date or not symbols:
        return {}
    q = text(
        """
        SELECT trade_date, symbol, state, breakout_level, cooldown_days_left, holding_lots
        FROM cn_epr_state_snap
        WHERE trade_date = :trade_date
          AND symbol IN ({in_list})
        """.format(in_list=",".join([f":s{i}" for i in range(len(symbols))]))
    )
    params: Dict[str, Any] = {"trade_date": trade_date}
    for i, s in enumerate(symbols):
        params[f"s{i}"] = s
    out: Dict[str, Dict[str, Any]] = {}
    with sqlite_engine.begin() as conn:
        rows = conn.execute(q, params).fetchall()
    for td, sym, st, bl, cd, hl in rows:
        out[str(sym)] = {
            "trade_date": td,
            "symbol": str(sym),
            "state": st,
            "breakout_level": bl,
            "cooldown_days_left": cd,
            "holding_lots": hl,
        }
    return out


def upsert_state_snap_rows(sqlite_engine, snap_rows: List[Dict[str, Any]]) -> None:
    if not snap_rows:
        return
    sql = text(
        """
        INSERT INTO cn_epr_state_snap (
          trade_date, symbol, state, breakout_level, trigger_close, trigger_volume_ratio,
          cooldown_days_left, holding_lots, entry_allowed, add_allowed, reduce_required,
          suggested_action, reason_codes, evidence_json, run_id, created_at
        ) VALUES (
          :trade_date, :symbol, :state, :breakout_level, :trigger_close, :trigger_volume_ratio,
          :cooldown_days_left, :holding_lots, :entry_allowed, :add_allowed, :reduce_required,
          :suggested_action, :reason_codes, :evidence_json, :run_id, :created_at
        )
        ON CONFLICT(trade_date, symbol) DO UPDATE SET
          state=excluded.state,
          breakout_level=excluded.breakout_level,
          trigger_close=excluded.trigger_close,
          trigger_volume_ratio=excluded.trigger_volume_ratio,
          cooldown_days_left=excluded.cooldown_days_left,
          holding_lots=excluded.holding_lots,
          entry_allowed=excluded.entry_allowed,
          add_allowed=excluded.add_allowed,
          reduce_required=excluded.reduce_required,
          suggested_action=excluded.suggested_action,
          reason_codes=excluded.reason_codes,
          evidence_json=excluded.evidence_json,
          run_id=excluded.run_id,
          created_at=excluded.created_at
        """
    )
    now = datetime.utcnow().isoformat(timespec="seconds")
    with sqlite_engine.begin() as conn:
        for r in snap_rows:
            payload = dict(r)
            payload.setdefault("created_at", now)
            # ensure evidence_json is string
            ev = payload.get("evidence_json")
            if isinstance(ev, (dict, list)):
                payload["evidence_json"] = json.dumps(ev, ensure_ascii=False)
            conn.execute(sql, payload)


def insert_state_events(sqlite_engine, event_rows: List[Dict[str, Any]]) -> None:
    if not event_rows:
        return
    sql = text(
        """
        INSERT INTO cn_epr_state_event (
          event_id, trade_date, symbol, prev_state, new_state, event_type,
          transition_rule, reason_codes, payload_json, run_id, created_at
        ) VALUES (
          :event_id, :trade_date, :symbol, :prev_state, :new_state, :event_type,
          :transition_rule, :reason_codes, :payload_json, :run_id, :created_at
        )
        """
    )
    now = datetime.utcnow().isoformat(timespec="seconds")
    with sqlite_engine.begin() as conn:
        for r in event_rows:
            payload = dict(r)
            payload.setdefault("created_at", now)
            pj = payload.get("payload_json")
            if isinstance(pj, (dict, list)):
                payload["payload_json"] = json.dumps(pj, ensure_ascii=False)
            conn.execute(sql, payload)
