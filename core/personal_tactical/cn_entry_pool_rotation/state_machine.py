#-*- coding: utf-8 -*-
"""
state_machine.py (CN_ENTRY_POOL_ROTATION_V1)

Frozen state machine (must not change):
States:
  READY, TRIGGERED, HOLDING, CONFIRMED, FAILED, COOLING

Transitions (frozen):
READY -> TRIGGERED
  close > high_60d
  volume > 1.5 * vol_ma20
  cooldown_days_left == 0

TRIGGERED -> HOLDING
  T+1 execute BUY  (handled in evaluate_t1)

HOLDING -> CONFIRMED
  2 consecutive days close not below breakout_level

HOLDING/CONFIRMED -> FAILED
  2 consecutive days close below breakout_level

FAILED -> COOLING
  after SELL (handled in evaluate_t1), set cooldown_days_left=cooling_days

COOLING -> READY
  cooldown_days_left reaches 0 (evaluated in EOD)
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Optional, Tuple

from .config import EPRConfig, StateRow
from .oracle_facts import SymbolFacts


@dataclass(frozen=True)
class TransitionResult:
    event_kind: str
    from_state: str
    to_state: str
    reason_code: str
    reason_text: str
    payload_json: str
    snap_after: dict

    def as_dict(self) -> dict:
        return {
            "event_kind": self.event_kind,
            "from_state": self.from_state,
            "to_state": self.to_state,
            "reason_code": self.reason_code,
            "reason_text": self.reason_text,
            "payload_json": self.payload_json,
            "snap_after": self.snap_after,
        }


def _snap(symbol: str, trade_date: str, state: str, breakout_level: Optional[float],
          confirm_ok_streak: int, fail_streak: int, cooldown_days_left: int, asof: str) -> dict:
    return {
        "trade_date": trade_date,
        "symbol": symbol,
        "state": state,
        "breakout_level": breakout_level,
        "confirm_ok_streak": int(confirm_ok_streak),
        "fail_streak": int(fail_streak),
        "cooldown_days_left": int(cooldown_days_left),
        "asof": asof,
    }


def evaluate_eod(cfg: EPRConfig, trade_date: str, symbol: str, prior: StateRow, facts: SymbolFacts) -> Optional[TransitionResult]:
    # Frozen breakout reference: high_60d computed excluding trade_date.
    breakout = prior.breakout_level if prior.breakout_level is not None else facts.high_60d

    # COOLING countdown handled daily
    if prior.state == "COOLING":
        new_left = max(0, int(prior.cooldown_days_left) - 1)
        if new_left == 0:
            payload = {"cooldown_days_left": new_left}
            return TransitionResult(
                event_kind="STATE_TRANSITION",
                from_state="COOLING",
                to_state="READY",
                reason_code="COOLDOWN_END",
                reason_text="Cooling period ended; back to READY",
                payload_json=json.dumps(payload, ensure_ascii=False),
                snap_after=_snap(symbol, trade_date, "READY", None, 0, 0, 0, trade_date),
            )
        else:
            # no transition, but snap will be written by engine using prior; keep state as COOLING via prior snap
            # We encode a transition only when state changes (frozen: state_event only for transitions)
            return None

    # READY -> TRIGGERED (EOD detection)
    if prior.state == "READY":
        if int(prior.cooldown_days_left) != 0:
            return None
        if (facts.close > facts.high_60d) and (facts.volume > cfg.vol_multiplier * facts.vol_ma20):
            payload = {
                "close": facts.close,
                "high_60d": facts.high_60d,
                "volume": facts.volume,
                "vol_ma20": facts.vol_ma20,
                "breakout_level": facts.high_60d,
            }
            return TransitionResult(
                event_kind="STATE_TRANSITION",
                from_state="READY",
                to_state="TRIGGERED",
                reason_code="BREAKOUT",
                reason_text="Breakout detected: close>high_60d and volume>mult*vol_ma20",
                payload_json=json.dumps(payload, ensure_ascii=False),
                snap_after=_snap(symbol, trade_date, "TRIGGERED", float(facts.high_60d), 0, 0, 0, trade_date),
            )
        return None

    # HOLDING / CONFIRMED confirmation / failure tracking
    if prior.state in ("HOLDING", "CONFIRMED"):
        # close not below breakout -> confirm_ok_streak++
        if facts.close >= breakout:
            confirm_ok = int(prior.confirm_ok_streak) + 1
            fail_streak = 0
        else:
            confirm_ok = 0
            fail_streak = int(prior.fail_streak) + 1

        # HOLDING -> CONFIRMED after 2 OK days
        if prior.state == "HOLDING" and confirm_ok >= cfg.confirm_days:
            payload = {"confirm_ok_streak": confirm_ok, "breakout_level": breakout, "close": facts.close}
            return TransitionResult(
                event_kind="STATE_TRANSITION",
                from_state="HOLDING",
                to_state="CONFIRMED",
                reason_code="CONFIRM_2D",
                reason_text="2 consecutive closes not below breakout level",
                payload_json=json.dumps(payload, ensure_ascii=False),
                snap_after=_snap(symbol, trade_date, "CONFIRMED", float(breakout), confirm_ok, 0, 0, trade_date),
            )

        # HOLDING/CONFIRMED -> FAILED after 2 fail days
        if fail_streak >= cfg.fail_days:
            payload = {"fail_streak": fail_streak, "breakout_level": breakout, "close": facts.close}
            return TransitionResult(
                event_kind="STATE_TRANSITION",
                from_state=prior.state,
                to_state="FAILED",
                reason_code="FAIL_2D",
                reason_text="2 consecutive closes below breakout level",
                payload_json=json.dumps(payload, ensure_ascii=False),
                snap_after=_snap(symbol, trade_date, "FAILED", float(breakout), 0, fail_streak, 0, trade_date),
            )

        # no state change; engine will write snap carry-forward; we do not emit event
        return None

    # TRIGGERED / FAILED handled by T+1, no EOD transition
    return None


def evaluate_t1(
    cfg: EPRConfig,
    trade_date: str,
    symbol: str,
    prior: StateRow,
    prior_position_lots: int,
    max_lots_2026: int,
) -> Tuple[Optional[TransitionResult], Optional[dict]]:
    # TRIGGERED -> HOLDING with BUY suggestion (T+1)
    if prior.state == "TRIGGERED":
        # buy 1 lot by default, capped by max_lots_2026
        target = min(max_lots_2026, max(0, int(max_lots_2026)))
        add = 1 if prior_position_lots < target else 0
        if add <= 0:
            # no execution; still transition to HOLDING? frozen spec says TRIGGERED->HOLDING on T+1 execute BUY.
            # If cannot buy due to cap=0, we keep TRIGGERED and output NONE.
            return None, {
                "trade_date": trade_date,
                "symbol": symbol,
                "action": "NONE",
                "lots": 0,
                "limit_price": None,
                "note": "TRIGGERED but max_lots cap prevents BUY",
                "payload_json": json.dumps({"prior_lots": prior_position_lots, "cap": target}, ensure_ascii=False),
                "post_position_lots": prior_position_lots,
            }

        payload = {"buy_lots": add, "prior_lots": prior_position_lots, "cap": target}
        tr = TransitionResult(
            event_kind="STATE_TRANSITION",
            from_state="TRIGGERED",
            to_state="HOLDING",
            reason_code="T1_BUY",
            reason_text="T+1 execute BUY after TRIGGERED",
            payload_json=json.dumps(payload, ensure_ascii=False),
            snap_after=_snap(symbol, trade_date, "HOLDING", prior.breakout_level, 0, 0, 0, trade_date),
        )
        exec_row = {
            "trade_date": trade_date,
            "symbol": symbol,
            "action": "BUY",
            "lots": add,
            "limit_price": None,
            "note": "T+1 BUY (confirm-only mode)",
            "payload_json": json.dumps(payload, ensure_ascii=False),
            "post_position_lots": prior_position_lots + add,
        }
        return tr, exec_row

    # FAILED -> COOLING with SELL suggestion
    if prior.state == "FAILED":
        if prior_position_lots <= 0:
            # no position to sell, but still enter COOLING (since failure occurred)
            payload = {"sell_lots": 0, "prior_lots": 0, "cooling_days": cfg.cooling_days}
            tr = TransitionResult(
                event_kind="STATE_TRANSITION",
                from_state="FAILED",
                to_state="COOLING",
                reason_code="T1_COOLING",
                reason_text="Enter cooling after FAILED (no lots to sell)",
                payload_json=json.dumps(payload, ensure_ascii=False),
                snap_after=_snap(symbol, trade_date, "COOLING", None, 0, 0, cfg.cooling_days, trade_date),
            )
            exec_row = {
                "trade_date": trade_date,
                "symbol": symbol,
                "action": "NONE",
                "lots": 0,
                "limit_price": None,
                "note": "FAILED but no position; enter COOLING",
                "payload_json": json.dumps(payload, ensure_ascii=False),
                "post_position_lots": 0,
            }
            return tr, exec_row

        payload = {"sell_lots": prior_position_lots, "prior_lots": prior_position_lots, "cooling_days": cfg.cooling_days}
        tr = TransitionResult(
            event_kind="STATE_TRANSITION",
            from_state="FAILED",
            to_state="COOLING",
            reason_code="T1_SELL",
            reason_text="T+1 SELL after FAILED, then enter COOLING",
            payload_json=json.dumps(payload, ensure_ascii=False),
            snap_after=_snap(symbol, trade_date, "COOLING", None, 0, 0, cfg.cooling_days, trade_date),
        )
        exec_row = {
            "trade_date": trade_date,
            "symbol": symbol,
            "action": "SELL",
            "lots": prior_position_lots,
            "limit_price": None,
            "note": "T+1 SELL (exit) then COOLING",
            "payload_json": json.dumps(payload, ensure_ascii=False),
            "post_position_lots": 0,
        }
        return tr, exec_row

    return None, None
