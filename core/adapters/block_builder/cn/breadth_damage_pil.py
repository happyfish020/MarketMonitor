"""core/adapters/transformers/cn/breadth_damage_tr.py

UnifiedRisk V12 FULL - Breadth Damage Pillar BlockBuilder
-------------------------------------------------------
Role (Phase-2 structural pillar):
    Convert DB-backed breadth statistics into a structural pillar object
    consumable by StructuralContext and Phase-2 Gate.

Design constraints (frozen):
    - window fixed at 50
    - ...
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, Optional

from core.adapters.block_builder.block_builder_base import FactBlockBuilderBase
from core.utils.logger import get_logger

LOG = get_logger("TR.BreadthDamage")


@dataclass(frozen=True, slots=True)
class BreadthDamagePillar:
    # Minimal attrs consumed by StructuralContext (via getattr)
    state: str
    since: str
    confidence: float
    health: str
    is_flapping: bool
    sustained: bool

    # Extra attrs optionally consumed by Gate
    confirmed_damage: bool
    confirmed_repair: bool
    extreme: bool

    # Raw stats
    trade_date: str
    window: int
    new_lows_50d: int
    universe: int
    new_lows_ratio: float


def _today_iso(x: Any) -> Optional[str]:
    if x is None:
        return None
    if isinstance(x, str) and len(x) >= 10:
        return x[:10]
    if isinstance(x, date):
        return x.isoformat()
    if isinstance(x, datetime):
        return x.date().isoformat()
    return None


class BreadthDamageBlockBuilder(FactBlockBuilderBase):
    """Build `breadth_damage` structural pillar from `breadth_50d` block."""

    def __init__(self) -> None:
        super().__init__(name="BreadthDamageBlockBuilder")

    def build_block(self, snapshot: Dict[str, Any], refresh_mode: str = "none") -> Dict[str, Any]:
        if not isinstance(snapshot, dict):
            return snapshot

        b = snapshot.get("breadth_50d")
        if not isinstance(b, dict):
            # pillar absent; keep append-only discipline
            return snapshot

        trade_date = _today_iso(b.get("trade_date"))
        if not trade_date:
            return snapshot

        ratio = float(b.get("new_lows_ratio") or 0.0)
        new_lows = int(b.get("new_lows_50d") or 0)
        universe = int(b.get("universe") or 0)
        window = int(b.get("window") or 50)

        # ----------------------------
        # State mapping (Phase-2 pragmatic thresholds)
        # NOTE: thresholds can be frozen later; these are conservative defaults.
        # ----------------------------
        confirmed_damage = ratio >= 0.08
        extreme = ratio >= 0.15
        confirmed_repair = ratio <= 0.02

        if extreme:
            state = "EXTREME_DAMAGED"
        elif confirmed_damage:
            state = "CONFIRMED_DAMAGED"
        elif confirmed_repair:
            state = "CONFIRMED_REPAIR"
        else:
            state = "NEUTRAL"

        confidence = round(min(1.0, max(0.0, ratio / 0.10)), 3) if universe > 0 else 0.0

        # ----------------------------
        # Phase-2 minimal pillar: transformer is pure (no file writes).
        # Since / flapping are conservative defaults (can be upgraded later).
        # ----------------------------
        since = trade_date
        is_flapping = False

        # sustained: only meaningful for damaged state; conservative
        sustained = False

        health = "FAIL" if universe <= 0 else "HEALTHY"

        pillar = BreadthDamagePillar(
            state=state,
            since=since,
            confidence=confidence,
            health=health,
            is_flapping=is_flapping,
            sustained=sustained,
            confirmed_damage=confirmed_damage,
            confirmed_repair=confirmed_repair,
            extreme=extreme,
            trade_date=trade_date,
            window=window,
            new_lows_50d=new_lows,
            universe=universe,
            new_lows_ratio=ratio,
        )

        snapshot["breadth_damage"] = pillar
        return snapshot
