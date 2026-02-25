from __future__ import annotations

from dataclasses import dataclass
from typing import List, Dict


@dataclass(frozen=True)
class ReportRow:
    symbol: str
    theme: str
    risk_level: str
    add_permission: int
    trim_required: int
    lots_held: int
    exposure_pct: float


def render_eod(rows: List[ReportRow]) -> str:
    lines = []
    lines.append("CN_POSITION_GOVERNANCE_V1 :: EOD Summary")
    lines.append("symbol | theme | risk | add | trim | lots | exposure_pct")
    for r in rows:
        lines.append(
            f"{r.symbol} | {r.theme} | {r.risk_level} | {r.add_permission} | {r.trim_required} | {r.lots_held} | {r.exposure_pct:.4f}"
        )
    return "\n".join(lines)


def render_t1(rows: List[Dict[str, str]]) -> str:
    lines = []
    lines.append("CN_POSITION_GOVERNANCE_V1 :: T+1 Signals")
    lines.append("symbol | signal | reason")
    for r in rows:
        lines.append(f"{r['symbol']} | {r['signal']} | {r['reason']}")
    return "\n".join(lines)
