from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import yaml


@dataclass(frozen=True)
class PoolItem:
    symbol: str
    name: str
    group_code: str
    max_lots_2026: int
    is_active: bool


@dataclass(frozen=True)
class RotationConfig:
    entry_mode: str
    cooldown_days: int
    trigger_lookback_high_days: int
    trigger_vol_ma_days: int
    trigger_vol_spike_ratio: float
    confirm_days: int
    fail_days: int
    pool: List[PoolItem]


def load_config(path: Path) -> RotationConfig:
    data: Dict[str, Any] = yaml.safe_load(path.read_text(encoding="utf-8"))
    tr = data.get("trigger_rules", {})
    cr = data.get("confirm_rules", {})
    pool_items: List[PoolItem] = []
    for x in data.get("entry_pool", []) or []:
        pool_items.append(
            PoolItem(
                symbol=str(x["symbol"]),
                name=str(x.get("name", "")),
                group_code=str(x.get("group_code", "")),
                max_lots_2026=int(x.get("max_lots_2026", 0)),
                is_active=bool(x.get("is_active", True)),
            )
        )

    return RotationConfig(
        entry_mode=str(data.get("entry_mode", "T1_CONFIRM_ONLY")),
        cooldown_days=int(data.get("cooldown_days", 5)),
        trigger_lookback_high_days=int(tr.get("lookback_high_days", 60)),
        trigger_vol_ma_days=int(tr.get("vol_ma_days", 20)),
        trigger_vol_spike_ratio=float(tr.get("vol_spike_ratio", 1.5)),
        confirm_days=int(cr.get("confirm_days", 2)),
        fail_days=int(cr.get("fail_days", 2)),
        pool=pool_items,
    )
