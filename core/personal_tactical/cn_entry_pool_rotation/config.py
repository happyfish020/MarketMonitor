#-*- coding: utf-8 -*-
"""
config.py (CN_ENTRY_POOL_ROTATION_V1)

Frozen responsibilities:
- Provide default paths (SQLite DB + schema SQL)
- Provide frozen Entry Pool (6 stocks) with themes and max_lots_2026
- Provide Oracle DSN (fixed default, but still loaded as a single value)
- Optionally load a YAML to override *values* (not structure):
    - oracle_dsn
    - sqlite_path
    - sqlite_schema_path
    - entry_pool[].oracle_symbol
    - entry_pool[].max_lots_2026
    - entry_pool[].is_active

Hard constraints:
- No dependency on other UnifiedRisk modules
- Stable function signatures
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

import yaml


@dataclass(frozen=True)
class EntryPoolItem:
    symbol: str
    theme: str
    name: str
    oracle_symbol: str
    entry_mode: str
    max_lots_2026: int
    is_active: int = 1


@dataclass(frozen=True)
class StateRow:
    symbol: str
    state: str
    breakout_level: float | None
    confirm_ok_streak: int
    fail_streak: int
    cooldown_days_left: int

    def to_snap_dict(self, trade_date: str, asof: str, breakout_level_fallback: float | None) -> dict:
        return {
            "trade_date": trade_date,
            "symbol": self.symbol,
            "state": self.state,
            "breakout_level": self.breakout_level if self.breakout_level is not None else breakout_level_fallback,
            "confirm_ok_streak": int(self.confirm_ok_streak),
            "fail_streak": int(self.fail_streak),
            "cooldown_days_left": int(self.cooldown_days_left),
            "asof": asof,
        }


@dataclass(frozen=True)
class EPRConfig:
    oracle_dsn: str
    sqlite_path: Path
    sqlite_schema_path: Path  # frozen attribute name used by engine/store
    entry_pool: Dict[str, EntryPoolItem]

    lookback_high: int = 60
    lookback_vol_ma: int = 20
    vol_multiplier: float = 1.5
    confirm_days: int = 2
    fail_days: int = 2
    cooling_days: int = 5

    def entry_pool_symbols(self) -> list[str]:
        return list(self.entry_pool.keys())

    def symbol_map_internal_to_oracle(self) -> Dict[str, str]:
        return {k: v.oracle_symbol for k, v in self.entry_pool.items()}


def _default_repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _default_schema_path() -> Path:
    return Path(__file__).resolve().parent / "resources" / "sqlite_schema.sql"


def _default_sqlite_path() -> Path:
    return _default_repo_root() / "data" / "cn_entry_pool_rotation.db"


def _frozen_entry_pool() -> Dict[str, EntryPoolItem]:
    return {
        "002463": EntryPoolItem("002463", "AI_HARDWARE", "沪电股份", "002463", "T1_CONFIRM_ONLY", 2, 1),
        "300476": EntryPoolItem("300476", "AI_HARDWARE", "胜宏科技", "300476", "T1_CONFIRM_ONLY", 2, 1),
        "300308": EntryPoolItem("300308", "AI_HARDWARE", "中际旭创", "300308", "T1_CONFIRM_ONLY", 2, 1),
        "603986": EntryPoolItem("603986", "SEMI_SUBSTITUTION", "兆易创新", "603986", "T1_CONFIRM_ONLY", 2, 1),
        "300054": EntryPoolItem("300054", "SEMI_SUBSTITUTION", "鼎龙股份", "300054", "T1_CONFIRM_ONLY", 2, 1),
        "300223": EntryPoolItem("300223", "SEMI_SUBSTITUTION", "北京君正", "300223", "T1_CONFIRM_ONLY", 2, 1),
    }


def load_config(yaml_path: Optional[str] = None) -> EPRConfig:
    oracle_dsn = "oracle+oracledb://secopr:secopr@localhost:1521/xe"
    sqlite_path = _default_sqlite_path()
    sqlite_schema_path = _default_schema_path()
    entry_pool = _frozen_entry_pool()

    if yaml_path:
        p = Path(yaml_path)
        if not p.exists():
            raise RuntimeError(f"Config YAML not found: {p}")
        raw = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
        if not isinstance(raw, dict):
            raise RuntimeError("Config YAML must be a mapping/object")

        if raw.get("oracle_dsn"):
            oracle_dsn = str(raw["oracle_dsn"])
        if raw.get("sqlite_path"):
            sqlite_path = Path(str(raw["sqlite_path"]))
        if raw.get("sqlite_schema_path"):
            sqlite_schema_path = Path(str(raw["sqlite_schema_path"]))

        pool_over = raw.get("entry_pool") or {}
        if pool_over:
            if not isinstance(pool_over, dict):
                raise RuntimeError("entry_pool in YAML must be a mapping: {symbol: {..}}")
            for sym, ov in pool_over.items():
                if sym not in entry_pool:
                    raise RuntimeError(f"YAML entry_pool contains unknown symbol: {sym}")
                if not isinstance(ov, dict):
                    raise RuntimeError(f"YAML entry_pool.{sym} must be a mapping/object")
                cur = entry_pool[sym]
                entry_pool[sym] = EntryPoolItem(
                    symbol=cur.symbol,
                    theme=cur.theme,
                    name=cur.name,
                    oracle_symbol=str(ov.get("oracle_symbol", cur.oracle_symbol)),
                    entry_mode=cur.entry_mode,
                    max_lots_2026=int(ov.get("max_lots_2026", cur.max_lots_2026)),
                    is_active=int(ov.get("is_active", cur.is_active)),
                )

    return EPRConfig(
        oracle_dsn=oracle_dsn,
        sqlite_path=sqlite_path,
        sqlite_schema_path=sqlite_schema_path,
        entry_pool=entry_pool,
    )


def default_state_row(symbol: str) -> StateRow:
    return StateRow(symbol, "READY", None, 0, 0, 0)


def default_state_row_from_db(symbol: str, raw: Optional[dict]) -> StateRow:
    if not raw:
        return default_state_row(symbol)
    return StateRow(
        symbol=symbol,
        state=str(raw.get("state") or "READY"),
        breakout_level=raw.get("breakout_level"),
        confirm_ok_streak=int(raw.get("confirm_ok_streak") or 0),
        fail_streak=int(raw.get("fail_streak") or 0),
        cooldown_days_left=int(raw.get("cooldown_days_left") or 0),
    )
