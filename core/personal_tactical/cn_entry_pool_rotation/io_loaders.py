from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def load_facts(facts_json: Path) -> Dict[str, Dict[str, Any]]:
    """Expected format:
    {
      "trade_date": "YYYY-MM-DD",
      "facts": {
        "300308.SZ": {"close": 1.0, "volume": 1.0, "high_60d": 1.0, "vol_ma20": 1.0},
        ...
      }
    }
    """
    data = load_json(facts_json)
    facts = data.get("facts") or {}
    if not isinstance(facts, dict):
        raise ValueError("facts_json must contain an object 'facts'")
    return {str(k): dict(v) for k, v in facts.items()}


def load_positions(positions_json: Path) -> Dict[str, int]:
    """Expected format:
    {
      "trade_date": "YYYY-MM-DD",
      "positions": {
        "300308.SZ": {"holding_lots": 1},
        ...
      }
    }
    """
    data = load_json(positions_json)
    pos = data.get("positions") or {}
    if not isinstance(pos, dict):
        raise ValueError("positions_json must contain an object 'positions'")
    out: Dict[str, int] = {}
    for sym, v in pos.items():
        if isinstance(v, dict):
            out[str(sym)] = int(v.get("holding_lots", 0))
        else:
            out[str(sym)] = int(v)
    return out


def load_executions(executions_json: Path) -> List[Dict[str, Any]]:
    """Expected format:
    {
      "trade_date": "YYYY-MM-DD",
      "executions": [
        {"symbol": "300308.SZ", "action": "BUY", "lots": 1, "source": "MANUAL"},
        ...
      ]
    }
    """
    data = load_json(executions_json)
    ex = data.get("executions") or []
    if not isinstance(ex, list):
        raise ValueError("executions_json must contain a list 'executions'")
    return [dict(x) for x in ex]
