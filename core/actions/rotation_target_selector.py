# -*- coding: utf-8 -*-
"""UnifiedRisk V12 · Rotation Target Selector (ETF V1)

Frozen Contract:
- ONLY reads local config (config/rotation_targets.yaml) and rotation snapshot fields.
- NO DB access, NO views, NO analytics.
- Fail-closed if mapping required but missing.

Output:
- dict { 'target_type': 'ETF', 'symbol': 'sh.512710', 'code': '512710', 'method': 'by_sector_id|by_sector_name', 'reasons': [...] }
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

import yaml

from core.utils.logger import get_logger
from core.utils.config_loader import CONFIG_DIR

LOG = get_logger("RotationTarget")


def _load_yaml(name: str) -> Dict[str, Any]:
    path = os.path.join(CONFIG_DIR, name)
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _normalize_etf_symbol(code: str) -> Optional[str]:
    if not isinstance(code, str):
        return None
    c = code.strip()
    if not c:
        return None
    if c.startswith("sh.") or c.startswith("sz."):
        return c
    # Accept pure 6-digit codes
    if c.isdigit() and len(c) == 6:
        return ("sh." + c) if c.startswith("5") else ("sz." + c)
    # Otherwise treat as unknown
    return None


class RotationTargetSelector:
    """Select tradable target for rotation Top1.

    ETF V1: requires a deterministic mapping from sector -> ETF.
    """

    def __init__(self) -> None:
        cfg = _load_yaml("rotation_targets.yaml")
        root = cfg.get("rotation_targets") if isinstance(cfg, dict) else None
        self.cfg = root if isinstance(root, dict) else {}
        self.require_mapping = bool(self.cfg.get("require_etf_mapping", True))
        self.by_sector_id = self.cfg.get("by_sector_id") if isinstance(self.cfg.get("by_sector_id"), dict) else {}
        self.by_sector_name = self.cfg.get("by_sector_name") if isinstance(self.cfg.get("by_sector_name"), dict) else {}

    def select(self, *, sector_id: Any, sector_name: Any) -> Dict[str, Any]:
        reasons = []
        sid = None if sector_id is None else str(sector_id).strip()
        sname = None if sector_name is None else str(sector_name).strip()

        code = None
        method = None

        if sid and sid in self.by_sector_id:
            code = self.by_sector_id.get(sid)
            method = "by_sector_id"
        elif sname and sname in self.by_sector_name:
            code = self.by_sector_name.get(sname)
            method = "by_sector_name"

        symbol = _normalize_etf_symbol(str(code)) if code is not None else None

        if symbol is None:
            if self.require_mapping:
                reasons.append("veto:no_etf_mapping")
            else:
                reasons.append("note:no_etf_mapping")
        return {
            "target_type": "ETF",
            "symbol": symbol,
            "code": str(code) if code is not None else None,
            "method": method,
            "reasons": reasons,
        }
