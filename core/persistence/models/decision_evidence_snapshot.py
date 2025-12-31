# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class DecisionEvidenceSnapshot:
    trade_date: str
    report_kind: str
    engine_version: str
    des_payload: Dict[str, Any]
    des_hash: str
    created_at_utc: int
