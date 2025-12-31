# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class ReportArtifact:
    trade_date: str
    report_kind: str
    content_text: str
    content_hash: str
    meta: Optional[Dict[str, Any]]
    created_at_utc: int
