from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional

RiskLevel = Literal["LOW", "NEUTRAL", "HIGH"]


@dataclass(frozen=True)
class ReportBlock:
    """
    Phase-3 Report block (extension blocks only).

    Frozen:
    - block_alias is semantic identity
    - block_id is ordering only
    - warnings is explicit (no silent skip)
    """
    #block_id: str
    block_alias: str
    title: str
    payload: Any
    warnings: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class ReportDocument:
    """
    Frozen Design-B (single source of truth):
    ReportDocument(meta, actionhint, summary, blocks)

    NOTE:
    - meta/actionhint/summary are TOP-LEVEL only (no duplicated core blocks)
    - blocks contains extension blocks only (e.g., 2~7)
    """
    meta: Dict[str, Any]
    actionhint: Dict[str, Any]
    summary: str
    blocks: List[ReportBlock]
