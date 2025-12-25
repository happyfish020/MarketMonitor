from __future__ import annotations

from dataclasses import dataclass

from typing import Optional, Dict, Any


@dataclass(frozen=True)
class ReportContext:
    """
    Phase-3 ReportContext (Design-B)

    Frozen:
    - kind is a STRING tag (e.g. "PRE_OPEN", "EOD")
    - slots carries Phase-2 outputs
    """

    trade_date: str
    kind: str            # ✅ 不再使用 ReportKind
    slots: Dict[str, Any]
    # Phase-3：行为裁决（新增）
    actionhint: Optional[Dict[str, Any]] = None