# -*- coding: utf-8 -*-
"""UnifiedRisk V12 · Regime Audit Service · v1.0

OOP/SRP:
- Engine acts as orchestrator only: call RegimeAuditService.audit_regime_shift(...)
- History loading and audit write are encapsulated here.

Frozen rules:
- Read-only audit. Must not affect Gate/DRS/Execution.
- Best-effort. Never raise to break daily run.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
import sqlite3

from core.services.regime_history_service import RegimeHistoryService
from core.persistence.regime_shift_auditor import RegimeShiftAuditor


@dataclass(frozen=True)
class RegimeAuditService:
    @staticmethod
    def audit_regime_shift(conn: sqlite3.Connection, trade_date: str, report_kind: str, n: int = 10) -> None:
        try:
            if conn is None:
                return
            hist = RegimeHistoryService.load_history(conn=conn, report_kind=report_kind, n=n)
            RegimeShiftAuditor(conn).log_shift(trade_date=trade_date, report_kind=report_kind, regime_history=hist)
        except Exception:
            return
