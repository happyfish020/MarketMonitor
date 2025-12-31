# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - Persistence Errors (Frozen v1)

Rules:
- Exceptions express control-flow semantics, not data storage.
- No mutable fields on frozen dataclasses.
- Use Python exception chaining: `raise X(...) from e`.
"""

from __future__ import annotations

from dataclasses import dataclass


# ----------------------------------------------------------------------
# Base
# ----------------------------------------------------------------------

class PersistenceError(RuntimeError):
    """Base class for all persistence-layer errors."""
    pass


# ----------------------------------------------------------------------
# L2 (Institutional) Errors
# ----------------------------------------------------------------------

@dataclass(frozen=True)
class AlreadyPublishedError(PersistenceError):
    trade_date: str
    report_kind: str

    def __str__(self) -> str:
        return f"Already published for {self.trade_date} / {self.report_kind}"


@dataclass(frozen=True)
class TamperedError(PersistenceError):
    entity: str
    trade_date: str
    report_kind: str

    def __str__(self) -> str:
        return (
            f"Tampered detected for {self.entity} "
            f"{self.trade_date} / {self.report_kind}"
        )


@dataclass(frozen=True)
class InvalidPayloadError(PersistenceError):
    entity: str
    reason: str

    def __str__(self) -> str:
        return f"Invalid payload for {self.entity}: {self.reason}"


# ----------------------------------------------------------------------
# L1 (Run Persistence) Errors
# ----------------------------------------------------------------------

@dataclass(frozen=True)
class AlreadyRecordedError(PersistenceError):
    entity: str
    run_id: str

    def __str__(self) -> str:
        return f"Already recorded: {self.entity} for run_id={self.run_id}"


@dataclass(frozen=True)
class RunNotFoundError(PersistenceError):
    run_id: str

    def __str__(self) -> str:
        return f"Run not found: run_id={self.run_id}"
