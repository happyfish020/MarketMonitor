# -*- coding: utf-8 -*-
"""UnifiedRisk V12 · Run → Persist · Replay/Audit closure (Frozen)

This module defines a minimal storage interface for *replayable* persisted runs.

Design constraints (Frozen Engineering):
- Keep the interface small and stable.
- No business logic here.
- No silent fallbacks: missing payloads should surface explicitly in adapters.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol

ReportDump = Dict[str, Any]


@dataclass(frozen=True)
class RunPayload:
    """A single persisted run payload for replay/audit.

    Notes:
    - snapshot_raw / factor_result / gate_decision are L1 (technical) artifacts.
    - report_dump is L2 (institutional) artifact (e.g. {des_payload, rendered}).
    - slots_final is optional: when available, it should match the slots used to
      render report blocks (often == des_payload for V12).
    """

    run_id: str
    trade_date: str
    kind: str

    # schema version of the persisted payload (adapter-defined, but stable)
    schema_version: str

    # engine version used when the run was created (if available)
    engine_version: Optional[str]

    # L1 artifacts
    snapshot_raw: Dict[str, Any]
    factor_result: Dict[str, Any]
    gate_decision: Optional[Dict[str, Any]]

    # Optional canonical slots (for rendering)
    slots_final: Optional[Dict[str, Any]]

    # L2 artifact (stored report dump / stored rendered text, adapter-defined)
    report_dump: Optional[ReportDump]


class RunStore(Protocol):
    """Adapter interface: your persistence layer → RunPayload."""

    def load_run(self, run_id: str) -> RunPayload:
        ...

    def find_runs(
        self,
        trade_date: Optional[str] = None,
        kind: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Return a list of dict summaries.

        Each dict MUST contain:
        - run_id
        - trade_date
        - kind

        Recommended (if available):
        - schema_version
        - created_at (utc timestamp or ISO string)
        - engine_version
        - status
        """
        ...
