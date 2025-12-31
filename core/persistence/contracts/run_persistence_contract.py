# -*- coding: utf-8 -*-
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class RunPersistenceContract(ABC):
    @abstractmethod
    def start_run(
        self,
        trade_date: str,
        report_kind: str,
        engine_version: str,
        started_at_utc: Optional[int] = None,
    ) -> str:
        """Start a run and return run_id."""
        raise NotImplementedError

    @abstractmethod
    def record_snapshot(
        self,
        run_id: str,
        source_name: str,
        payload: Dict[str, Any],
        seq: Optional[int] = None,
        created_at_utc: Optional[int] = None,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def record_factor(
        self,
        run_id: str,
        factor_name: str,
        payload: Dict[str, Any],
        factor_version: Optional[str] = None,
        seq: Optional[int] = None,
        created_at_utc: Optional[int] = None,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def record_gate(
        self,
        run_id: str,
        gate: str,
        drs: str,
        frf: str,
        action_hint: Optional[str] = None,
        rule_hits: Optional[Dict[str, Any]] = None,
        created_at_utc: Optional[int] = None,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def finish_run(
        self,
        run_id: str,
        status: str,
        finished_at_utc: Optional[int] = None,
        error_type: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
        raise NotImplementedError
