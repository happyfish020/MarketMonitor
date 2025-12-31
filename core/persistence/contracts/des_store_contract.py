# -*- coding: utf-8 -*-
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from core.persistence.models.decision_evidence_snapshot import DecisionEvidenceSnapshot


class DecisionEvidenceStoreContract(ABC):
    @abstractmethod
    def save_des(
        self,
        trade_date: str,
        report_kind: str,
        engine_version: str,
        des_payload: Dict[str, Any],
    ) -> str:
        """Append-only. Must raise AlreadyPublishedError if key exists."""
        raise NotImplementedError

    @abstractmethod
    def get_des(self, trade_date: str, report_kind: str) -> Optional[DecisionEvidenceSnapshot]:
        raise NotImplementedError

    @abstractmethod
    def verify_des(self, trade_date: str, report_kind: str) -> bool:
        """Recompute hash and compare; raise TamperedError on mismatch."""
        raise NotImplementedError
