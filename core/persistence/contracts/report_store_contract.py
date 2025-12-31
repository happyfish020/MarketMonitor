# -*- coding: utf-8 -*-
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from core.persistence.models.report_artifact import ReportArtifact


class ReportStoreContract(ABC):
    @abstractmethod
    def save_report(
        self,
        trade_date: str,
        report_kind: str,
        content_text: str,
        meta: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Append-only. Must raise AlreadyPublishedError if key exists."""
        raise NotImplementedError

    @abstractmethod
    def get_report(self, trade_date: str, report_kind: str) -> Optional[ReportArtifact]:
        raise NotImplementedError

    @abstractmethod
    def verify_report(self, trade_date: str, report_kind: str) -> bool:
        """Recompute hash and compare; raise TamperedError on mismatch."""
        raise NotImplementedError
