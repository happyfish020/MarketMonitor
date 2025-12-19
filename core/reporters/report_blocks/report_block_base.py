from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, Any

from core.reporters.report_context import ReportContext
from core.reporters.report_types import ReportBlock


class ReportBlockRendererBase(ABC):
    """
    UnifiedRisk V12 · Phase-3 ONLY

    冻结说明：
    - block_alias 是唯一制度身份
    - block_id 已废弃（不参与排序 / 语义）
    """

    block_alias: str
    title: str

    @abstractmethod
    def render(
        self,
        context: ReportContext,
        doc_partial: Dict[str, Any],
    ) -> ReportBlock:
        raise NotImplementedError
