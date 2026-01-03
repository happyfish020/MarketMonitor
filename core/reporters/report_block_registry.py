# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 · Report Block Registry (Frozen)

Goal:
- Allow ReportEngine to be configured by YAML (block order/selection) without editing code for every change.
- Keep it safe: only allow known blocks (allowlist).

Notes:
- Registry keys are stable ids used in config/report_blocks.yaml
- Each entry maps to a renderer class (must implement .render(context, doc_partial) -> ReportBlock)
"""

from __future__ import annotations

from typing import Dict, Type

from core.reporters.report_blocks.structure_facts_blk import StructureFactsBlock
from core.reporters.report_blocks.summary_a_n_d_blk import SummaryANDBlock
from core.reporters.report_blocks.execution_summary_blk import ExecutionSummaryBlock
from core.reporters.report_blocks.exit_readiness_blk import ExitReadinessBlock
from core.reporters.report_blocks.gate_decision_blk import GateDecisionBlock


REPORT_BLOCK_REGISTRY: Dict[str, Type] = {
    "structure_facts": StructureFactsBlock,
    "summary_a_n_d": SummaryANDBlock,
    "execution_summary": ExecutionSummaryBlock,
    "exit_readiness": ExitReadinessBlock,
    "gate_decision": GateDecisionBlock,
}
