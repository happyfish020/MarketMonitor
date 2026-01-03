# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 · Report Block Loader

Load config/report_blocks.yaml and build ReportEngine.block_builders dict in YAML order.

Frozen principles:
- allowlist via REPORT_BLOCK_REGISTRY
- missing/invalid config => fallback to minimal safe default
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import os

try:
    import yaml
except Exception:
    yaml = None

from core.reporters.report_block_registry import REPORT_BLOCK_REGISTRY


def _safe_yaml_load(path: str) -> Optional[Dict[str, Any]]:
    if yaml is None:
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _resolve_path(path: str) -> str:
    # try as-is
    if os.path.isabs(path) and os.path.exists(path):
        return path
    if os.path.exists(path):
        return path
    # try relative to CWD / config
    alt = os.path.join(os.getcwd(), path)
    if os.path.exists(alt):
        return alt
    return path  # best-effort


def load_block_builders(report_kind: str, config_path: str) -> Dict[str, Any]:
    """
    Returns a dict: {alias: renderer.render}
    Note: dict insertion order is preserved (Python 3.7+).
    """
    resolved = _resolve_path(config_path)
    cfg = _safe_yaml_load(resolved) or {}

    reports = cfg.get("reports") if isinstance(cfg.get("reports"), dict) else {}
    kind_cfg = reports.get(report_kind) if isinstance(reports.get(report_kind), dict) else None
    if kind_cfg is None:
        # fallback: try upper-case key
        kind_cfg = reports.get(str(report_kind).upper()) if isinstance(reports.get(str(report_kind).upper()), dict) else None

    blocks = kind_cfg.get("blocks") if isinstance(kind_cfg, dict) else None
    if not isinstance(blocks, list):
        # minimal safe default
        blocks = [
            {"alias": "structure.facts", "id": "structure_facts"},
            {"alias": "summary", "id": "summary_a_n_d"},
            {"alias": "governance.gate", "id": "gate_decision"},
            {"alias": "execution.summary", "id": "execution_summary"},
            {"alias": "exit.readiness", "id": "exit_readiness"},
        ]

    out: Dict[str, Any] = {}
    for b in blocks:
        if not isinstance(b, dict):
            continue
        alias = b.get("alias")
        bid = b.get("id")
        if not isinstance(alias, str) or not isinstance(bid, str):
            continue
        cls = REPORT_BLOCK_REGISTRY.get(bid)
        if cls is None:
            continue
        try:
            inst = cls()
            out[alias] = inst.render
        except Exception:
            # frozen: skip bad block
            continue
    return out
