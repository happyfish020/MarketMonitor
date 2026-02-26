# -*- coding: utf-8 -*-
# UnifiedRisk V12 - Watchlist Lead Input BlockBuilder (Multi-DS Join)
#
# 作用（冻结/只读）：
# - 将 watchlist_lead_raw + watchlist_supply_raw 组合为单一输入块：watchlist_lead_input_raw
# - 不做业务解释/打分（这些必须放在 Factor）
# - 永不抛异常：缺块 -> data_status=PARTIAL/MISSING + warnings（禁止 silent exception）

from __future__ import annotations

import os
from typing import Dict, Any, List, Optional

from core.adapters.block_builder.block_builder_base import FactBlockBuilderBase
from core.utils.logger import get_logger

LOG = get_logger("TR.WatchlistLeadBB")


class WatchlistLeadBlockBuilder(FactBlockBuilderBase):
    """Build combined input block for WatchlistLeadFactor.

    Input (read-only from snapshot):
      - snapshot["watchlist_lead_raw"]
      - snapshot["watchlist_supply_raw"]

    Output (write back by fetcher):
      - snapshot["watchlist_lead_input_raw"]
    """

    def __init__(self):
        super().__init__(name="WatchlistLeadInput")

    def build_block(self, snapshot: Dict[str, Any] ) -> Dict[str, Any]:
        warnings: List[str] = []
        schema = "WL_LEAD_INPUT_RAW_MVP_2026Q1"

        trade_date = snapshot.get("trade_date")
        asof = {"trade_date": str(trade_date) if trade_date else None, "kind": snapshot.get("kind")}

        lead_raw = snapshot.get("watchlist_lead_raw")
        supply_raw = snapshot.get("watchlist_supply_raw")

        # Leading-Structure DataPack v1 (append-only)
        # Note: raw blocks may be missing in early stage; never raise.
        internals_raw = snapshot.get("market_sentiment_raw") or snapshot.get("market_internals_raw")
        breadth_plus_raw = snapshot.get("breadth_plus_raw")
        liquidity_quality_raw = snapshot.get("liquidity_quality_raw")
        margin_intensity_raw = snapshot.get("margin_intensity_raw")
        
        # compat fallback: if margin_intensity_raw missing, try margin_raw (T-1 is acceptable; must not mark missing)
        if not (isinstance(margin_intensity_raw, dict) and bool(margin_intensity_raw)):
            _mr = snapshot.get("margin_raw")
            if isinstance(_mr, dict) and bool(_mr):
                margin_intensity_raw = _mr
                warnings.append("fallback:margin_intensity_raw_from:margin_raw")
        
        etf_flow_raw = snapshot.get("etf_flow_raw")
        futures_basis_raw = snapshot.get("futures_basis_raw")
        options_risk_raw = snapshot.get("options_risk_raw")

        def _is_ok_block(b: Any) -> bool:
            if not (isinstance(b, dict) and bool(b)):
                return False
            ds = str(b.get("data_status") or "").upper()
            # MISSING/ERROR blocks are placeholders; treat as not-ok for panel join.
            if ds in {"MISSING", "ERROR"}:
                return False
            return True

        lead_ok = _is_ok_block(lead_raw)
        supply_ok = _is_ok_block(supply_raw)

        internals_ok = _is_ok_block(internals_raw)
        breadth_plus_ok = _is_ok_block(breadth_plus_raw)
        liquidity_quality_ok = _is_ok_block(liquidity_quality_raw)
        margin_intensity_ok = _is_ok_block(margin_intensity_raw)
        etf_flow_ok = _is_ok_block(etf_flow_raw)
        futures_basis_ok = _is_ok_block(futures_basis_raw)
        options_risk_ok = _is_ok_block(options_risk_raw)

        if not lead_ok:
            warnings.append("missing:watchlist_lead_raw")
        if not supply_ok:
            warnings.append("missing:watchlist_supply_raw")

        if not internals_ok:
            warnings.append("missing:market_sentiment_raw")
        if not breadth_plus_ok:
            warnings.append("missing:breadth_plus_raw")
        if not liquidity_quality_ok:
            warnings.append("missing:liquidity_quality_raw")
        if not margin_intensity_ok:
            warnings.append("missing:margin_intensity_raw")
        if not etf_flow_ok:
            warnings.append("missing:etf_flow_raw")
        if not futures_basis_ok:
            warnings.append("missing:futures_basis_raw")
        if not options_risk_ok:
            warnings.append("missing:options_risk_raw")

        # data_status: OK / PARTIAL / MISSING / ERROR (best-effort)
        data_status = "OK"
        # minimal rollup: require lead_raw as base; missing others => PARTIAL
        any_ok = any([lead_ok, supply_ok, internals_ok, breadth_plus_ok, liquidity_quality_ok, margin_intensity_ok, etf_flow_ok, futures_basis_ok, options_risk_ok])
        if not any_ok:
            data_status = "MISSING"
        elif not lead_ok:
            data_status = "PARTIAL"
        elif not supply_ok or not internals_ok:
            data_status = "PARTIAL"
        # propagate upstream ERROR if any joined block explicitly declares ERROR
        for _k, _b in [
            ("lead", lead_raw), ("supply", supply_raw), ("internals", internals_raw),
            ("breadth_plus", breadth_plus_raw), ("liquidity_quality", liquidity_quality_raw),
            ("margin_intensity", margin_intensity_raw), ("etf_flow", etf_flow_raw),
            ("futures_basis", futures_basis_raw), ("options_risk", options_risk_raw),
        ]:
            if isinstance(_b, dict) and _b.get("data_status") == "ERROR":
                data_status = "ERROR"
                warnings.append(f"upstream_error:{_k}")


        block = {
            "schema": schema,
            "asof": asof,
            "data_status": data_status,
            "warnings": warnings,
            "meta": {
                "contribute_to_market_score": False,
                "source": "block_builder",
                "joined": {"lead": bool(lead_ok), "supply": bool(supply_ok), "internals": bool(internals_ok), "breadth_plus": bool(breadth_plus_ok), "liquidity_quality": bool(liquidity_quality_ok), "margin_intensity": bool(margin_intensity_ok), "etf_flow": bool(etf_flow_ok), "futures_basis": bool(futures_basis_ok), "options_risk": bool(options_risk_ok)},
            },
            "lead_raw": lead_raw if isinstance(lead_raw, dict) else {},
            "supply_raw": supply_raw if isinstance(supply_raw, dict) else {},
            "internals_raw": internals_raw if isinstance(internals_raw, dict) else {},
            "breadth_plus_raw": breadth_plus_raw if isinstance(breadth_plus_raw, dict) else {},
            "liquidity_quality_raw": liquidity_quality_raw if isinstance(liquidity_quality_raw, dict) else {},
            "margin_intensity_raw": margin_intensity_raw if isinstance(margin_intensity_raw, dict) else {},
            "etf_flow_raw": etf_flow_raw if isinstance(etf_flow_raw, dict) else {},
            "futures_basis_raw": futures_basis_raw if isinstance(futures_basis_raw, dict) else {},
            "options_risk_raw": options_risk_raw if isinstance(options_risk_raw, dict) else {},

        }

        if warnings:
            LOG.warning("[WatchlistLeadBB] build_block warnings=%s", ",".join(warnings))
        
        import json
        os.makedirs(r"run\temp", exist_ok=True)
        with open(r"run\temp\watch.json", "w", encoding="utf-8") as f:
            json.dump(block, f, ensure_ascii=False, indent=2)
         


        return block
