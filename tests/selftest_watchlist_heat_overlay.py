"""Selftest: WatchlistLead Heat Overlay (observation-only)

Run:
  python -m tests.selftest_watchlist_heat_overlay

This test does NOT require DB. It exercises WatchlistLeadFactor only.
"""

from __future__ import annotations

from typing import Any, Dict

from core.factors.cn.watchlist_lead_factor import WatchlistLeadFactor


def _mk_market_sentiment_raw_ok() -> Dict[str, Any]:
    return {
        "schema_version": "market_sentiment_raw_v1",
        "asof": {"trade_date": "2026-01-09", "kind": "EOD"},
        "data_status": "OK",
        "warnings": [],
        "error_type": None,
        "error_message": None,
        "evidence": {
            "total_stocks": 5180,
            "adv": 3715,
            "dec": 1460,
            "flat": 5,
            "limit_up": 152,
            "limit_down": 2,
            "broken_limit_rate_std": 0.3286,
            "max_consecutive_limit_up": 8,
            "stuck_locked_ratio_pct": 0.14,
        },
    }


def _mk_breadth_plus_raw_ok() -> Dict[str, Any]:
    return {
        "schema_version": "breadth_plus_raw_v1",
        "asof": {"trade_date": "2026-01-09", "kind": "EOD"},
        "data_status": "OK",
        "warnings": [],
        "error_type": None,
        "error_message": None,
        "evidence": {
            "pct_above_ma20_pct": 85.72,
            "pct_above_ma50_pct": 67.42,
            "new_high_low_ratio": 29.06,
            "new_low_ratio_pct": 0.62,
            "ad_slope_10d": 165.79,
        },
    }


def _run_case_ok() -> None:
    factor = WatchlistLeadFactor()

    input_block = {
        "watchlist_lead_input_raw": {
            "schema_version": "watchlist_lead_input_raw_v1",
            "asof": {"trade_date": "2026-01-09", "kind": "EOD"},
            "data_status": "OK",
            "warnings": [],
            "error_type": None,
            "error_message": None,
            "lead_raw": {"items": {}},
            "market_sentiment_raw": _mk_market_sentiment_raw_ok(),
            "breadth_plus_raw": _mk_breadth_plus_raw_ok(),
            # placeholders
            "supply_raw": {"items": {}},
            "internals_raw": {"items": {}},
        }
    }

    fr = factor.compute(input_block)

    details = fr.details or {}
    assert isinstance(details, dict)
    assert isinstance(details.get("lead_panels"), dict), "lead_panels missing"
    assert isinstance(details.get("tplus2_lead"), dict), "tplus2_lead missing"

    # Heat overlay must apply
    ov_all = details.get("bucket_overlays")
    assert isinstance(ov_all, dict) and isinstance(ov_all.get("heat_overlay"), dict), "bucket_overlays.heat_overlay missing"
    hov = ov_all["heat_overlay"]
    assert hov.get("applied") is True
    assert hov.get("cap_code") == "CAPPED_BY_HEAT_RED"

    groups = details.get("groups")
    assert isinstance(groups, dict), "groups missing"

    for bk in ("BUCKET_AI", "BUCKET_NE", "BUCKET_HARDTECH"):
        g = groups.get(bk)
        assert isinstance(g, dict), f"group {bk} missing"
        lv_disp = str(g.get("level_display") or "")
        assert "CAPPED_BY_HEAT_RED" in lv_disp, f"{bk} level_display not capped: {lv_disp}"
        aa = g.get("action_allowed")
        af = g.get("action_forbidden")
        assert isinstance(aa, list) and "HOLD" in aa and "TRIM_ON_STRENGTH" in aa
        assert isinstance(af, list) and "ADD_RISK" in af


def _run_case_missing() -> None:
    factor = WatchlistLeadFactor()

    input_block = {
        "watchlist_lead_input_raw": {
            "schema_version": "watchlist_lead_input_raw_v1",
            "asof": {"trade_date": "2026-01-09", "kind": "EOD"},
            "data_status": "OK",
            "warnings": [],
            "error_type": None,
            "error_message": None,
            "lead_raw": {"items": {}},
            # market_sentiment_raw missing on purpose
            "breadth_plus_raw": _mk_breadth_plus_raw_ok(),
            "supply_raw": {"items": {}},
            "internals_raw": {"items": {}},
        }
    }

    fr = factor.compute(input_block)

    details = fr.details or {}
    assert isinstance(details, dict)
    panels = details.get("lead_panels")
    assert isinstance(panels, dict)
    a = panels.get("market_sentiment")
    assert isinstance(a, dict)
    assert a.get("data_status") in ("MISSING", "PARTIAL", "ERROR")

    # Heat overlay must NOT apply
    ov_all = details.get("bucket_overlays")
    if isinstance(ov_all, dict) and isinstance(ov_all.get("heat_overlay"), dict):
        assert ov_all["heat_overlay"].get("applied") is not True


def main() -> None:
    _run_case_ok()
    _run_case_missing()
    print("PASS: selftest_watchlist_heat_overlay")


if __name__ == "__main__":
    main()
