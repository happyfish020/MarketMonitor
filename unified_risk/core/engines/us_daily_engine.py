from __future__ import annotations
from typing import Dict, Any

from unified_risk.core.fetchers.us_fetcher import USDataFetcher
from unified_risk.core.factors.us_daily import compute_us_daily
from unified_risk.core.factors.us_short_term import compute_us_short_term
from unified_risk.core.factors.us_mid_term import compute_us_mid_term

def run_us_daily() -> Dict[str, Any]:
    fetcher = USDataFetcher()
    snap = fetcher.get_daily_snapshot()
    short_series = fetcher.get_short_term_series()
    weekly_series = fetcher.get_weekly_series()

    daily = compute_us_daily(snap)
    short = compute_us_short_term(short_series)
    mid = compute_us_mid_term(weekly_series)

    summary = (
        f"[美股日级] {daily.description}\n"
        f"- 短期：{short.description}\n"
        f"- 中期：{mid.description}"
    )

    return {
        "snapshot": snap,
        "daily": daily,
        "short": short,
        "mid": mid,
        "summary": summary,
    }
