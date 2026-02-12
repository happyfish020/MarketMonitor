# -*- coding: utf-8 -*-
"""Shim module for WatchlistLeadBuilder.

Keep backward compatibility for earlier imports:
    from core.watchlist.watchlist_lead_builder import WatchlistLeadBuilder

Canonical implementation lives in:
    core.regime.observation.watchlist.watchlist_lead_builder
"""

from __future__ import annotations

from core.regime.observation.watchlist.watchlist_lead_builder import WatchlistLeadBuilder  # noqa: F401
