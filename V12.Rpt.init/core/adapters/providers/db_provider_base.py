from __future__ import annotations

"""DB Provider Base (UnifiedRisk V12)

This provider family is dedicated to local DB access (Oracle/MySQL) for
historical time series used by Phase-2 structural pillars (e.g., Breadth,
Index–Sector Correlation).

Design constraints:
    - Providers must ONLY handle DB connection + SQL execution.
    - Business logic (joins, aggregation, correlation, scoring) MUST live in
      DataSource/BlockBuilder/Factor layers.
    - This project does NOT include ETL that writes into DB.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable, List, Sequence, Tuple


class DBProviderBase(ABC):
    """Abstract base for DB providers."""

    @abstractmethod
    def query_stock_closes(
        self,
        *,
        window_start: str,
        trade_date: str,
    ) -> Sequence[Tuple[str, str, Any, float]]:
        """Return rows: (symbol, exchange, trade_date, close)."""

    @abstractmethod
    def query_index_closes(
        self,
        *,
        index_code: str,
        window_start: str,
        trade_date: str,
    ) -> Sequence[Tuple[str, Any, float]]:
        """Return rows: (index_code, trade_date, close)."""

    @abstractmethod
    def query_universe_symbols(self) -> Sequence[Tuple[str, str, str]]:
        """Return rows: (symbol, exchange, sw_l1)."""
