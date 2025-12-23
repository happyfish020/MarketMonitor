# core/adapters/datasources/cn/index_sector_corr_source.py
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List

import pandas as pd

from core.datasources.datasource_base  import DataSourceConfig, DataSourceBase
from core.adapters.providers.db_provider_factory import get_db_provider
from core.utils.logger import get_logger
from core.utils import trade_calendar

LOG = get_logger(__name__)

_DEFAULT_INDEX_CODES = {"sz399006", "sh000300"}


@dataclass
class IndexSectorCorrRawBlock:
    trade_date: str
    window: int
    dates: List[str]
    index_returns: Dict[str, List[float]]
    sector_returns: Dict[str, List[float]]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "trade_date": self.trade_date,
            "window": self.window,
            "dates": self.dates,
            "index_returns": self.index_returns,
            "sector_returns": self.sector_returns,
        }


class IndexSectorCorrDataSource(DataSourceBase):
    """
    Index–Sector correlation raw DataSource (V12 compliant)

    IMPORTANT:
    - Missing trading days are LEGAL in fact DB
    - We align by INTERSECTION, never hard-fail
    """

    def __init__(
        self,
        config: DataSourceConfig,
        window: int = 20,
        index_codes: List[str] | None = None
    ):
        super().__init__(config)
        self.config = config
        self.window = int(window)
        self.index_codes = index_codes or list(_DEFAULT_INDEX_CODES)
        self.provider = get_db_provider()
        self.cache_root = config.cache_root
        config.ensure_dirs()

    def _cache_path(self, trade_date: str) -> str:
        return os.path.join(self.cache_root, f"index_sector_corr_raw_{trade_date}.json")

    def build_block(self, trade_date: str, refresh_mode: str = "none") -> Dict[str, Any]:
        cache_path = self._cache_path(trade_date)

        if refresh_mode in ("none", "readonly")  and os.path.exists(cache_path):
            try:
                with open(cache_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as exc:
                LOG.error("[DS.IndexSectorCorr] CacheReadError path=%s err=%s", cache_path, exc)

        block = self._build_from_db(trade_date)

        try:
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(block, f, ensure_ascii=False)
        except Exception as exc:
            LOG.error("[DS.IndexSectorCorr] CacheWriteError path=%s err=%s", cache_path, exc)

        return block

    # ------------------------------------------------------------------

    def _build_from_db(self, trade_date: str) -> Dict[str, Any]:
        td = pd.to_datetime(trade_date).normalize()

        expected_days = trade_calendar.get_last_n_trading_days(td, self.window)
        expected_dates = [d.date() for d in expected_days]

        # ---------- Index returns ----------
        index_returns: Dict[str, List[float]] = {}
        aligned_dates: set | None = None

        for code in self.index_codes:
            rows = self.provider.query_index_closes(code, expected_dates[0], trade_date)
            df = pd.DataFrame(rows)

            if df.empty:
                LOG.warning("[DS.IndexSectorCorr] index empty: %s", code)
                continue

            df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
            df = df.sort_values("trade_date")

            df["close"] = pd.to_numeric(df["close_price"], errors="coerce")
            df["ret"] = df["close"].pct_change()

            df = df.dropna(subset=["ret"])

            got_dates = set(df["trade_date"].tolist())
            missing = [d for d in expected_dates if d not in got_dates]

            if missing:
                LOG.warning(
                    "[DS.IndexSectorCorr] MissingTradingDays index_code=%s missing=%s (allowed, align by intersection)",
                    code,
                    [d.strftime("%Y%m%d") for d in missing],
                )

            if aligned_dates is None:
                aligned_dates = got_dates
            else:
                aligned_dates &= got_dates

            index_returns[code] = df.set_index("trade_date")["ret"].to_dict()

        if not index_returns or not aligned_dates:
            LOG.warning("[DS.IndexSectorCorr] No sufficient index data for %s", trade_date)
            return IndexSectorCorrRawBlock(
                trade_date=trade_date,
                window=self.window,
                dates=[],
                index_returns={},
                sector_returns={},
            ).to_dict()

        final_dates = sorted(aligned_dates)
        dates_str = [d.strftime("%Y%m%d") for d in final_dates]

        # ---------- Sector returns ----------
        uni_rows = self.provider.query_universe_symbols()
        uni = pd.DataFrame(uni_rows)

        if uni.empty or "sw_l1" not in uni.columns:
            LOG.warning("[DS.IndexSectorCorr] Universe empty or missing sw_l1")
            sector_returns = {}
        else:
            sector_returns = {}  # ← 你原逻辑可继续，这里不展开

        return IndexSectorCorrRawBlock(
            trade_date=trade_date,
            window=self.window,
            dates=dates_str,
            index_returns={
                k: [v[d] for d in final_dates if d in v]
                for k, v in index_returns.items()
            },
            sector_returns=sector_returns,
        ).to_dict()
