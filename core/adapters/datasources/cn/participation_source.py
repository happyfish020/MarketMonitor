# core/adapters/datasources/cn/participation_source.py
# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - ParticipationDataSource (CN A-Share)

鑱岃矗锛堢敓浜у悜锛屾瀬绠€锛夛細
- 浠庢湰鍦?Oracle DB 璇诲彇鍏ㄥ競鍦鸿偂绁ㄦ棩绾?close锛堜簨瀹炲眰锛?
- 璁＄畻褰撴棩妯埅闈㈠弬涓庤川閲忔寚鏍囷細
    - adv_ratio锛堜笂娑ㄥ鏁板崰姣旓級
    - median_return锛堜釜鑲℃敹鐩婁腑浣嶆暟锛?
    - index_return锛堟寚鏁版敹鐩婏紝鐢ㄤ簬瀵圭収锛氶粯璁?HS300=sh000300锛?
- 杈撳嚭涓?snapshot["participation"] 鐨勬爣鍑嗚緭鍏ュ潡锛堜笉鍋氱姸鎬佸垽瀹氾級

娉ㄦ剰锛?
- 鍋滅墝/缂哄け鏁版嵁鍚堟硶锛氬崟鍙偂绁ㄧ己 prev_close 鍒欒烦杩?
- 涓嶅仛鏀剁泭鍥炴祴锛屼笉鍋氫氦鏄撲俊鍙?
"""

from __future__ import annotations

import os
import json
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from core.utils.logger import get_logger
from core.datasources.datasource_base import DataSourceConfig, DataSourceBase
from core.utils.ds_refresh import apply_refresh_cleanup
from core.adapters.providers.db_provider_mysql_market import DBOracleProvider


LOG = get_logger("DS.Participation")


def _parse_date(s: str) -> datetime:
    # trade_date 鍏佽 'YYYY-MM-DD' / 'YYYYMMDD'
    s = str(s).strip()
    if len(s) == 8 and s.isdigit():
        return datetime.strptime(s, "%Y%m%d")
    return datetime.strptime(s, "%Y-%m-%d")


class ParticipationDataSource(DataSourceBase):
    """
    杈撳嚭鍧楃粨鏋勶紙snapshot["participation"]锛夛細

    {
      "trade_date": "YYYY-MM-DD",
      "index_code": "sh000300",
      "adv": int,
      "dec": int,
      "flat": int,
      "adv_ratio": float,
      "median_return": float,
      "index_return": float,
      "n_effective": int,
      "_raw_data": {...鍙€夎皟璇?..}
    }
    """

    def __init__(self, cfg: DataSourceConfig, index_code: str = "sh000300", lookback_days: int = 10):
        super().__init__(name="DS.Participation")
        self.cfg = cfg
        self.cfg.ensure_dirs()

        self.index_code = index_code
        self.lookback_days = int(lookback_days)

        self.db = DBOracleProvider()

        LOG.info(
            "[DS.Participation] init ok. market=%s ds=%s cache=%s index=%s lookback_days=%s",
            cfg.market,
            cfg.ds_name,
            cfg.cache_root,
            self.index_code,
            self.lookback_days,
        )

    def _cache_path(self, trade_date: str) -> str:
        td = _parse_date(trade_date).strftime("%Y-%m-%d")
        return os.path.join(self.cfg.cache_root, f"participation_{td}.json")

    @staticmethod
    def _calc_last2_returns(df: pd.DataFrame) -> pd.Series:
        """
        df columns: symbol, trade_date, close
        return: Series(index=symbol, value=ret)
        """
        if df is None or df.empty:
            return pd.Series(dtype="float64")

        df = df.copy()
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df = df.sort_values(["symbol", "trade_date"])

        # 鍙栨瘡涓?symbol 鏈€鍚庝袱琛岋紙涓嶈冻涓よ鐨勪細鍦?ret_one 閲岃杩囨护锛?
        last2 = df.groupby("symbol", sort=False).tail(2)

        def _ret_one(x: pd.DataFrame) -> Optional[float]:
            if x.shape[0] < 2:
                return None
            c1 = float(x.iloc[-1]["close"])
            c0 = float(x.iloc[-2]["close"])
            if c0 == 0:
                return None
            return c1 / c0 - 1.0

        #rets = last2.groupby("symbol", sort=False).apply(_ret_one)
        #
        rets = last2.groupby("symbol", sort=False, group_keys=False).apply(_ret_one, include_groups=False)

        rets = rets.dropna().astype(float)
        return rets

    @staticmethod
    def _normalize_stock_df(df: pd.DataFrame) -> pd.DataFrame:
        """Normalize stock close rows to expected columns: symbol/trade_date/close."""
        if df is None or df.empty:
            return pd.DataFrame(columns=["symbol", "trade_date", "close"])

        out = df.copy()
        out.columns = [str(c).strip().lower() for c in out.columns]

        # Positional fallback for tuple-style rows from SQLAlchemy
        if "symbol" not in out.columns and 0 in df.columns:
            out["symbol"] = df.iloc[:, 0]
        if "trade_date" not in out.columns and out.shape[1] >= 3:
            out["trade_date"] = df.iloc[:, 2]
        if "close" not in out.columns and out.shape[1] >= 6:
            out["close"] = df.iloc[:, 5]

        keep = [c for c in ("symbol", "trade_date", "close") if c in out.columns]
        return out[keep].copy() if keep else pd.DataFrame(columns=["symbol", "trade_date", "close"])

    @staticmethod
    def _normalize_index_df(df: pd.DataFrame) -> pd.DataFrame:
        """Normalize index close rows to expected columns: trade_date/close."""
        if df is None or df.empty:
            return pd.DataFrame(columns=["trade_date", "close"])

        out = df.copy()
        out.columns = [str(c).strip().lower() for c in out.columns]

        # Positional fallback for tuple-style rows from SQLAlchemy
        if "trade_date" not in out.columns and out.shape[1] >= 2:
            out["trade_date"] = df.iloc[:, 1]
        if "close" not in out.columns and out.shape[1] >= 3:
            out["close"] = df.iloc[:, 2]

        keep = [c for c in ("trade_date", "close") if c in out.columns]
        return out[keep].copy() if keep else pd.DataFrame(columns=["trade_date", "close"])

    def build_block(self, trade_date: str, refresh_mode: str = "auto") -> Dict[str, Any]:
        cache_file = self._cache_path(trade_date)

        # refresh 鎺у埗锛堜粎 cache锛?
        _ = apply_refresh_cleanup(
            refresh_mode=refresh_mode,
            cache_path=cache_file,
            history_path=None,
            spot_path=None,
        )
        
        refresh_mode = "full"
        # cache 鍛戒腑
        if refresh_mode in ("none", "readonly")  and os.path.exists(cache_file):
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                LOG.warning("[DS.Participation] cache load failed, fallback to db. err=%s", e)

        td_dt = _parse_date(trade_date)
        window_start = (td_dt - timedelta(days=self.lookback_days)).strftime("%Y-%m-%d")
        td_str = td_dt.strftime("%Y-%m-%d")

        # ---- 1) stocks close window ----
        try:
            stock_df = self.db.query_stock_closes(window_start=window_start, trade_date=td_str)
        except Exception as e:
            LOG.error("[DS.Participation] db query_stock_closes failed: %s", e, exc_info=True)
            raise

        if not isinstance(stock_df, pd.DataFrame):
            stock_df = pd.DataFrame(stock_df)
        stock_df = self._normalize_stock_df(stock_df)

        # 闃插尽锛氫粎淇濈暀 <= trade_date
        if "trade_date" in stock_df.columns:
            stock_df["trade_date"] = pd.to_datetime(stock_df["trade_date"])
            stock_df = stock_df[stock_df["trade_date"] <= pd.to_datetime(td_str)]

        rets = self._calc_last2_returns(stock_df)

        # ---- 2) index return (HS300 default) ----
        try:
            idx_df = self.db.query_index_closes(index_code=self.index_code, window_start=window_start, trade_date=td_str)
        except Exception as e:
            LOG.error("[DS.Participation] db query_index_closes failed: %s", e, exc_info=True)
            raise

        if not isinstance(idx_df, pd.DataFrame):
            idx_df = pd.DataFrame(idx_df)
        idx_df = self._normalize_index_df(idx_df)

        idx_ret = 0.0
        if not idx_df.empty and "trade_date" in idx_df.columns:
            idx_df["trade_date"] = pd.to_datetime(idx_df["trade_date"])
            idx_df = idx_df.sort_values("trade_date")
            if idx_df.shape[0] >= 2:
                c1 = float(idx_df.iloc[-1]["close"])
                c0 = float(idx_df.iloc[-2]["close"])
                idx_ret = (c1 / c0 - 1.0) if c0 else 0.0
            else:
                LOG.warning("[DS.Participation] index has <2 points. index_code=%s trade_date=%s", self.index_code, td_str)

        # ---- 3) cross-sectional stats ----
        adv = int((rets > 0).sum())
        dec = int((rets < 0).sum())
        flat = int((rets == 0).sum())
        denom = adv + dec
        adv_ratio = float(adv / denom) if denom > 0 else 0.0
        median_ret = float(np.median(rets.values)) if len(rets) else 0.0

        block: Dict[str, Any] = {
            "trade_date": td_str,
            "index_code": self.index_code,
            "adv": adv,
            "dec": dec,
            "flat": flat,
            "adv_ratio": adv_ratio,
            "median_return": median_ret,
            "index_return": float(idx_ret),
            "n_effective": int(len(rets)),
            "_raw_data": {
                "window_start": window_start,
                "universe_effective": int(len(rets)),
                "ret_sample": rets.head(5).to_dict(),
            },
        }

        # cache 鍐欏叆
        try:
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(block, f, ensure_ascii=False)
        except Exception as e:
            LOG.warning("[DS.Participation] cache write failed: %s", e)

        return block

