# core/adapters/datasources/providers/provider_yf.py
# UnifiedRisk V12 — Yahoo Finance Provider (full version)

from __future__ import annotations
import pandas as pd
import traceback
import time
import random
import yfinance as yf

from core.adapters.providers.provider_base import ProviderBase
from core.utils.logger import get_logger

LOG = get_logger("Provider.YF")


class YFProvider(ProviderBase):
    """
    UnifiedRisk V12: YFProvider
    ------------------------------------------------------------
    - 继承 ProviderBase
    - 支持多个子方法 (index / equity / future / crypto / default)
    - 针对 yfinance 输出进行 dataframe 标准化
    """

    def __init__(self):
        super().__init__("yf")

    # =====================================================================
    # [核心方法] 覆写抽象方法：fetch_series_raw()
    # =====================================================================
    def fetch_series_raw(self, symbol: str, window: int, method: str = "default"):
        """
        ProviderBase 要求子类必须实现的底层数据获取方法。

        method 来自 symbols.yaml：
            method: "index"
            method: "equity"
            method: "future"
            method: "crypto"
            method: "default"
        """

        LOG.info(f"[YFProvider] Fetch symbol={symbol}, method={method}, window={window}")

        try:
            # 路由到子方法，如 _fetch_index_method()
            method_name = f"_fetch_{method}_method" if method else "default"

            if method_name == "default":
                return self._fetch_default_method(symbol, window)
            elif hasattr(self, method_name):   
                LOG.info(f"[YFProvider] Using YF submethod: {method_name} for symbol={symbol}")
                return getattr(self, method_name)(symbol, window)

            # fallback
            LOG.warning(f"[YFProvider] No such method={method}, using default")
            #raise Exception(f"No such method={method},{symbol}") #debug
            return self._fetch_default_method(symbol, window)

        except Exception as e:
            LOG.error(f"[YFProvider] fetch_series_raw fatal: symbol={symbol}, error={e}")
            traceback.print_exc()
            return None

    # =====================================================================
    # 子方法统一走 _fetch_yf_raw()，只是为了扩展性拆开
    # =====================================================================
    def _fetch_default_method(self, symbol: str, window: int):
        return self._fetch_yf_raw(symbol, window)

    def _fetch_index_method(self, symbol: str, window: int):
        return self._fetch_yf_raw(symbol, window)

    def _fetch_equity_method(self, symbol: str, window: int):
        return self._fetch_yf_raw(symbol, window)

    def _fetch_future_method(self, symbol: str, window: int):
        return self._fetch_yf_raw(symbol, window)

    def _fetch_crypto_method(self, symbol: str, window: int):
        return self._fetch_yf_raw(symbol, window)

    # =====================================================================
    # 实际 YF 抓取底层逻辑
    # =====================================================================
    def _fetch_yf_raw(self, symbol: str, window: int) -> pd.DataFrame:
        LOG.info(f"[YFProvider] _fetch_yf_raw: {symbol}, window={window}")
       
        try:
            df = self._download_with_retry(symbol=symbol, window=window)

            if df is None or df.empty:
                LOG.warning(f"[YFProvider] Empty df for symbol={symbol}")
                return None

            # yfinance 的列名有时是 MultiIndex，需要 flatten
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [c[0].lower() for c in df.columns]
            else:
                df.columns = [c.lower() for c in df.columns]

            # reset index → 日期列通常为“Date”
            df = df.reset_index()

            # 修复 date 列
            if "Date" in df.columns:
                df.rename(columns={"Date": "date"}, inplace=True)
            if "date" not in df.columns and "index" in df.columns:
                df.rename(columns={"index": "date"}, inplace=True)

            # 若字段不完整，ProviderBase.normalize_df 会进一步补全
            return df

        except Exception as e:
            LOG.error(f"[YFProvider] _fetch_yf_raw fatal for symbol={symbol}: {e}")
            traceback.print_exc()
            return None

    # =====================================================================
    # Retry wrapper for yfinance download
    # =====================================================================
    def _download_with_retry(
        self,
        symbol: str,
        window: int,
        max_attempts: int = 4,
        base_sleep_sec: float = 0.8,
        max_sleep_sec: float = 12.0,
    ) -> pd.DataFrame | None:
        """Download with retries.

        Why:
        - yfinance occasionally fails with transient network errors (curl:56, connection reset, etc.)
        - sometimes yfinance prints error and returns empty df without raising

        Strategy:
        - exponential backoff + jitter
        - retry on exception OR empty df
        """
        last_exc: Exception | None = None

        # Avoid yfinance multi-thread download to reduce connection resets
        for attempt in range(1, max_attempts + 1):
            try:
                df = yf.download(
                    symbol,
                    period=f"{window}d",
                    auto_adjust=False,
                    progress=False,
                    threads=False,
                )

                if df is not None and not df.empty:
                    if attempt > 1:
                        LOG.info(f"[YFProvider] Download recovered after retry: symbol={symbol} attempt={attempt}")
                    return df

                # Empty df: treat as retryable (common when yfinance prints 'Failed download')
                LOG.warning(f"[YFProvider] Empty df (retryable) symbol={symbol} attempt={attempt}/{max_attempts}")

            except Exception as e:
                last_exc = e
                LOG.warning(
                    f"[YFProvider] Download error (retryable) symbol={symbol} attempt={attempt}/{max_attempts}: {e}"
                )

            if attempt < max_attempts:
                sleep = min(max_sleep_sec, base_sleep_sec * (2 ** (attempt - 1)))
                sleep += random.random() * 0.5
                time.sleep(sleep)

        if last_exc is not None:
            LOG.error(f"[YFProvider] Download failed after retries: symbol={symbol} err={last_exc}")
        return None

    
