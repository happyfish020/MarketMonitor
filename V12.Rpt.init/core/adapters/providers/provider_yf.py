# core/adapters/datasources/providers/provider_yf.py
# UnifiedRisk V12 — Yahoo Finance Provider (full version)

from __future__ import annotations
import pandas as pd
import traceback
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
            method_name = f"_fetch_{method}_method"
            if hasattr(self, method_name):
                LOG.info(f"[YFProvider] Using YF submethod: {method_name} for symbol={symbol}")
                return getattr(self, method_name)(symbol, window)

            # fallback
            LOG.warning(f"[YFProvider] No such method={method}, using default")
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
            df = yf.download(
                symbol,
                period=f"{window}d",
                auto_adjust=False,
                progress=False
            )

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
