# core/adapters/datasources/providers/provider_bs.py
# UnifiedRisk V12 — Baostock Provider (A 股指数专用)

from __future__ import annotations
import pandas as pd
import baostock as bs
import traceback
from core.adapters.providers.provider_base import ProviderBase
from core.utils.logger import get_logger

LOG = get_logger("Provider.BS")


class BSProvider(ProviderBase):
    """
    UnifiedRisk V12:
    Baostock Provider for A-share index (e.g. zz500/kc50)

    输出必须统一 DataFrame：
        date / open / high / low / close / volume
    pct 由 ProviderBase.normalize_df 自动补充
    """

    def __init__(self):
        super().__init__("bs")

        LOG.info("[BSProvider] login baostock...")
        lg = bs.login()

        if lg.error_code != '0':
            LOG.error(f"[BSProvider] login fail: {lg.error_msg}")
            raise SystemExit("Baostock login failed")

    # -----------------------------------------------------------
    # 覆写抽象方法
    # -----------------------------------------------------------
    def fetch_series_raw(self, symbol: str, window: int, method: str = "default"):
        """
        symbol 格式：
            "sh.000905" → 中证500
            "sh.000688" → 科创50
        """
        LOG.info(f"[BSProvider] Fetch symbol={symbol}, window={window}, method={method}")

        try:
            return self._fetch_bs_raw(symbol, window)

        except Exception as e:
            LOG.error(f"[BSProvider] fetch_series_raw fatal: symbol={symbol}, error={e}")
            traceback.print_exc()
            return None

    # -----------------------------------------------------------
    # 核心 Baostock 获取逻辑
    # -----------------------------------------------------------
    def _fetch_bs_raw(self, symbol: str, window: int):
        """
        使用 baostock 拉取过去 window 天的指数日线
        """
        # 查询过去 window 天的日期区间
        rs = bs.query_history_k_data_plus(
            symbol,
            "date,open,high,low,close,volume",
            frequency="d",
            adjustflag="3",
        )

        if rs.error_code != '0':
            LOG.error(f"[BSProvider] fetch error: {rs.error_msg} ({symbol})")
            return None

        rows = []
        while rs.next():
            rows.append(rs.get_row_data())

        if not rows:
            LOG.warning(f"[BSProvider] No rows for {symbol}")
            return None

        df = pd.DataFrame(rows, columns=rs.fields)

        # 转换类型
        df["open"] = df["open"].astype(float)
        df["high"] = df["high"].astype(float)
        df["low"] = df["low"].astype(float)
        df["close"] = df["close"].astype(float)
        df["volume"] = df["volume"].astype(float)

        # 按日期排序并截取 window 天
        df = df.sort_values("date").tail(window).reset_index(drop=True)

        return df
