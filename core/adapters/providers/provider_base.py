# core/adapters/datasources/providers/provider_base.py
# UnifiedRisk V12 — Provider 抽象基类
# 所有数据源（YF / BS / push2 / ak / web）必须继承此类。
# Provider 不负责缓存、不负责 snapshot，只负责底层数据获取。

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Optional
import pandas as pd
import traceback
import pandas
from core.utils.logger import get_logger

LOG = get_logger("Provider.Base")


class ProviderBase(ABC):
    """
    UnifiedRisk V12 Provider 抽象基类
    ---------------------------------------------------------------
    DataSource → Provider.fetch() → DataFrame
    Provider 只负责底层数据获取，不负责写入 cache/history。
    
    所有 Provider 必须确保返回 DataFrame 的标准字段：
        date:   YYYY-MM-DD
        open:   float
        high:   float
        low:    float
        close:  float
        volume: float
        pct:    float (ProviderBase.normalize_df 自动填补)
    """

    def __init__(self, name: str):
        self.name = name
        LOG.info(f"[ProviderBase] Init provider={name}")

    # ---------------------------------------------------------------
    # DataSource 永远调用 provider.fetch()
    # ---------------------------------------------------------------
    def fetch(self, symbol: str, window: int = 60, method: str = "default") -> Optional[pandas.DataFrame]:
        """
        统一入口：
        - Provider 子类必须实现 fetch_series_raw()，返回原始数据结构。
        - ProviderBase 会把原始数据规范成统一 DataFrame。
        """

        LOG.info(f"[ProviderBase] fetch(provider={self.name}, symbol={symbol}, method={method})")

        try:
            # 让子类实现具体抓取逻辑
            raw_df = self.fetch_series_raw(symbol, window, method)

            if raw_df is None:
                LOG.error(f"[ProviderBase] Provider {self.name} returned None for {symbol}")
                return None

            # 统一标准化成核心 DataFrame 结构
            df = self.normalize_df(raw_df)

            return df

        except SystemExit:
            raise

        except Exception as e:
            LOG.error(f"[ProviderBase] fetch fatal error provider={self.name}, symbol={symbol}, error={e}")
            traceback.print_exc()
            return None

    # ---------------------------------------------------------------
    # 子类必须实现：fetch_series_raw()
    # ---------------------------------------------------------------
    @abstractmethod
    def fetch_series_raw(self, symbol: str, window: int, method: str = "default"):
        """
        Provider 子类必须实现的底层抓数方法。
        返回的可以是 DataFrame / dict / list
        最终都会被 normalize_df 统一成 DF。
        """
        pass

    # ---------------------------------------------------------------
    # DataFrame 标准化（所有 Provider 必须返回同样格式）
    # ---------------------------------------------------------------
    def normalize_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        强制统一 DataFrame 格式：
        - 包含 date/open/high/low/close/volume
        - pct 自动计算（如不存在）
        """

        if not isinstance(df, pd.DataFrame):
            LOG.error(f"[ProviderBase] normalize_df: raw is not DataFrame (provider={self.name})")
            raise SystemExit(f"Provider {self.name} returned invalid data type")

        df = df.copy()

        # -----------------------------------------------------------
        # 日期字段检查
        # -----------------------------------------------------------
        if "date" not in df.columns:
            LOG.error(f"[ProviderBase] normalize_df: df missing 'date' column provider={self.name}")
            raise SystemExit(f"[FATAL] Provider {self.name} returned df without 'date'")

        df["date"] = df["date"].astype(str)

        # -----------------------------------------------------------
        # 统一补全字段
        # -----------------------------------------------------------
        required_cols = ["open", "high", "low", "close", "volume"]
        for col in required_cols:
            if col not in df.columns:
                LOG.warning(f"[ProviderBase] normalize_df: missing {col}, provider={self.name}")
                df[col] = 0.0

        # pct 计算
        if "pct" not in df.columns:
            df["pct"] = df["close"].pct_change().fillna(0.0)

        # 排序
        df = df.sort_values("date").reset_index(drop=True)

        return df
