# core/adapters/datasources/glo/global_lead_source.py

import os
import json
from typing import Optional, Dict

import pandas as pd

from core.utils.logger import get_logger
from core.utils.datasource_config import DataSourceConfig
from core.adapters.cache.symbol_cache import _normalize_symbol

LOG = get_logger("GlobalLeadSource")

 
class GlobalLeadSource:
    """
    统一的 Global Lead 数据源（含缓存 + 远程 fetch + snapshot）
    要求 fetch_client 实现：
        fetch_global_lead(symbol, start=None, end=None) -> DataFrame(date, close, pct)
    如果暂时没有 client，可先只用本地缓存。
    """

    def __init__(self, config: Optional[DataSourceConfig] = None, fetch_client=None):
        self.config = config or DataSourceConfig(market="glo")
        self.config.ensure_dirs()

        self.fetch_client = fetch_client

        self.history_root = os.path.join(self.config.history_root, "global_lead")
        os.makedirs(self.history_root, exist_ok=True)

        LOG.info("初始化 GlobalLeadSource: history_root=%s", os.path.abspath(self.history_root))

    # ---------- path ----------
    def _history_path(self, symbol: str) -> str:
        safe = _normalize_symbol(symbol)
        return os.path.join(self.history_root, f"{safe}.json")

    # ---------- load cache ----------
    def _load_local(self, symbol: str) -> pd.DataFrame:
        path = self._history_path(symbol)
        abs_path = os.path.abspath(path)

        LOG.info("尝试读取 GlobalLead 本地缓存: symbol=%s path=%s", symbol, abs_path)

        if not os.path.exists(abs_path):
            LOG.info("本地缓存不存在: symbol=%s", symbol)
            return pd.DataFrame(columns=["date", "close", "pct"])

        try:
            with open(abs_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            LOG.error("读取本地缓存失败: symbol=%s path=%s error=%s", symbol, abs_path, e)
            return pd.DataFrame(columns=["date", "close", "pct"])

        df = pd.DataFrame(data)
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)

        LOG.info("读取缓存成功: symbol=%s rows=%s", symbol, len(df))
        return df

    # ---------- save cache ----------
    def _save_local(self, symbol: str, df: pd.DataFrame):
        if df is None or df.empty:
            LOG.warning("写缓存时数据为空，跳过: symbol=%s", symbol)
            return

        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])
        df["date"] = df["date"].dt.strftime("%Y-%m-%d")
        records = df.to_dict(orient="records")

        path = self._history_path(symbol)
        abs_path = os.path.abspath(path)
        os.makedirs(os.path.dirname(abs_path), exist_ok=True)

        LOG.info("写入 GlobalLead 本地缓存: symbol=%s rows=%s path=%s", symbol, len(df), abs_path)

        try:
            with open(abs_path, "w", encoding="utf-8") as f:
                json.dump(records, f, ensure_ascii=False, indent=2)
        except Exception as e:
            LOG.error("写缓存失败: symbol=%s path=%s error=%s", symbol, abs_path, e)

    # ---------- slice helper ----------
    def _slice(self, df: pd.DataFrame, start: Optional[str], end: Optional[str]) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        if start:
            df = df[df["date"] >= pd.to_datetime(start)]
        if end:
            df = df[df["date"] <= pd.to_datetime(end)]
        return df.reset_index(drop=True)

    # ---------- public: get_series ----------
    def get_series(
        self,
        symbol: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
        refresh: bool = False,
    ) -> pd.DataFrame:
        LOG.info(
            "请求 GlobalLead 序列: symbol=%s start=%s end=%s refresh=%s",
            symbol, start, end, refresh,
        )

        local_df = self._load_local(symbol)

        # 无 client 只能返回本地
        if not self.fetch_client:
            LOG.warning("fetch_client 未提供，仅使用本地缓存: symbol=%s", symbol)
            return self._slice(local_df, start, end)

        # 不刷新且本地有数据
        if not refresh and not local_df.empty:
            LOG.info("使用本地缓存（非刷新模式）: symbol=%s", symbol)
            return self._slice(local_df, start, end)

        # 需要刷新：以本地最后日期为起点
        latest = None
        if not local_df.empty:
            latest = local_df["date"].max().strftime("%Y-%m-%d")

        fetch_start = latest or start
        LOG.info("远程获取 GlobalLead: symbol=%s fetch_start=%s end=%s", symbol, fetch_start, end)

        try:
            remote_df = self.fetch_client.fetch_global_lead(
                symbol=symbol,
                start=fetch_start,
                end=end,
            )
        except Exception as e:
            LOG.error("远程获取失败，使用缓存 fallback: symbol=%s error=%s", symbol, e)
            return self._slice(local_df, start, end)

        if remote_df is None or remote_df.empty:
            LOG.warning("远程返回空数据，使用缓存 fallback: symbol=%s", symbol)
            return self._slice(local_df, start, end)

        remote_df["date"] = pd.to_datetime(remote_df["date"])
        remote_df = remote_df.sort_values("date")

        if not local_df.empty:
            merged = pd.concat([local_df, remote_df], ignore_index=True)
            merged = merged.sort_values("date").drop_duplicates("date").reset_index(drop=True)
        else:
            merged = remote_df

        LOG.info(
            "合并本地与远程: symbol=%s local=%s remote=%s final=%s",
            symbol, len(local_df), len(remote_df), len(merged),
        )

        self._save_local(symbol, merged)
        return self._slice(merged, start, end)

    # ---------- public: last_quote ----------
    def get_last_quote(self, symbol: str, refresh: bool = False) -> Dict[str, Optional[float]]:
        LOG.info("获取 GlobalLead 最新 quote: symbol=%s refresh=%s", symbol, refresh)

        df = self.get_series(symbol, refresh=refresh)
        if df is None or df.empty:
            LOG.warning("GlobalLead 无数据: symbol=%s", symbol)
            return {"close": None, "pct": None}

        last = df.iloc[-1]
        close = float(last["close"])
        pct = float(last.get("pct", 0.0)) if last.get("pct") is not None else None

        LOG.info("GlobalLead 最新: symbol=%s close=%s pct=%s", symbol, close, pct)
        return {"close": close, "pct": pct}
