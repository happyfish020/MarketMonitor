"""
BaseIndexSource (V12 FULL)
抽象：指数/序列类 DataSource 的统一基础类

职责：
    - 统一 Cache/History 的路径规则（但不负责决定 cache_root/history_root —— 由 DataSourceConfig 决定）
    - 统一 df → block（标准 schema）
    - 统一 fallback from history
    - 统一 neutral block
    - 统一 last/prev/pct 提取
    - 统一 history 写入（按日文件，append-only）
    
子类需要实现：
    - _fetch_df(self, entry)     # 返回 DataFrame 或 None
    - parse symbol entries       # 如 parse from symbols.yaml
    
适用对象：
    - IndexGlobalSource
    - GlobalLeadSource
    - IndexSeriesSource
    - IndexTechSource（如使用序列）
"""

import os
import json
from typing import Any, Dict, List, Optional

import pandas as pd

from core.datasources.datasource_base import DataSourceBase
from core.adapters.cache.symbol_cache import normalize_symbol
from core.utils.logger import get_logger

LOG = get_logger("DS.BaseIndex")


class IndexSourceBase(DataSourceBase):
    """
    指数 DS 的通用父类
    """

    # 子类需设置序列窗口默认长度
    default_window = 120

    def __init__(self, config):
        super().__init__(config)

        # 子类可以覆盖
        self.window = self.default_window

    # ----------------------------------------------------------------------
    # 子类必须实现的方法
    # ----------------------------------------------------------------------

    def _fetch_df(self, entry: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """
        抽象方法：实际拉取 DataFrame。
        子类必须实现。
        """
        raise NotImplementedError("_fetch_df() must be implemented by subclass")

    # ----------------------------------------------------------------------
    # Cache / History 路径辅助
    # ----------------------------------------------------------------------

    def cache_file(self, safe_symbol: str, trade_date: str) -> str:
        """
        Cache 路径（V12 规范）：
            {cache_root}/{trade_date}/{safe_symbol}.json
        这里的 cache_root 已经由 DataSourceConfig 配置好。
        """
        return os.path.join(self.cache_root, trade_date, f"{safe_symbol}.json")

    def history_file(self, safe_symbol: str, trade_date: str) -> str:
        """
        History 单日文件（V12 规范）：
            {history_root}/{safe_symbol}/{trade_date}.json
        """
        return os.path.join(self.history_root, safe_symbol, f"{trade_date}.json")

    def history_dir(self, safe_symbol: str) -> str:
        return os.path.join(self.history_root, safe_symbol)

    # ----------------------------------------------------------------------
    # History 写入逻辑（V12 统一）
    # ----------------------------------------------------------------------

    def write_history(self, safe_symbol: str, df: pd.DataFrame, trade_date: str) -> None:
        """
        将 df（多日序列）拆分为多个单日 JSON 文件写入 history。

        规则：
            - 仅写入 ["date", "close", "pct"] 字段（如存在）
            - 非 trade_date 的历史文件存在 → 不覆盖
            - trade_date 文件允许覆盖（full 模式重算）
        """
        if "date" not in df.columns:
            LOG.warning(
                "[BaseIndex] write_history: df 无 date 列 safe_symbol=%s，跳过",
                safe_symbol,
            )
            return

        cols = [c for c in ("date", "close", "pct") if c in df.columns]
        records = df[cols].to_dict("records")

        dir_path = self.history_dir(safe_symbol)
        os.makedirs(dir_path, exist_ok=True)

        for rec in records:
            date_str = rec.get("date")
            if not date_str:
                continue

            path = os.path.join(dir_path, f"{date_str}.json")

            # 保护历史：非今天文件不覆盖
            if date_str != trade_date and os.path.exists(path):
                continue

            try:
                self._save_json(path, rec)
            except Exception as e:
                LOG.error(
                    "[BaseIndex] HistoryWriteError safe_symbol=%s date=%s: %s",
                    safe_symbol,
                    date_str,
                    e,
                )

    # ----------------------------------------------------------------------
    # History fallback（尝试从 history 合并序列）
    # ----------------------------------------------------------------------

    def load_history_series(self, safe_symbol: str) -> Optional[pd.DataFrame]:
        """
        遍历 history/{safe_symbol}/ 下所有 JSON 文件
        合并成序列 DataFrame。
        """
        dir_path = self.history_dir(safe_symbol)
        if not os.path.isdir(dir_path):
            return None

        files = sorted(
            f for f in os.listdir(dir_path)
            if f.endswith(".json")
        )
        if not files:
            return None

        records: List[Dict[str, Any]] = []

        for fname in files:
            path = os.path.join(dir_path, fname)
            try:
                obj = self._load_json(path)
            except Exception as e:
                LOG.error("[BaseIndex] HistoryReadError file=%s: %s", path, e)
                continue

            # 兼容旧格式：可能是 list or dict
            if isinstance(obj, list):
                records.extend(obj)
            elif isinstance(obj, dict):
                records.append(obj)

        if not records:
            return None

        df = pd.DataFrame(records)
        if "date" in df.columns:
            df = df.sort_values("date")

        return df

    def fallback_from_history(
        self, name: str, entry: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        当远程拉取失败或空数据时，从 history 回退。
        如果 history 也无数据，产生 neutral block。
        """
        symbol = entry["symbol"]
        safe_symbol = normalize_symbol(symbol)

        df = self.load_history_series(safe_symbol)
        if df is None or df.empty:
            return self.neutral_block(name, entry)

        return self.df_to_block(name, entry, df)

    # ----------------------------------------------------------------------
    # df → block 转换（统一 schema）
    # ----------------------------------------------------------------------

    @staticmethod
    def extract_last_prev(df: pd.DataFrame) -> tuple[Any, Any]:
        """
        提取 last/prev 收盘价。
        """
        if df is None or df.empty or "close" not in df.columns:
            return None, None

        last_close = float(df.iloc[-1]["close"])
        prev_close = float(df.iloc[-2]["close"]) if len(df) >= 2 else None
        return last_close, prev_close

    def df_to_block(
        self,
        name: str,
        entry: Dict[str, Any],
        df: pd.DataFrame,
    ) -> Dict[str, Any]:
        """
        标准 block schema：

        {
            "name": name,
            "alias": alias,
            "symbol": symbol,
            "last": float,
            "prev": float | None,
            "pct": float | None,
            "series": [...records...],

            # 兼容字段：
            "close": last,
            "prev_close": prev,
            "window": series
        }
        """
        symbol = entry["symbol"]
        alias = entry.get("alias", name)

        last_close, prev_close = self.extract_last_prev(df)

        # pct：优先使用 df["pct"]，否则用 last/prev 计算
        pct_val = None
        if "pct" in df.columns and not df["pct"].isna().all():
            pct_val = float(df.iloc[-1]["pct"])
        elif last_close is not None and prev_close not in (None, 0):
            pct_val = (last_close / prev_close - 1.0) * 100.0

        series = df.to_dict("records")

        return {
            "name": name,
            "alias": alias,
            "symbol": symbol,
            "last": last_close,
            "prev": prev_close,
            "pct": pct_val,
            "series": series,

            # 兼容字段
            "close": last_close,
            "prev_close": prev_close,
            "window": series,
        }

    # ----------------------------------------------------------------------
    # neutral block（无数据时）
    # ----------------------------------------------------------------------

    @staticmethod
    def neutral_block(name: str, entry: Dict[str, Any]) -> Dict[str, Any]:
        """
        中性 block，避免 snapshot 中断。
        """
        symbol = entry["symbol"]
        alias = entry.get("alias", name)

        return {
            "name": name,
            "alias": alias,
            "symbol": symbol,
            "last": None,
            "prev": None,
            "pct": None,
            "series": [],
            "close": None,
            "prev_close": None,
            "window": [],
        }

    # ----------------------------------------------------------------------
    # JSON 工具
    # ----------------------------------------------------------------------

    @staticmethod
    def _load_json(path: str) -> Any:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def _save_json(path: str, obj: Any) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
