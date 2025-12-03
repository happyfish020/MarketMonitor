from __future__ import annotations

import os
from datetime import date
from typing import Dict, Any, Optional

import pandas as pd

from core.utils.logger import log


class MarketDataReaderCN:
    """
    基于 zh_spot.parquet 的市场聚合数据读取器。

    spot_mode:
      - "strict":        只读 parquet，不存在时仅告警
      - "fallback_once": parquet 不存在时调用 ak 抓一次并写入，再读
      - "dev_debug":     每次都调用 ak 抓并覆盖 parquet（开发调试用）
    """

    def __init__(self, trade_date: date, root: str = "data/ashare", spot_mode: str = "fallback_once") -> None:
        self.trade_date = trade_date
        self.root = root
        self.spot_mode = spot_mode  # strict / fallback_once / dev_debug

    # ----------- 路径 & parquet -----------

    def _day_root(self) -> str:
        day_str = self.trade_date.strftime("%Y%m%d")
        return os.path.join(self.root, day_str)

    def _get_parquet_path(self) -> str:
        return os.path.join(self._day_root(), "zh_spot.parquet")

    def _fetch_and_save_parquet(self, path: str) -> bool:
        """调用 ak.stock_zh_a_spot 抓一次全市场行情，写入 parquet。"""
        try:
            import akshare as ak
        except Exception as e:
            log(f"[MarketDataReaderCN] 导入 akshare 失败，无法抓取 zh_spot: {e}")
            return False

        try:
            log(f"[MarketDataReaderCN] 调用 ak.stock_zh_a_spot 抓取全市场行情 (mode={self.spot_mode})")
            df = ak.stock_zh_a_spot()
            if df is None or df.empty:
                log("[MarketDataReaderCN] ak.stock_zh_a_spot 返回空 DataFrame")
                return False

            os.makedirs(os.path.dirname(path), exist_ok=True)
            df.to_parquet(path, index=False)
            log(f"[MarketDataReaderCN] zh_spot parquet 已写入: {path} (rows={len(df)})")
            return True
        except Exception as e:
            log(f"[MarketDataReaderCN] ak.zh_spot 抓取或写入失败: {e}")
            return False

    def _ensure_parquet(self) -> Optional[str]:
        """根据 spot_mode 保证 parquet 存在。"""
        path = self._get_parquet_path()

        if self.spot_mode == "strict":
            if not os.path.exists(path):
                log(f"[MarketDataReaderCN] strict 模式: parquet 不存在 {path}")
                return None
            return path

        if self.spot_mode == "fallback_once":
            if os.path.exists(path):
                return path
            ok = self._fetch_and_save_parquet(path)
            return path if ok else None

        if self.spot_mode == "dev_debug":
            ok = self._fetch_and_save_parquet(path)
            return path if ok else None

        log(f"[MarketDataReaderCN] 未知 spot_mode={self.spot_mode}，按 strict 处理")
        if not os.path.exists(path):
            return None
        return path

    def load_zh_spot(self) -> Optional[pd.DataFrame]:
        """统一入口：根据 spot_mode 决定是否尝试抓取，最终返回 DataFrame 或 None。"""
        path = self._ensure_parquet()
        if not path or not os.path.exists(path):
            return None

        try:
            df = pd.read_parquet(path)
            if df is None or df.empty:
                log(f"[MarketDataReaderCN] parquet 为空: {path}")
                return None
            return df
        except Exception as e:
            log(f"[MarketDataReaderCN] 读取 parquet 失败: {e}")
            return None

    # ----------- 成交额（Turnover） -----------

    def get_turnover_summary(self) -> Dict[str, float]:
        df = self.load_zh_spot()
        if df is None or df.empty:
            return {
                "sh_turnover_e9": 0.0,
                "sz_turnover_e9": 0.0,
                "total_turnover_e9": 0.0,
            }

        if "amount" not in df.columns:
            log("[MarketDataReaderCN] parquet 缺少 amount 列，请检查 zh_spot 字段")
            return {
                "sh_turnover_e9": 0.0,
                "sz_turnover_e9": 0.0,
                "total_turnover_e9": 0.0,
            }

        if "market" in df.columns:
            df_sh = df[df["market"] == "SH"]
            df_sz = df[df["market"] == "SZ"]
        else:
            sym_col = "symbol" if "symbol" in df.columns else "代码"
            sym_series = df[sym_col].astype(str)
            df_sh = df[sym_series.str.endswith("SH")]
            df_sz = df[sym_series.str.endswith("SZ")]

        sh_turnover_e9 = float(df_sh["amount"].sum()) / 1e9
        sz_turnover_e9 = float(df_sz["amount"].sum()) / 1e9
        total = sh_turnover_e9 + sz_turnover_e9

        return {
            "sh_turnover_e9": sh_turnover_e9,
            "sz_turnover_e9": sz_turnover_e9,
            "total_turnover_e9": total,
        }

    # ----------- 市场情绪（Breadth） -----------

    def get_breadth_summary(self) -> Dict[str, Any]:
        df = self.load_zh_spot()
        if df is None or df.empty:
            return {
                "adv": 0,
                "dec": 0,
                "limit_up": 0,
                "limit_down": 0,
                "total": 0,
            }

        price_col = "close" if "close" in df.columns else ("最新价" if "最新价" in df.columns else None)
        prev_col = "pre_close" if "pre_close" in df.columns else ("昨收" if "昨收" in df.columns else None)

        if price_col is None or prev_col is None:
            log("[MarketDataReaderCN] parquet 缺少 close / pre_close（或对应中文列），无法统计涨跌")
            return {
                "adv": 0,
                "dec": 0,
                "limit_up": 0,
                "limit_down": 0,
                "total": len(df),
            }

        diff = df[price_col] - df[prev_col]
        adv = int((diff > 0).sum())
        dec = int((diff < 0).sum())

        df_valid = df[df[prev_col] > 0]
        if df_valid.empty:
            return {
                "adv": adv,
                "dec": dec,
                "limit_up": 0,
                "limit_down": 0,
                "total": len(df),
            }

        pct = (df_valid[price_col] - df_valid[prev_col]) / df_valid[prev_col]
        limit_up = int((pct > 0.098).sum())
        limit_down = int((pct < -0.098).sum())

        total = int(len(df))

        return {
            "adv": adv,
            "dec": dec,
            "limit_up": limit_up,
            "limit_down": limit_down,
            "total": total,
        }