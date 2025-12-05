from __future__ import annotations

import os
from datetime import date
from typing import Dict, Any, Optional

import pandas as pd

from core.utils.logger import log
from core.adapters.datasources.cn.zh_spot_utils import normalize_zh_spot_columns


class MarketDataReaderCN:
    """
    基于 zh_spot.parquet 的市场聚合数据读取器。

    spot_mode:
      - "strict":        只读 parquet，不存在时仅告警
      - "fallback_once": parquet 不存在时调用 ak 抓一次并写入，再读
      - "dev_debug":     每次都调用 ak 抓并覆盖 parquet（开发调试用）
      - "dev_debug_once": 本实例生命周期内只强制抓一次（配合 --force）
    """

    def __init__(
        self,
        trade_date: date,
        root: str = "data/ashare",
        spot_mode: str = "fallback_once",
    ) -> None:
        self.trade_date = trade_date
        self.root = root
        self.spot_mode = spot_mode  # strict / fallback_once / dev_debug / dev_debug_once
        # dev_debug_once 模式下使用，保证本实例内只刷新一次
        self._refreshed = False

    # ---------------- 路径辅助 ----------------

    def _day_root(self) -> str:
        day_str = self.trade_date.strftime("%Y%m%d")
        return os.path.join(self.root, day_str)

    def _get_parquet_path(self) -> str:
        return os.path.join(self._day_root(), "zh_spot.parquet")

    @staticmethod
    def _abs(path: str) -> str:
        try:
            return os.path.abspath(path)
        except Exception:
            return path

    # ---------------- 抓取 + 写 parquet ----------------

    def _fetch_and_save_parquet(self, path: str) -> bool:
        """调用 ak.stock_zh_a_spot 抓一次全市场行情，写入 parquet。"""
        try:
            import akshare as ak
        except Exception as e:
            log(f"[IO] FETCH FAIL ← zh_spot (akshare import error): {e}")
            return False

        try:
            log(f"[IO] FETCH → zh_spot from ak.stock_zh_a_spot() (mode={self.spot_mode})")
            df = ak.stock_zh_a_spot()
            df = normalize_zh_spot_columns(df)

            if df is None or df.empty:
                log("[IO] FETCH FAIL ← zh_spot: ak 返回空 DataFrame")
                return False

            os.makedirs(os.path.dirname(path), exist_ok=True)
            abs_path = self._abs(path)
            log(f"[IO] Writing zh_spot.parquet → {abs_path}")
            df.to_parquet(path, index=False)
            log(f"[IO] Write zh_spot.parquet OK ← {abs_path} (rows={len(df)})")
            return True
        except Exception as e:
            abs_path = self._abs(path)
            log(f"[IO] Write zh_spot.parquet FAIL ← {abs_path}, error={e}")
            return False

    # ---------------- 保证 parquet 存在 ----------------

    def _ensure_parquet(self) -> Optional[str]:
        """根据 spot_mode 保证 parquet 存在。"""
        path = self._get_parquet_path()
        abs_path = self._abs(path)

        if self.spot_mode == "strict":
            if not os.path.exists(path):
                log(f"[IO] Load zh_spot.parquet skipped (not exists, strict) ← {abs_path}")
                return None
            log(f"[IO] Loading zh_spot.parquet ← {abs_path} (strict cache hit)")
            return path

        if self.spot_mode == "fallback_once":
            if os.path.exists(path):
                log(f"[IO] Loading zh_spot.parquet ← {abs_path} (fallback_once cache hit)")
                return path
            log(f"[IO] Load zh_spot.parquet miss (fallback_once) ← {abs_path}, will fetch")
            ok = self._fetch_and_save_parquet(path)
            return path if ok else None

        if self.spot_mode == "dev_debug":
            log(f"[IO] FORCE → zh_spot(dev_debug) 强制刷新 parquet: {abs_path}")
            ok = self._fetch_and_save_parquet(path)
            if ok:
                log(f"[IO] FORCE OK ← zh_spot(dev_debug) {abs_path}")
            else:
                log(f"[IO] FORCE FAIL ← zh_spot(dev_debug) {abs_path}")
            return path if ok else None

        if self.spot_mode == "dev_debug_once":
            if not self._refreshed:
                log(f"[IO] FORCE → zh_spot(dev_debug_once) 首次刷新 parquet: {abs_path}")
                ok = self._fetch_and_save_parquet(path)
                if ok:
                    self._refreshed = True
                    log(f"[IO] FORCE OK ← zh_spot(dev_debug_once) {abs_path}")
                else:
                    log(f"[IO] FORCE FAIL ← zh_spot(dev_debug_once) {abs_path}")
                if ok or os.path.exists(path):
                    return path
                return None

            if os.path.exists(path):
                log(f"[IO] Loading zh_spot.parquet ← {abs_path} (dev_debug_once cached)")
                return path

            log(f"[IO] Load zh_spot.parquet miss (dev_debug_once cached) ← {abs_path}, fallback fetch")
            ok = self._fetch_and_save_parquet(path)
            return path if ok else None

        log(f"[MarketDataReaderCN] 未知 spot_mode={self.spot_mode}，按 strict 处理")
        if not os.path.exists(path):
            log(f"[IO] Load zh_spot.parquet skipped (not exists, unknown mode) ← {abs_path}")
            return None
        log(f"[IO] Loading zh_spot.parquet ← {abs_path} (unknown-mode cache hit)")
        return path

    # ---------------- 读取 parquet ----------------

    def load_zh_spot(self) -> Optional[pd.DataFrame]:
        path = self._ensure_parquet()
        abs_path = self._abs(path) if path else "<None>"

        if not path or not os.path.exists(path):
            log(f"[IO] Load zh_spot.parquet FAIL ← {abs_path} (path 不存在或 _ensure_parquet 返回 None)")
            return None

        try:
            log(f"[IO] Loading zh_spot.parquet ← {abs_path}")
            df = pd.read_parquet(path)
            df = normalize_zh_spot_columns(df)
            if df is None or df.empty:
                log(f"[IO] Load zh_spot.parquet FAIL ← {abs_path} (df 为空)")
                return None
            log(f"[IO] Load zh_spot.parquet OK ← {abs_path} (rows={len(df)})")
            return df
        except Exception as e:
            log(f"[IO] Load zh_spot.parquet EXC ← {abs_path}, error={e}")
            return None

    # ---------------- 成交额汇总 ----------------

    def get_turnover_summary(self) -> Dict[str, float]:
        df = self.load_zh_spot()
        if df is None or df.empty:
            log("[Turnover] zh_spot df 为空，返回 0 成交额")
            return {
                "sh_turnover_e9": 0.0,
                "sz_turnover_e9": 0.0,
                "total_turnover_e9": 0.0,
            }

        sym_col = "symbol" if "symbol" in df.columns else ("代码" if "代码" in df.columns else None)
        if sym_col is None:
            log("[Turnover] 缺少 symbol/代码 列，无法区分 SH/SZ，返回 0")
            return {
                "sh_turnover_e9": 0.0,
                "sz_turnover_e9": 0.0,
                "total_turnover_e9": 0.0,
            }

        sym_series = df[sym_col].astype(str).str.lower()
        is_sh = sym_series.str.startswith("sh")
        is_sz = sym_series.str.startswith("sz")
        df_sh = df[is_sh]
        df_sz = df[is_sz]

        if "amount" not in df.columns:
            log("[Turnover] 缺少 amount 列，返回 0 成交额")
            return {
                "sh_turnover_e9": 0.0,
                "sz_turnover_e9": 0.0,
                "total_turnover_e9": 0.0,
            }

        sh_turnover_e9 = float(df_sh["amount"].sum()) / 1e9
        sz_turnover_e9 = float(df_sz["amount"].sum()) / 1e9
        total = sh_turnover_e9 + sz_turnover_e9

        log(f"[Turnover] SH={sh_turnover_e9:.2f} SZ={sz_turnover_e9:.2f} TOTAL={total:.2f} (unit=亿)")
        return {
            "sh_turnover_e9": sh_turnover_e9,
            "sz_turnover_e9": sz_turnover_e9,
            "total_turnover_e9": total,
        }

    # ---------------- 市场宽度汇总 ----------------

    def get_breadth_summary(self) -> Dict[str, Any]:
        df = self.load_zh_spot()
        if df is None or df.empty:
            log("[Breadth] zh_spot df 为空，返回 0 宽度")
            return {
                "adv": 0,
                "dec": 0,
                "limit_up": 0,
                "limit_down": 0,
                "total": 0,
            }

        if "price" not in df.columns or "pre_close" not in df.columns:
            log("[Breadth] 缺少 price/pre_close 列，返回 0 宽度")
            return {
                "adv": 0,
                "dec": 0,
                "limit_up": 0,
                "limit_down": 0,
                "total": len(df),
            }

        diff = df["price"] - df["pre_close"]
        adv = int((diff > 0).sum())
        dec = int((diff < 0).sum())

        base = df["pre_close"].replace(0, 1)
        pct = diff / base

        limit_up = int((pct > 0.098).sum())
        limit_down = int((pct < -0.098).sum())

        total = int(len(df))
        log(f"[Breadth] adv={adv} dec={dec} limit_up={limit_up} limit_down={limit_down} total={total}")
        return {
            "adv": adv,
            "dec": dec,
            "limit_up": limit_up,
            "limit_down": limit_down,
            "total": total,
        }
