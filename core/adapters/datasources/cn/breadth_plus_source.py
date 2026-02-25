# core/adapters/datasources/cn/breadth_plus_source.py
# -*- coding: utf-8 -*-
"""
BreadthPlusDataSource (Leading-Structure DataPack v1 / Panel B)

鐩爣锛堝喕缁擄級锛?
- 杈撳嚭骞垮害澧炲己 raw block锛堜笉鍋氫笟鍔¤В閲?鎵撳垎锛?
- 鏁版嵁缂哄け/瑕嗙洊涓嶈冻锛氬繀椤讳互 MISSING/PARTIAL + warnings 浣撶幇锛涚姝?silent exception
- 榛樿浠呮敮鎸?EOD锛汭NTRADAY 杩斿洖鍗犱綅 MISSING锛坅ppend-only 鍚庡啀鎵╁睍锛?

鏈増鏈寮猴紙v1 schema append-only锛夛細
- 琛ラ綈 %>MA20 / %>MA50锛堣В鍐?coverage=0 / missing:pct_above_ma20/ma50锛?
- 琛ラ綈 New High / New Low锛?0D/50D锛変笌 new_high_low_ratio锛堥粯璁?50D锛?
- 琛ラ綈 A/D line锛堝噣涓婃定瀹舵暟绱鐨?5D/20D delta 绛夛級
"""

from __future__ import annotations

import os
import json
import math
import datetime
from typing import Dict, Any, List, Optional

import pandas as pd

from core.utils.logger import get_logger
from core.datasources.datasource_base import DataSourceBase, DataSourceConfig
from core.utils.ds_refresh import apply_refresh_cleanup
from core.adapters.providers.db_provider_mysql_market import DBOracleProvider

LOG = get_logger("DS.BreadthPlus")


def _to_date_any(v) -> datetime.date:
    """Accept datetime.date / datetime.datetime / YYYY-MM-DD / YYYYMMDD."""
    if v is None:
        raise ValueError("trade_date is None")
    if isinstance(v, datetime.datetime):
        return v.date()
    if isinstance(v, datetime.date):
        return v
    s = str(v).strip()
    if not s:
        raise ValueError("trade_date is empty")
    if "-" in s and len(s) >= 10:
        return datetime.date.fromisoformat(s[:10])
    if len(s) == 8 and s.isdigit():
        return datetime.date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
    # fallback: try pandas
    try:
        ts = pd.to_datetime(s)
        return ts.date()
    except Exception as e:
        raise ValueError(f"invalid trade_date: {v}") from e


def _safe_float(x, ndigits: int = 4):
    if x is None:
        return None
    try:
        if isinstance(x, (float, int)):
            return round(float(x), ndigits)
        # numpy / pandas
        return round(float(x), ndigits)
    except Exception:
        return None


def _safe_int(x):
    if x is None:
        return None
    try:
        return int(x)
    except Exception:
        return None


class BreadthPlusDataSource(DataSourceBase):
    def __init__(self, config: DataSourceConfig, is_intraday: bool = False):
        super().__init__(name="DS.BreadthPlus")
        self.config = config
        self.cache_root = config.cache_root
        self.history_root = config.history_root
        self.is_intraday = bool(is_intraday)

        os.makedirs(self.cache_root, exist_ok=True)
        os.makedirs(self.history_root, exist_ok=True)

        self.db = DBOracleProvider()

    # -------------------------
    # Public API
    # -------------------------
    def build_block(self, trade_date: str, refresh_mode: str = "none") -> Dict[str, Any]:
        schema_version = "BREADTH_PLUS_RAW_V1_2026Q1"  # keep stable, append-only evidence keys
        kind = "INTRADAY" if self.is_intraday else "EOD"

        cache_file = os.path.join(self.cache_root, f"breadth_plus_raw_{trade_date}.json")
         
        apply_refresh_cleanup( cache_path=cache_file, refresh_mode=refresh_mode,) 
        # cache hit
        if os.path.exists(cache_file):
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                # broken cache -> continue to rebuild
                pass

        if self.is_intraday:
            block = self._missing_block(
                schema_version=schema_version,
                trade_date=str(trade_date),
                kind=kind,
                warnings=["intraday_not_supported:v1"],
            )
            self._save_cache(cache_file, block)
            return block

        warnings: List[str] = []
        error_type: Optional[str] = None
        error_message: Optional[str] = None

        try:
            asof = _to_date_any(trade_date)
            look_back_days = int(getattr(self.config, "look_back_days", 120) or 120)
            ma20 = 20
            ma50 = 50
            nhnl20 = 20
            nhnl50 = 50

            window_start = asof - datetime.timedelta(days=look_back_days)

            rows = self.db.query_stock_closes(window_start=window_start, trade_date=asof)
            if not rows:
                block = self._missing_block(
                    schema_version=schema_version,
                    trade_date=str(asof),
                    kind=kind,
                    warnings=["empty:stock_closes_window"],
                )
                self._save_cache(cache_file, block)
                return block

            df = pd.DataFrame(
                rows,
                columns=["symbol", "exchange", "trade_date", "pre_close", "chg_pct", "close", "amount"],
            )
            LOG.info("[DS.BreadthPlus] Compute MA / NHNL on per-symbol tails...")
            # normalize trade_date to date
            df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

            # Keep only needed cols
            df = df[["symbol", "trade_date", "pre_close", "close"]]
            df = df.dropna(subset=["symbol", "trade_date", "close"])

            # sort for rolling
            df = df.sort_values(["symbol", "trade_date"], ascending=[True, True])

            # ---------------------------------------
            # Compute MA / NHNL on per-symbol tails
            # ---------------------------------------
            # keep last 50 (enough for ma50/nhnl50)
            df_tail = df.groupby("symbol", sort=False).tail(max(ma50, nhnl50)).copy()

            # rolling metrics
            df_tail["ma20"] = df_tail.groupby("symbol", sort=False)["close"].transform(lambda s: s.rolling(ma20).mean())
            df_tail["ma50"] = df_tail.groupby("symbol", sort=False)["close"].transform(lambda s: s.rolling(ma50).mean())

            # rolling max/min (include current)
            df_tail["max20"] = df_tail.groupby("symbol", sort=False)["close"].transform(lambda s: s.rolling(nhnl20).max())
            df_tail["min20"] = df_tail.groupby("symbol", sort=False)["close"].transform(lambda s: s.rolling(nhnl20).min())
            df_tail["max50"] = df_tail.groupby("symbol", sort=False)["close"].transform(lambda s: s.rolling(nhnl50).max())
            df_tail["min50"] = df_tail.groupby("symbol", sort=False)["close"].transform(lambda s: s.rolling(nhnl50).min())

            # previous window extrema (exclude current), for strict "new"
            df_tail["prev_max20"] = df_tail.groupby("symbol", sort=False)["close"].transform(lambda s: s.rolling(nhnl20).max().shift(1))
            df_tail["prev_min20"] = df_tail.groupby("symbol", sort=False)["close"].transform(lambda s: s.rolling(nhnl20).min().shift(1))
            df_tail["prev_max50"] = df_tail.groupby("symbol", sort=False)["close"].transform(lambda s: s.rolling(nhnl50).max().shift(1))
            df_tail["prev_min50"] = df_tail.groupby("symbol", sort=False)["close"].transform(lambda s: s.rolling(nhnl50).min().shift(1))

            last = df_tail.groupby("symbol", sort=False).tail(1)
            last = last[last["trade_date"] == asof].copy()

            total = int(last["symbol"].nunique())

            # MA coverage & % above
            valid_ma20 = int(last["ma20"].notna().sum())
            valid_ma50 = int(last["ma50"].notna().sum())

            above_ma20 = int(((last["close"] > last["ma20"]) & last["ma20"].notna()).sum())
            above_ma50 = int(((last["close"] > last["ma50"]) & last["ma50"].notna()).sum())

            pct_above_ma20 = (above_ma20 * 100.0 / valid_ma20) if valid_ma20 > 0 else None
            pct_above_ma50 = (above_ma50 * 100.0 / valid_ma50) if valid_ma50 > 0 else None

            if valid_ma20 <= 0:
                warnings.append("missing:pct_above_ma20_no_coverage")
            if valid_ma50 <= 0:
                warnings.append("missing:pct_above_ma50_no_coverage")

            cov_ma20 = (valid_ma20 / max(total, 1)) if total > 0 else 0.0
            cov_ma50 = (valid_ma50 / max(total, 1)) if total > 0 else 0.0

            # New High / New Low (strict)
            nh20 = int(((last["close"] >= last["max20"]) & (last["prev_max20"].notna()) & (last["close"] > last["prev_max20"])).sum())
            nl20 = int(((last["close"] <= last["min20"]) & (last["prev_min20"].notna()) & (last["close"] < last["prev_min20"])).sum())
            nh50 = int(((last["close"] >= last["max50"]) & (last["prev_max50"].notna()) & (last["close"] > last["prev_max50"])).sum())
            nl50 = int(((last["close"] <= last["min50"]) & (last["prev_min50"].notna()) & (last["close"] < last["prev_min50"])).sum())

            ratio20 = (nh20 + 1.0) / (nl20 + 1.0)
            ratio50 = (nh50 + 1.0) / (nl50 + 1.0)
            # ---------------------------------------
            # A/D line (market-wide, by date)
            # ---------------------------------------
            ad: Dict[str, Any] = {}
            try:
                df_ad = df[["symbol", "trade_date", "close", "pre_close"]].copy()
                df_ad = df_ad.sort_values(["symbol", "trade_date"], kind="mergesort")

                # Fallback: if PRE_CLOSE is missing in DB, infer by previous CLOSE per symbol.
                # This keeps A/D available even when PRE_CLOSE column is sparsely populated.
                if df_ad["pre_close"].isna().any():
                    df_ad["pre_close_fallback"] = df_ad.groupby("symbol", sort=False)["close"].shift(1)
                    # Only fill where original pre_close is missing
                    df_ad["pre_close2"] = df_ad["pre_close"]
                    df_ad.loc[df_ad["pre_close2"].isna(), "pre_close2"] = df_ad["pre_close_fallback"]
                    warnings.append("assumption:ad_line_pre_close_fallback_shift_close")
                else:
                    df_ad["pre_close2"] = df_ad["pre_close"]

                df_ad = df_ad.dropna(subset=["close", "pre_close2"]).copy()
                df_ad["adv"] = (df_ad["close"] > df_ad["pre_close2"]).astype(int)
                df_ad["dec"] = (df_ad["close"] < df_ad["pre_close2"]).astype(int)

                by_day = df_ad.groupby("trade_date", sort=True).agg(
                    adv=("adv", "sum"),
                    dec=("dec", "sum"),
                )
                by_day["net"] = by_day["adv"] - by_day["dec"]
                by_day["cum"] = by_day["net"].cumsum()

                # align to last available day (should be asof)
                if asof in by_day.index:
                    by_day = by_day.loc[by_day.index <= asof]
                    net_1d = int(by_day.loc[asof, "net"])
                else:
                    net_1d = int(by_day["net"].iloc[-1]) if len(by_day) else 0
                    warnings.append("partial:ad_line_asof_missing")

                last5 = by_day.tail(5)
                last20 = by_day.tail(20)

                net_5d = int(last5["net"].sum()) if len(last5) else None
                net_20d = int(last20["net"].sum()) if len(last20) else None

                cum_20d_delta = None
                if len(by_day) >= 21:
                    cum_20d_delta = int(by_day["cum"].iloc[-1] - by_day["cum"].iloc[-21])

                slope_10d = None
                if len(by_day) >= 10:
                    y = by_day["cum"].tail(10).astype(float).tolist()
                    x = list(range(len(y)))
                    x_mean = sum(x) / len(x)
                    y_mean = sum(y) / len(y)
                    num = sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x, y))
                    den = sum((xi - x_mean) ** 2 for xi in x)
                    slope_10d = (num / den) if den != 0 else None

                ad = {
                    "net_adv_1d": net_1d,
                    "net_adv_5d": net_5d,
                    "net_adv_20d": net_20d,
                    "cum_20d_delta": cum_20d_delta,
                    "slope_10d": _safe_float(slope_10d, 4),
                }
                LOG.info("[DS.BreadthPlus] Compute MA / NHNL on per-symbol done")
            except Exception:
                warnings.append("partial:ad_line_compute_failed")

            evidence: Dict[str, Any] = {
                "window": {
                    "look_back_days": int(look_back_days),
                    "ma20": int(ma20),
                    "ma50": int(ma50),
                    "nhnl20": int(nhnl20),
                    "nhnl50": int(nhnl50),
                },
                "total_stocks": int(total),
                "total": int(total),
                "count": int(total),
                # factor鍏煎锛歝overage 瀛楁鐢ㄤ簬鈥滄€绘牱鏈暟鈥濊€屼笉鏄?dict
                "coverage": int(total),
                # 璇︾粏瑕嗙洊淇℃伅锛坅ppend-only锛?
                "coverage_detail": {
                    "total": int(total),
                    "valid_ma20": int(valid_ma20),
                    "valid_ma50": int(valid_ma50),
                    "coverage_ma20": _safe_float(cov_ma20, 4),
                    "coverage_ma50": _safe_float(cov_ma50, 4),
                },
                "ma20_coverage": _safe_float(cov_ma20, 4),
                "ma50_coverage": _safe_float(cov_ma50, 4),
                # pct_above_* keep both numeric and *_pct naming for compatibility
                "pct_above_ma20": _safe_float(pct_above_ma20, 2),
                "pct_above_ma50": _safe_float(pct_above_ma50, 2),
                "pct_above_ma20_pct": _safe_float(pct_above_ma20, 2),
                "pct_above_ma50_pct": _safe_float(pct_above_ma50, 2),
                "above_ma20": int(above_ma20),
                "above_ma50": int(above_ma50),
                "new_high_20d": int(nh20),
                "new_low_20d": int(nl20),
                "new_high_50d": int(nh50),
                "new_low_50d": int(nl50),
                # aliases for factor compatibility
                "new_high_20": int(nh20),
                "new_low_20": int(nl20),
                "new_high_50": int(nh50),
                "new_low_50": int(nl50),
                "new_high_low_ratio_20d": _safe_float(ratio20, 4),
                "new_high_low_ratio_20": _safe_float(ratio20, 4),  # alias
                "new_high_low_ratio": _safe_float(ratio50, 4),  # keep legacy key -> 50D
                "new_high_low_ratio_50d": _safe_float(ratio50, 4),
                "new_high_low_ratio_50": _safe_float(ratio50, 4),  # alias
                "ad_line": ad or {},
            }

            # coverage warnings (non-fatal)
            if total > 0 and cov_ma20 < 0.75:
                warnings.append(f"partial:ma20_coverage_low:{_safe_float(cov_ma20,4)}")
            if total > 0 and cov_ma50 < 0.75:
                warnings.append(f"partial:ma50_coverage_low:{_safe_float(cov_ma50,4)}")

            # data_status
            data_status = "OK"
            if (
                evidence.get("pct_above_ma20") is None
                or evidence.get("pct_above_ma50") is None
                or not isinstance(evidence.get("ad_line"), dict)
                or not evidence.get("ad_line")
            ):
                data_status = "PARTIAL"
            # keep PARTIAL if warnings indicate partial
            if any(w.startswith("partial:") or w.startswith("missing:") for w in warnings):
                data_status = "PARTIAL" if data_status != "MISSING" else "MISSING"

            block = {
                "schema_version": schema_version,
                "asof": {"trade_date": str(asof), "kind": kind},
                "data_status": data_status,
                "warnings": warnings,
                "error_type": error_type,
                "error_message": error_message,
                "evidence": evidence,
            }

            self._save_cache(cache_file, block)
            return block

        except Exception as e:
            error_type = type(e).__name__
            error_message = str(e)
            LOG.exception("[DS.BreadthPlus] build_block failed: %s", error_message)
            block = {
                "schema_version": schema_version,
                "asof": {"trade_date": str(trade_date), "kind": kind},
                "data_status": "ERROR",
                "warnings": warnings + ["error:breadth_plus_raw_build_failed"],
                "error_type": error_type,
                "error_message": error_message,
                "evidence": {},
            }
            self._save_cache(cache_file, block)
            return block

    # -------------------------
    # IO helpers
    # -------------------------
    def _save_cache(self, cache_file: str, block: Dict[str, Any]) -> None:
        try:
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(block, f, ensure_ascii=False)
        except Exception:
            # cache write failure is non-fatal
            pass

    def _missing_block(
        self,
        schema_version: str,
        trade_date: str,
        kind: str,
        warnings: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        return {
            "schema_version": schema_version,
            "asof": {"trade_date": str(trade_date), "kind": kind},
            "data_status": "MISSING",
            "warnings": list(warnings or []),
            "error_type": None,
            "error_message": None,
            "evidence": {},
        }

