# core/adapters/datasources/cn/market_sentiment_source.py
# -*- coding: utf-8 -*-


from datetime import date as _date, datetime as _datetime

def _to_date(v):
    """Accept date/datetime/ISO date str -> date."""
    if v is None:
        return None
    if isinstance(v, _date) and not isinstance(v, _datetime):
        return v
    if isinstance(v, _datetime):
        return v.date()
    if isinstance(v, str):
        s = v.strip()[:10]
        try:
            return _datetime.strptime(s, "%Y-%m-%d").date()
        except Exception:
            return None
    return None

import os
import json
import re
from datetime import datetime, timedelta, date
from typing import Dict, Any, Optional, List, Tuple

import pandas as pd

from core.utils.logger import get_logger
from core.datasources.datasource_base import DataSourceConfig, DataSourceBase
from core.utils.ds_refresh import apply_refresh_cleanup
from core.utils.spot_store import get_spot_daily

from core.adapters.providers.db_provider_mysql_market import DBOracleProvider

LOG = get_logger("DS.Sentiment")


class MarketSentimentDataSource(DataSourceBase):
    """
    V12 市场情绪（宽度）数据源（market_sentiment_raw）：

    - EOD：从本地 Oracle（CN_STOCK_DAILY_PRICE 聚合）获取“确认态”全市场横截面统计
    - INTRADAY：从 SpotStore 获取当日全市场 spot，再做横截面统计
    - 可选增强（近期数据）：东方财富涨停/炸板股池（AkShare）
      * stock_zt_pool_em(date)  -> 涨停股池（ztgc）
      * stock_zt_pool_zbgc_em(date) -> 炸板股池（zbgc）

    冻结约束（本文件内遵守）：
    - 不做业务判断（仅 raw 事实统计）
    - 缺数据不抛 silent exception：返回 MISSING/ERROR + warnings/error_type/error_message
    - append-only：保留历史字段（trade_date/adv/dec/flat/limit_up/limit_down/adv_ratio/window）
    """

    SCHEMA_VERSION = "market_sentiment_raw.v1"
    # 近期接口保护：东财涨停/炸板股池只能取近期
    RECENT_ONLY_DAYS = 120
    # 板块/状态涨跌停阈值（v2, Frozen / append-only）：
    # - ST（NAME 以 '*ST' 或 'ST' 开头）: 5%
    # - 20% 板块（SYMBOL 前缀 300/301/688/689）: 20%
    # - 北交所启发式（SYMBOL 前缀 8* 或 43*/83*/87*）: 30%
    # - 其它默认 10%
    BOARD20_PREFIX = {"300", "301", "688", "689"}
    BOARD30_PREFIX_1 = {"8"}
    BOARD30_PREFIX_2 = {"43", "83", "87"}

    LIMIT_PCT_DEFAULT = 10.0
    LIMIT_PCT_20 = 20.0
    LIMIT_PCT_30 = 30.0
    LIMIT_PCT_ST = 5.0

    LIMIT_TOL = 0.05  # 0.05% tolerance for rounding boundary
    # down-limit lock proxy (EOD) params (Frozen):
    # - tol is expressed in *fraction* (e.g. 0.002 == 0.20%)
    # - stuck quantile uses amount/volume within limit-down proxy group
    LOCK_PROXY_TOL_FRAC = 0.002
    LOCK_PROXY_STUCK_QUANTILE = 0.20

    def __init__(self, config: DataSourceConfig, is_intraday: bool = False):
        super().__init__(name="DS.Sentiment")
        self.config = config
        self.cache_root = config.cache_root
        self.history_root = config.history_root
        self.db = DBOracleProvider()
        self.is_intraday = is_intraday

        os.makedirs(self.cache_root, exist_ok=True)
        os.makedirs(self.history_root, exist_ok=True)

        LOG.info(
            "[DS.Sentiment] Init: market=%s ds=%s cache_root=%s history_root=%s intraday=%s",
            config.market,
            config.ds_name,
            self.cache_root,
            self.history_root,
            self.is_intraday,
        )

    # ------------------------------------------------------------
    def build_block(self, trade_date: str, refresh_mode: str = "none") -> Dict[str, Any]:
        """V12 统一入口（支持 cache）。"""
        cache_file = os.path.join(self.cache_root, f"sentiment_{trade_date}.json")

        _ = apply_refresh_cleanup(
            refresh_mode=refresh_mode,
            cache_path=cache_file,
            history_path=None,
            spot_path=None,
        )

        if refresh_mode in ("none", "readonly") and os.path.exists(cache_file):
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                LOG.error("[DS.Sentiment] load cache error: %s", e)

        # daily window：intraday 也需要 window，但不必重复拉涨停/炸板池（避免重复网络）
        daily_series_block = self.build_daily_series_block(trade_date, attach_zt_zb_pool=(not self.is_intraday))

        if not self.is_intraday:
            block = daily_series_block
        else:
            block = self.build_intraday_block(trade_date, refresh_mode=refresh_mode)
            block["window"] = daily_series_block.get("window", [])

        try:
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(block, f, ensure_ascii=False, indent=2)
        except Exception as e:
            LOG.error("[DS.Sentiment] save cache error: %s", e)

        return block

    # ------------------------------------------------------------
    def build_daily_series_block(self, trade_date: str, attach_zt_zb_pool: bool = True) -> Dict[str, Any]:
        """EOD：从 Oracle 聚合得到当日与 20D window。"""
        warnings: List[str] = []
        error_type: Optional[str] = None
        error_message: Optional[str] = None

        look_back_days = 20

        try:
            df: pd.DataFrame = self.db.fetch_stock_daily_chg_pct_raw(
                start_date=trade_date,
                look_back_days=look_back_days,
            )
        except Exception as e:
            LOG.error("[DS.Sentiment] oracle fetch error: %s", e)
            error_type = type(e).__name__
            error_message = str(e)
            return self._neutral_block(
                trade_date=trade_date,
                kind="EOD",
                data_status="ERROR",
                warnings=["error:oracle_fetch_failed"],
                error_type=error_type,
                error_message=error_message,
            )

        if df is None or df.empty:
            return self._neutral_block(
                trade_date=trade_date,
                kind="EOD",
                data_status="MISSING",
                warnings=["empty:oracle_agg_df"],
            )

        # 降序已由 SQL 保证，最新在前
        recent_df = df.head(20).copy()

        latest_row = recent_df.iloc[0]
        latest_trade_date = pd.to_datetime(latest_row["trade_date"]).strftime("%Y-%m-%d")

        # base metrics
        try:
            current_adv = int(latest_row.get("adv", 0))
            current_dec = int(latest_row.get("dec", 0))
            current_flat = int(latest_row.get("flat", 0))
            current_limit_up = int(latest_row.get("limit_up", 0))
            current_limit_down = int(latest_row.get("limit_down", 0))
            current_total = int(latest_row.get("total_stocks", current_adv + current_dec + current_flat))
            current_adv_ratio = float(latest_row.get("adv_ratio", 0.0))  # percent (0~100)
        except Exception as e:
            error_type = type(e).__name__
            error_message = str(e)
            return self._neutral_block(
                trade_date=latest_trade_date,
                kind="EOD",
                data_status="ERROR",
                warnings=["error:oracle_row_parse_failed"],
                error_type=error_type,
                error_message=error_message,
            )

        # window
        window = []
        for _, row in recent_df.iterrows():
            td = pd.to_datetime(row["trade_date"]).strftime("%Y-%m-%d")
            window.append(
                {
                    "trade_date": td,
                    "adv": int(row.get("adv", 0)),
                    "dec": int(row.get("dec", 0)),
                    "flat": int(row.get("flat", 0)),
                    "limit_up": int(row.get("limit_up", 0)),
                    "limit_down": int(row.get("limit_down", 0)),
                    "adv_ratio": round(float(row.get("adv_ratio", 0.0)), 2),  # percent
                }
            )

        # enhance: zt/zb pools (recent only; failures won't break base)
        zt_pool = None
        zb_pool = None
        broken_limit_rate_std = None
        broken_limit_rate_proxy = None

        if attach_zt_zb_pool:
            zt_pool = self._build_zt_pool_stats(latest_trade_date)
            zb_pool = self._build_zb_pool_stats(latest_trade_date)

            zt_cnt = self._safe_int(zt_pool, ["evidence", "count"])
            zb_cnt = self._safe_int(zb_pool, ["evidence", "count"])

            if zt_cnt is not None and zb_cnt is not None:
                den = zt_cnt + zb_cnt
                if den > 0:
                    broken_limit_rate_std = round(zb_cnt / den, 4)
                else:
                    broken_limit_rate_std = 0.0
                    warnings.append("empty:zt_zb_denominator_0")

            # proxy from zt pool: ratio of opened-in-limitup-pool
            if zt_pool and zt_pool.get("data_status") == "OK":
                opened_ratio = self._safe_float(zt_pool, ["evidence", "opened_limitup_ratio_proxy"])
                if opened_ratio is not None:
                    broken_limit_rate_proxy = opened_ratio

            # propagate pool warnings
            for sub in (zt_pool, zb_pool):
                if sub and sub.get("data_status") in ("PARTIAL", "MISSING", "ERROR"):
                    warnings.extend(sub.get("warnings", []))


        # enhance: down-limit queue strength (MISSING in v1: no L2 order queue source)
        down_limit_queue_strength = self._wrap_subblock(
            name="down_limit_queue_strength",
            trade_date=latest_trade_date,
            kind="EOD",
            data_status="MISSING",
            warnings=["missing:down_limit_queue_strength_no_l2_order_queue_source"],
            evidence={},
        )
        warnings.append("missing:down_limit_queue_strength_no_l2_order_queue_source")

        # enhance: down-limit lock proxy (EOD proxy from CN_STOCK_DAILY_PRICE; NOT equal to L2 order-book queue strength)
        # Frozen rules:
        # - No L2 => DO NOT fabricate queue_strength
        # - Use EOD cross-section: close/pre_close (+ amount/volume) to build a *lock/stuck* proxy
        # - Missing fields => PARTIAL/MISSING with warnings (never silent)
        down_limit_lock_proxy, lock_top_warnings = self._build_down_limit_lock_proxy_eod(trade_date=latest_trade_date)
        warnings.extend(lock_top_warnings)

        # overall status
        data_status = "OK"
        if attach_zt_zb_pool:
            if (zt_pool and zt_pool.get("data_status") != "OK") or (zb_pool and zb_pool.get("data_status") != "OK"):
                data_status = "PARTIAL"

        warnings.append("policy:st_limit_pct=5_mainboard_only_by_name_prefix(*ST|ST)_if_name_available_else_assume_nonst")
        warnings.append("policy:board_limit_pct_20_by_prefix(300/301/688/689)_30_by_prefix(8/43/83/87)_else_10")
        warnings.append("policy:eod_limit_updown_by_limit_price_hit_round2_from_pre_close_close")
        # NOTE: limit_up/down for EOD are computed by *limit-price hit* in DB provider (NOT by CHG_PCT threshold)

        evidence = {
            "total_stocks": current_total,
            "adv": current_adv,
            "dec": current_dec,
            "flat": current_flat,
            "limit_up": current_limit_up,
            "limit_down": current_limit_down,
            "adv_ratio": current_adv_ratio,  # percent
            "window": window,
            "down_limit_queue_strength": down_limit_queue_strength,
            "down_limit_lock_proxy": down_limit_lock_proxy,
        }
        if attach_zt_zb_pool:
            evidence.update(
                {
                    "zt_pool": zt_pool,
                    "zb_pool": zb_pool,
                    "broken_limit_rate_std": broken_limit_rate_std,
                    "broken_limit_rate_proxy": broken_limit_rate_proxy,
                }
            )

        legacy = {
            "trade_date": latest_trade_date,
            "adv": current_adv,
            "dec": current_dec,
            "flat": current_flat,
            "limit_up": current_limit_up,
            "limit_down": current_limit_down,
            "adv_ratio": current_adv_ratio,
            "window": window,
        }

        return self._wrap_raw(
            trade_date=latest_trade_date,
            kind="EOD",
            data_status=data_status,
            warnings=self._dedup_warnings(warnings),
            error_type=error_type,
            error_message=error_message,
            evidence=evidence,
            legacy=legacy,
        )


    def _build_down_limit_lock_proxy_eod(self, trade_date: str) -> Tuple[Dict[str, Any], List[str]]:
        """Build an EOD *proxy* for down-limit lock / stuck-ness.

        Why proxy:
        - We do NOT have L2 order-book queue data in V12 daily pipeline.
        - We only have EOD price + amount/volume.

        Proxy definition (Frozen / append-only):
        - Identify limit-down candidates by close/pre_close vs dynamic limit (10% or 20%) with tolerance.
        - Within limit-down group, "stuck" is approximated by low liquidity (amount/volume bottom quantile).

        Returns:
            (subblock, top_level_warnings)
        """
        top_warnings: List[str] = []
        sub_warnings: List[str] = []

        # ST flag:
        # - if NAME column exists: derive ST via prefix '*ST' or 'ST'
        # - else: fallback assume non-ST (append warnings)

        table = getattr(self.db, "tables", {}).get("stock_daily") if hasattr(self.db, "tables") else None
        schema = getattr(self.db, "schema", None)
        if not table or not schema:
            sub = self._wrap_subblock(
                name="down_limit_lock_proxy",
                trade_date=trade_date,
                kind="EOD",
                data_status="ERROR",
                warnings=["error:oracle_stock_daily_table_not_configured"],
                evidence={},
                error_type="RuntimeError",
                error_message="db.oracle.tables.stock_daily or schema not configured",
            )
            return sub, ["error:oracle_stock_daily_table_not_configured"] + top_warnings

                # Try query with (NAME, amount, volume) -> fallback gracefully if some columns don't exist.
        # - NAME is used to derive ST flag via prefix '*ST' / 'ST'
        # - amount/volume are used only for "stuck" liquidity proxy
        sql_tpl = """
        SELECT
            SYMBOL    AS symbol,
            PRE_CLOSE AS pre_close,
            CLOSE     AS close{extra_cols}
        FROM {schema}.{table}
        WHERE TRADE_DATE = :trade_date
        """.strip()

        raw = None
        used_cols = None
        has_name = False

        attempts = [
            (", NAME AS name, AMOUNT AS amount, VOLUME AS volume", ["symbol", "pre_close", "close", "name", "amount", "volume"], []),
            (", NAME AS name, AMOUNT AS amount", ["symbol", "pre_close", "close", "name", "amount"], ["missing:volume_col_unavailable_in_stock_daily"]),
            (", NAME AS name", ["symbol", "pre_close", "close", "name"], ["missing:amount_volume_cols_unavailable_in_stock_daily"]),
            (", AMOUNT AS amount, VOLUME AS volume", ["symbol", "pre_close", "close", "amount", "volume"], ["missing:name_col_unavailable_in_stock_daily", "assumption:st_flag_unavailable_assume_nonst"]),
            (", AMOUNT AS amount", ["symbol", "pre_close", "close", "amount"], ["missing:name_col_unavailable_in_stock_daily", "assumption:st_flag_unavailable_assume_nonst", "missing:volume_col_unavailable_in_stock_daily"]),
            ("", ["symbol", "pre_close", "close"], ["missing:name_col_unavailable_in_stock_daily", "assumption:st_flag_unavailable_assume_nonst", "missing:amount_volume_cols_unavailable_in_stock_daily"]),
        ]

        last_exc = None
        for extra_cols, cols, warn_list in attempts:
            try:
                sql = sql_tpl.format(schema=schema, table=table, extra_cols=extra_cols)
                raw = self.db.execute(sql, {"trade_date": _to_date(trade_date)})
                used_cols = cols
                has_name = ("name" in cols)
                for w in warn_list:
                    top_warnings.append(w)
                    sub_warnings.append(w)
                break
            except Exception as e:
                last_exc = e
                continue

        if raw is None:
            sub = self._wrap_subblock(
                name="down_limit_lock_proxy",
                trade_date=trade_date,
                kind="EOD",
                data_status="ERROR",
                warnings=["error:down_limit_lock_proxy_query_failed"],
                evidence={},
                error_type=type(last_exc).__name__ if last_exc else "RuntimeError",
                error_message=str(last_exc) if last_exc else "query attempts exhausted",
            )
            return sub, ["error:down_limit_lock_proxy_query_failed"] + top_warnings



        if not raw:
            sub = self._wrap_subblock(
                name="down_limit_lock_proxy",
                trade_date=trade_date,
                kind="EOD",
                data_status="MISSING",
                warnings=["empty:stock_daily_xsection"],
                evidence={},
            )
            return sub, ["empty:stock_daily_xsection"] + top_warnings

        import pandas as pd

        df = pd.DataFrame(raw, columns=used_cols)
        # normalize columns
        if "symbol" not in df.columns:
            sub = self._wrap_subblock(
                name="down_limit_lock_proxy",
                trade_date=trade_date,
                kind="EOD",
                data_status="ERROR",
                warnings=["error:stock_daily_symbol_col_missing"],
                evidence={},
                error_type="KeyError",
                error_message="missing symbol col",
            )
            return sub, ["error:stock_daily_symbol_col_missing"] + top_warnings

        df["symbol"] = df["symbol"].astype(str).str.strip()
        for c in ("pre_close", "close", "amount", "volume"):
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        # valid rows for chg proxy
        xs_total = int(len(df))
        valid = df[(df.get("pre_close") > 0) & (df.get("close").notna())].copy()
        valid_total = int(len(valid))
        if valid_total <= 0:
            sub = self._wrap_subblock(
                name="down_limit_lock_proxy",
                trade_date=trade_date,
                kind="EOD",
                data_status="MISSING",
                warnings=["empty:stock_daily_valid_pre_close_close"],
                evidence={"xs_total": xs_total},
            )
            return sub, ["empty:stock_daily_valid_pre_close_close"] + top_warnings

        invalid_cnt = xs_total - valid_total
        if invalid_cnt > 0:
            top_warnings.append("missing:pre_close_or_close_invalid_for_lock_proxy")
            sub_warnings.append("missing:pre_close_or_close_invalid_for_lock_proxy")

        # compute chg_pct proxy
        valid["chg_pct_proxy"] = valid["close"] / valid["pre_close"] - 1.0

        # dynamic limit pct per board + ST (append-only):
        # - default 10%
        # - 20%: prefix 300/301/688/689
        # - 30% (heuristic): prefix 8* or 43*/83*/87*
        # - ST (NAME '*ST'/'ST'): 5% (mainboard only; does NOT override 20%/30% boards)
        sym = valid["symbol"].astype(str).str.strip()
        prefix3 = sym.str[:3]
        prefix2 = sym.str[:2]
        prefix1 = sym.str[:1]

        limit_pct = pd.Series(self.LIMIT_PCT_DEFAULT, index=valid.index, dtype="float")
        limit_pct = limit_pct.where(~prefix3.isin(self.BOARD20_PREFIX), self.LIMIT_PCT_20)
        limit_pct = limit_pct.where(~(prefix1.isin(self.BOARD30_PREFIX_1) | prefix2.isin(self.BOARD30_PREFIX_2)), self.LIMIT_PCT_30)

        st_flag = "assumed_nonst"
        st_count = None
        if has_name and "name" in valid.columns:
            nm = valid["name"].astype(str).str.strip().str.upper()
            is_st = nm.str.startswith("*ST") | nm.str.startswith("ST")
            # Apply ST 5% only when the stock is on mainboard default limit (10%).
            is_mainboard = (limit_pct == float(self.LIMIT_PCT_DEFAULT))
            st_applied = is_st & is_mainboard
            st_count = int(st_applied.sum())
            st_flag = "derived_from_name_prefix_mainboard_only"
            limit_pct = limit_pct.where(~st_applied, self.LIMIT_PCT_ST)

        limit_frac = limit_pct / 100.0
        tol = float(self.LOCK_PROXY_TOL_FRAC)
        thr = -(limit_frac - tol)
        is_limit_down = valid["chg_pct_proxy"] <= thr

        ld_cnt = int(is_limit_down.sum())
        ld_ratio = round(ld_cnt * 100.0 / valid_total, 2) if valid_total > 0 else 0.0

        # stuck proxy within limit-down group
        stuck_cnt = 0
        stuck_ratio = 0.0
        liquidity_field_used = "none"
        data_status = "OK"

        if ld_cnt <= 0:
            sub_warnings.append("empty:limit_down_proxy_count_0")
        else:
            ld_df = valid[is_limit_down].copy()
            q = float(self.LOCK_PROXY_STUCK_QUANTILE)

            def _try_stuck(series_name: str) -> bool:
                nonlocal stuck_cnt, liquidity_field_used
                if series_name not in ld_df.columns:
                    return False
                s = pd.to_numeric(ld_df[series_name], errors="coerce")
                if s.isna().all():
                    return False
                try:
                    cutoff = float(s.dropna().astype(float).quantile(q))
                except Exception:
                    return False
                stuck_cnt = int((s.astype(float) <= cutoff).sum())
                liquidity_field_used = series_name
                return True

            ok = _try_stuck("amount")
            if not ok:
                if "amount" not in used_cols:
                    sub_warnings.append("missing:amount_col_unavailable_fallback_volume")
                    top_warnings.append("missing:amount_col_unavailable_fallback_volume")
                else:
                    sub_warnings.append("missing:amount_all_nan_fallback_volume")
                    top_warnings.append("missing:amount_all_nan_fallback_volume")
                ok2 = _try_stuck("volume")
                if not ok2:
                    sub_warnings.append("missing:amount_volume_unavailable_skip_stuck_proxy")
                    top_warnings.append("missing:amount_volume_unavailable_skip_stuck_proxy")
                    data_status = "PARTIAL"

            stuck_ratio = round(stuck_cnt * 100.0 / valid_total, 2) if valid_total > 0 else 0.0

        # record proxy meta
        evidence = {
            "method": "CHG_PCT_PROXY",
            "limit_rule": "st5_by_name_prefix(*ST|ST)_20_by_prefix(300/301/688/689)_30_by_prefix(8/43/83/87)_else_10",
            "tol": float(self.LOCK_PROXY_TOL_FRAC),
            "st_flag": st_flag,
            "st_count": st_count,
            "stuck_quantile": float(self.LOCK_PROXY_STUCK_QUANTILE),
            "liquidity_field_used": liquidity_field_used,
            "xs_total": xs_total,
            "total_stocks": valid_total,
            "limit_down_count": ld_cnt,
            "limit_down_ratio_pct": ld_ratio,
            "stuck_locked_count": stuck_cnt,
            "stuck_locked_ratio_pct": stuck_ratio,
        }

        # put one explicit proxy note
        sub_warnings.insert(0, "proxy:down_limit_lock_from_eod_chg_pct_and_amount_volume")

        sub = self._wrap_subblock(
            name="down_limit_lock_proxy",
            trade_date=trade_date,
            kind="EOD",
            data_status=data_status,
            warnings=self._dedup_warnings(sub_warnings),
            evidence=evidence,
        )

        return sub, self._dedup_warnings(top_warnings)

    # ------------------------------------------------------------
    def build_intraday_block(self, trade_date: str, refresh_mode: str) -> Dict[str, Any]:
        """INTRADAY：从 SpotStore spot 统计当日横截面。"""
        warnings: List[str] = []
        error_type: Optional[str] = None
        error_message: Optional[str] = None

        try:
            df: pd.DataFrame = get_spot_daily(trade_date, refresh_mode=refresh_mode)
        except Exception as e:
            LOG.error("[DS.Sentiment] get_spot_daily error: %s", e)
            error_type = type(e).__name__
            error_message = str(e)
            return self._neutral_block(
                trade_date=trade_date,
                kind="INTRADAY",
                data_status="ERROR",
                warnings=["error:get_spot_daily_failed"],
                error_type=error_type,
                error_message=error_message,
            )

        if df is None or df.empty:
            return self._neutral_block(
                trade_date=trade_date,
                kind="INTRADAY",
                data_status="MISSING",
                warnings=["empty:spot_df"],
            )

        if "涨跌幅" not in df.columns:
            return self._neutral_block(
                trade_date=trade_date,
                kind="INTRADAY",
                data_status="ERROR",
                warnings=["missing:spot_chg_pct_col"],
                error_type="KeyError",
                error_message="missing column 涨跌幅",
            )

        chg = pd.to_numeric(df["涨跌幅"], errors="coerce")
        if chg.isna().all():
            return self._neutral_block(
                trade_date=trade_date,
                kind="INTRADAY",
                data_status="ERROR",
                warnings=["error:spot_chg_pct_all_nan"],
                error_type="ValueError",
                error_message="spot chg_pct all NaN",
            )

        # normalize unit: ratio(0.032) vs percent(3.2)
        max_abs = float(chg.abs().max())
        if max_abs <= 1.0:
            chg = chg * 100.0
            warnings.append("normalize:chg_pct_ratio_to_percent")

        adv = int((chg > 0).sum())
        dec = int((chg < 0).sum())
        flat = int((chg == 0).sum())

        # board-aware limit up/down
        symbol_col = self._pick_col(df, ["代码", "symbol", "证券代码", "股票代码", "ts_code", "代码6"])
        if symbol_col is None:
            warnings.append("missing:symbol_col_limit_by_prefix_fallback_9.9")
            data_status = "PARTIAL"
            limit_up = int((chg >= (9.9 - self.LIMIT_TOL)).sum())
            limit_down = int((chg <= -(9.9 - self.LIMIT_TOL)).sum())
        else:
            sym6 = df[symbol_col].astype(str).str.extract(r"(\d{6})", expand=False)
            prefix3 = sym6.str[:3]
            prefix2 = sym6.str[:2]
            prefix1 = sym6.str[:1]

            limit_pct = pd.Series(self.LIMIT_PCT_DEFAULT, index=df.index, dtype="float")
            limit_pct = limit_pct.where(~prefix3.isin(self.BOARD20_PREFIX), self.LIMIT_PCT_20)
            limit_pct = limit_pct.where(~(prefix1.isin(self.BOARD30_PREFIX_1) | prefix2.isin(self.BOARD30_PREFIX_2)), self.LIMIT_PCT_30)

            name_col = self._pick_col(df, ["名称", "name", "股票简称", "证券简称"])
            if name_col is not None:
                nm = df[name_col].astype(str).str.strip().str.upper()
                is_st = nm.str.startswith("*ST") | nm.str.startswith("ST")
                # Apply ST 5% only when the stock is on mainboard default limit (10%).
                is_mainboard = (limit_pct == float(self.LIMIT_PCT_DEFAULT))
                st_applied = is_st & is_mainboard
                limit_pct = limit_pct.where(~st_applied, self.LIMIT_PCT_ST)
                warnings.append("policy:intraday_st_limit_pct=5_mainboard_only_by_name_prefix(*ST|ST)")
            else:
                warnings.append("assumption:intraday_st_flag_unavailable_assume_nonst")

            limit_up = int((chg >= (limit_pct - self.LIMIT_TOL)).sum())
            limit_down = int((chg <= -(limit_pct - self.LIMIT_TOL)).sum())
            data_status = "OK"
            if sym6.isna().any():
                data_status = "PARTIAL"
                warnings.append("partial:symbol_extract_failed_some_rows")

        total = adv + dec + flat
        adv_ratio = round(adv * 100.0 / total, 2) if total > 0 else 0.0  # percent

        # zt/zb pools (recent only; failures won't break base)
        zt_pool = self._build_zt_pool_stats(trade_date)
        zb_pool = self._build_zb_pool_stats(trade_date)

        zt_cnt = self._safe_int(zt_pool, ["evidence", "count"])
        zb_cnt = self._safe_int(zb_pool, ["evidence", "count"])
        broken_limit_rate_std = None
        broken_limit_rate_proxy = None
        if zt_cnt is not None and zb_cnt is not None:
            den = zt_cnt + zb_cnt
            broken_limit_rate_std = round((zb_cnt / den), 4) if den > 0 else 0.0
        opened_ratio = self._safe_float(zt_pool, ["evidence", "opened_limitup_ratio_proxy"])
        if opened_ratio is not None:
            broken_limit_rate_proxy = opened_ratio

        for sub in (zt_pool, zb_pool):
            if sub and sub.get("data_status") in ("PARTIAL", "MISSING", "ERROR"):
                warnings.extend(sub.get("warnings", []))
                if data_status == "OK":
                    data_status = "PARTIAL"

        warnings.append("policy:board_limit_pct_20_by_prefix(300/301/688/689)_30_by_prefix(8/43/83/87)_else_10")
        warnings.append("assumption:intraday_limit_updown_by_chg_pct_threshold_proxy_not_limit_price")
        warnings.append("missing:down_limit_queue_strength_no_l2_order_queue_source")
        warnings.append("missing:down_limit_lock_proxy_eod_only")

        evidence = {
            "adv": adv,
            "dec": dec,
            "flat": flat,
            "limit_up": limit_up,
            "limit_down": limit_down,
            "adv_ratio": adv_ratio,
            "zt_pool": zt_pool,
            "zb_pool": zb_pool,
            "broken_limit_rate_std": broken_limit_rate_std,
            "broken_limit_rate_proxy": broken_limit_rate_proxy,
            "down_limit_queue_strength": self._wrap_subblock(
                name="down_limit_queue_strength",
                trade_date=trade_date,
                kind="INTRADAY",
                data_status="MISSING",
                warnings=["missing:down_limit_queue_strength_no_l2_order_queue_source"],
                evidence={},
            ),
            "down_limit_lock_proxy": self._wrap_subblock(
                name="down_limit_lock_proxy",
                trade_date=trade_date,
                kind="INTRADAY",
                data_status="MISSING",
                warnings=["missing:down_limit_lock_proxy_eod_only"],
                evidence={},
            ),
        }

        legacy = {
            "trade_date": trade_date,
            "adv": adv,
            "dec": dec,
            "flat": flat,
            "limit_up": limit_up,
            "limit_down": limit_down,
            "adv_ratio": adv_ratio,
        }

        return self._wrap_raw(
            trade_date=trade_date,
            kind="INTRADAY",
            data_status=data_status,
            warnings=self._dedup_warnings(warnings),
            error_type=error_type,
            error_message=error_message,
            evidence=evidence,
            legacy=legacy,
        )

    # ------------------------------------------------------------
    # EM pools (recent only)
    # ------------------------------------------------------------
    def _build_zt_pool_stats(self, trade_date: str) -> Dict[str, Any]:
        warnings: List[str] = []
        if not self._is_recent(trade_date, days=self.RECENT_ONLY_DAYS):
            return {
                "schema_version": "em_zt_pool.v1",
                "asof": {"trade_date": trade_date, "kind": "EOD"},
                "data_status": "MISSING",
                "warnings": [f"skip:zt_pool_recent_only_{self.RECENT_ONLY_DAYS}d"],
                "error_type": None,
                "error_message": None,
                "evidence": {"count": None},
            }

        date_em = trade_date.replace("-", "")
        try:
            import akshare as ak  # local import: avoid hard dependency at module import time
            df = ak.stock_zt_pool_em(date=date_em)
        except Exception as e:
            return {
                "schema_version": "em_zt_pool.v1",
                "asof": {"trade_date": trade_date, "kind": "EOD"},
                "data_status": "ERROR",
                "warnings": ["error:zt_pool_fetch_failed"],
                "error_type": type(e).__name__,
                "error_message": str(e),
                "evidence": {"count": None},
            }

        if df is None:
            return {
                "schema_version": "em_zt_pool.v1",
                "asof": {"trade_date": trade_date, "kind": "EOD"},
                "data_status": "MISSING",
                "warnings": ["empty:zt_pool_df_none"],
                "error_type": None,
                "error_message": None,
                "evidence": {"count": None},
            }

        cnt = int(len(df))
        evidence: Dict[str, Any] = {"count": cnt}

        # 连板高度
        if "连板数" in df.columns:
            try:
                evidence["max_consecutive_limit_up"] = int(pd.to_numeric(df["连板数"], errors="coerce").max())
            except Exception:
                warnings.append("parse_failed:zt_pool_max_consecutive")
        else:
            warnings.append("missing:zt_pool_col_连板数")

        # 封板资金强度
        if "封板资金" in df.columns:
            x = pd.to_numeric(df["封板资金"], errors="coerce").dropna()
            if not x.empty:
                evidence["seal_fund_total"] = float(x.sum())
                evidence["seal_fund_median"] = float(x.median())
                evidence["seal_fund_top10_sum"] = float(x.sort_values(ascending=False).head(10).sum())
            else:
                warnings.append("empty:zt_pool_seal_fund_all_nan")
        else:
            warnings.append("missing:zt_pool_col_封板资金")

        # 炸板 proxy：涨停池内炸板次数>0 占比
        if "炸板次数" in df.columns:
            z = pd.to_numeric(df["炸板次数"], errors="coerce").fillna(0)
            opened_cnt = int((z > 0).sum())
            evidence["opened_limitup_count_proxy"] = opened_cnt
            evidence["opened_limitup_ratio_proxy"] = round(opened_cnt / cnt, 4) if cnt > 0 else 0.0
        else:
            warnings.append("missing:zt_pool_col_炸板次数")

        data_status = "OK"
        if warnings:
            data_status = "PARTIAL"

        return {
            "schema_version": "em_zt_pool.v1",
            "asof": {"trade_date": trade_date, "kind": "EOD"},
            "data_status": data_status,
            "warnings": warnings,
            "error_type": None,
            "error_message": None,
            "evidence": evidence,
        }

    def _build_zb_pool_stats(self, trade_date: str) -> Dict[str, Any]:
        warnings: List[str] = []
        if not self._is_recent(trade_date, days=self.RECENT_ONLY_DAYS):
            return {
                "schema_version": "em_zb_pool.v1",
                "asof": {"trade_date": trade_date, "kind": "EOD"},
                "data_status": "MISSING",
                "warnings": [f"skip:zb_pool_recent_only_{self.RECENT_ONLY_DAYS}d"],
                "error_type": None,
                "error_message": None,
                "evidence": {"count": None},
            }

        date_em = trade_date.replace("-", "")
        try:
            import akshare as ak
            df = ak.stock_zt_pool_zbgc_em(date=date_em)
        except Exception as e:
            return {
                "schema_version": "em_zb_pool.v1",
                "asof": {"trade_date": trade_date, "kind": "EOD"},
                "data_status": "ERROR",
                "warnings": ["error:zb_pool_fetch_failed"],
                "error_type": type(e).__name__,
                "error_message": str(e),
                "evidence": {"count": None},
            }

        if df is None:
            return {
                "schema_version": "em_zb_pool.v1",
                "asof": {"trade_date": trade_date, "kind": "EOD"},
                "data_status": "MISSING",
                "warnings": ["empty:zb_pool_df_none"],
                "error_type": None,
                "error_message": None,
                "evidence": {"count": None},
            }

        cnt = int(len(df))
        evidence: Dict[str, Any] = {"count": cnt}

        if "炸板次数" in df.columns:
            z = pd.to_numeric(df["炸板次数"], errors="coerce").fillna(0)
            evidence["broken_times_sum"] = int(z.sum())
            evidence["broken_times_max"] = int(z.max()) if len(z) else 0
        else:
            warnings.append("missing:zb_pool_col_炸板次数")

        # 首次封板时间分布（可选）
        if "首次封板时间" in df.columns:
            try:
                t = df["首次封板时间"].astype(str)
                # normalize to HH:MM:SS if possible
                t2 = t.str.replace(":", "", regex=False).str.strip()
                evidence["first_seal_0925_count"] = int((t2 == "092500").sum())
            except Exception:
                warnings.append("parse_failed:zb_pool_first_seal_time")
        else:
            warnings.append("missing:zb_pool_col_首次封板时间")

        data_status = "OK"
        if warnings:
            data_status = "PARTIAL"

        return {
            "schema_version": "em_zb_pool.v1",
            "asof": {"trade_date": trade_date, "kind": "EOD"},
            "data_status": data_status,
            "warnings": warnings,
            "error_type": None,
            "error_message": None,
            "evidence": evidence,
        }

    # ------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------
    def _wrap_raw(
        self,
        trade_date: str,
        kind: str,
        data_status: str,
        warnings: List[str],
        error_type: Optional[str],
        error_message: Optional[str],
        evidence: Dict[str, Any],
        legacy: Dict[str, Any],
    ) -> Dict[str, Any]:
        block: Dict[str, Any] = {
            "schema_version": self.SCHEMA_VERSION,
            "asof": {"trade_date": trade_date, "kind": kind},
            "data_status": data_status,
            "warnings": warnings or [],
            "error_type": error_type,
            "error_message": error_message,
            "evidence": evidence or {},
        }
        # append-only legacy fields
        block.update(legacy or {})
        return block


    def _wrap_subblock(
        self,
        name: str,
        trade_date: str,
        kind: str,
        data_status: str,
        warnings: List[str],
        evidence: Optional[Dict[str, Any]] = None,
        error_type: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> Dict[str, Any]:
        """稳定子块封装（append-only）。"""
        return {
            "schema_version": f"{name}.v1",
            "asof": {"trade_date": trade_date, "kind": kind},
            "data_status": data_status,
            "warnings": warnings or [],
            "error_type": error_type,
            "error_message": error_message,
            "evidence": evidence or {},
        }

    def _neutral_block(
        self,
        trade_date: str,
        kind: str,
        data_status: str,
        warnings: List[str],
        error_type: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> Dict[str, Any]:
        legacy = {
            "trade_date": trade_date,
            "adv": 0,
            "dec": 0,
            "flat": 0,
            "limit_up": 0,
            "limit_down": 0,
            "adv_ratio": 0.0,
            "window": [],
        }
        evidence = {
            "adv": 0,
            "dec": 0,
            "flat": 0,
            "limit_up": 0,
            "limit_down": 0,
            "adv_ratio": 0.0,
            "window": [],
        }
        return self._wrap_raw(
            trade_date=trade_date,
            kind=kind,
            data_status=data_status,
            warnings=warnings or [],
            error_type=error_type,
            error_message=error_message,
            evidence=evidence,
            legacy=legacy,
        )

    def _pick_col(self, df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
        cols = set(getattr(df, "columns", []) or [])
        for c in candidates:
            if c in cols:
                return c
        return None

    def _is_recent(self, trade_date: str, days: int) -> bool:
        try:
            td = pd.to_datetime(trade_date).date()
        except Exception:
            return False
        return (date.today() - td) <= timedelta(days=days)

    def _dedup_warnings(self, ws: List[str]) -> List[str]:
        out = []
        seen = set()
        for w in ws or []:
            if not w:
                continue
            if w in seen:
                continue
            seen.add(w)
            out.append(w)
        return out

    def _safe_int(self, obj: Any, path: List[str]) -> Optional[int]:
        try:
            x = obj
            for k in path:
                x = x.get(k) if isinstance(x, dict) else None
            if x is None:
                return None
            return int(x)
        except Exception:
            return None

    def _safe_float(self, obj: Any, path: List[str]) -> Optional[float]:
        try:
            x = obj
            for k in path:
                x = x.get(k) if isinstance(x, dict) else None
            if x is None:
                return None
            return float(x)
        except Exception:
            return None

