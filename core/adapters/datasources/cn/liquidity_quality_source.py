# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - Liquidity Quality DataSource (F Block)

鍔熻兘锛?
    璁＄畻鍏ㄥ競鍦烘祦鍔ㄦ€ц川閲忕浉鍏虫寚鏍囷紝鍖呮嫭锛?
      - Top20 鎴愪氦闆嗕腑搴︼紙鍓?20 鍚嶆垚浜ら鍗犳瘮锛?
      - 澶?灏忕洏鎴愪氦鍗犳瘮锛堟寜鑲＄エ浠ｇ爜鍓嶇紑鍒掑垎澶х洏涓庡皬鐩橈級
      - 缂╅噺涓嬭穼姣旓紙涓嬭穼鑲＄エ涓垚浜ら浣庝簬鑷韩杩?20 鏃ュ潎棰濈殑姣斾緥锛?

瀹炵幇瑕佺偣锛?
    1. 浠?Oracle 涓殑 CN_STOCK_DAILY_PRICE 琛ㄨ鍙栨寚瀹氬洖婧獥鍙ｅ唴鐨勮偂绁ㄦ棩绾挎暟鎹細
         symbol, exchange, trade_date, chg_pct, amount
       閫氳繃 DBOracleProvider.query_stock_closes 鎺ュ彛鑾峰彇銆?
    2. 鎸夎偂绁ㄩ€愪竴璁＄畻 20 鏃ユ粴鍔ㄥ钩鍧囨垚浜ら锛堝惈褰撳墠鏃ワ級銆?
    3. 鍦ㄦ墍閫夌獥鍙ｅ唴锛堥粯璁?60 鏃ワ級锛岄€愭棩缁熻涓婅堪鎸囨爣锛?
         * Top20 鎴愪氦闆嗕腑搴?= sum(top 20 amount) / sum(total amount)
         * 澶?灏忕洏鎴愪氦鍗犳瘮 = sum(amount of big-cap) / sum(amount of small-cap)
           澶х洏鑲＄エ鐨勫垽鏂熀浜庤偂绁ㄤ唬鐮佸墠缂€锛?00/601/603锛夛細
               鑻?symbol 浠?"60", "601", "603" 寮€澶村垯瑙嗕负澶х洏锛涘惁鍒欒涓哄皬鐩樸€?
         * 缂╅噺涓嬭穼姣?= count(chg_pct < 0 & amount < ma20_amount) / count(chg_pct < 0)
    4. 璁＄畻 10 鏃ヨ秼鍔垮拰 3 鏃ュ姞閫熷害锛堝彇鍚勬寚鏍囪繎 10 鏃?3 鏃ュ樊鍊硷級銆?
    5. 杈撳嚭鏈€鏂版棩鏈熺殑鎸囨爣锛屼互鍙婂巻鍙插簭鍒椾綔涓?evidence銆?

    娉ㄦ剰锛?
      - 鏈暟鎹簮涓嶈闂閮?API锛屼粎渚濊禆鏈湴 Oracle 鏁版嵁銆?
      - 鑻ヨ繑鍥炴暟鎹负绌烘垨寮傚父锛屽垯杈撳嚭涓€у潡骞舵爣璁?data_status銆?
"""

from __future__ import annotations

import os
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd

from core.datasources.datasource_base import DataSourceBase, DataSourceConfig
from core.utils.ds_refresh import apply_refresh_cleanup
from core.utils.logger import get_logger
from core.adapters.providers.db_provider_mysql_market import DBOracleProvider

LOG = get_logger("DS.LiquidityQuality")


class LiquidityQualityDataSource(DataSourceBase):
    """Liquidity quality DataSource for F block."""

    def __init__(self, config: DataSourceConfig, window: int = 60):
        super().__init__(name="DS.LiquidityQuality")
        self.config = config
        self.window = int(window) if window and window > 0 else 60
        self.db = DBOracleProvider()

        # Prepare cache and history directories
        self.cache_root = config.cache_root
        self.history_root = config.history_root
        os.makedirs(self.cache_root, exist_ok=True)
        os.makedirs(self.history_root, exist_ok=True)

        self.cache_file = os.path.join(self.cache_root, "liquidity_quality_today.json")
        self.history_file = os.path.join(self.history_root, "liquidity_quality_series.json")

        LOG.info(
            "[DS.LiquidityQuality] Init: market=%s ds_name=%s cache_root=%s history_root=%s window=%s",
            config.market,
            config.ds_name,
            self.cache_root,
            self.history_root,
            self.window,
        )

    # --------------------------------------------------------------
    @staticmethod
    def _save(path: str, obj: Any) -> None:
        """Persist json to disk (history/cache helpers).

        NOTE: DataSourceBase in this repo is intentionally minimal and does not
        provide a shared _save(). Other DS modules implement their own helper.
        This DS follows the same convention to avoid runtime AttributeError.
        """
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(obj, f, ensure_ascii=False, indent=2)
        except Exception as exc:
            LOG.error("[DS.LiquidityQuality] _save failed: path=%s err=%s", path, exc)

    # --------------------------------------------------------------
    def build_block(self, trade_date: str, refresh_mode: str = "none") -> Dict[str, Any]:
        """
        鏋勫缓娴佸姩鎬ц川閲忓師濮嬫暟鎹潡銆?

        鍙傛暟锛?
            trade_date: 璇勪及鏃ユ湡锛堝瓧绗︿覆鏍煎紡 'YYYY-MM-DD' 鎴?'YYYYMMDD'锛?
            refresh_mode: 鍒锋柊绛栫暐锛屾敮鎸?'none'|'readonly'|'full'

        杩斿洖锛氬寘鍚渶鏂版棩鏈熸寚鏍囧強鍘嗗彶搴忓垪鐨勫瓧鍏搞€?
        """
        # 娓呯悊缂撳瓨
        apply_refresh_cleanup(
            refresh_mode=refresh_mode,
            cache_path=self.cache_file,
            history_path=self.history_file,
            spot_path=None,
        )

        # 灏濊瘯璇诲彇缂撳瓨
        if refresh_mode in ("none", "readonly") and os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as exc:
                LOG.error("[DS.LiquidityQuality] load cache error: %s", exc)

        # 璁＄畻鍥炴函鍖洪棿锛氶澶栧彇 20 澶╃敤浜庤绠楁粴鍔ㄥ潎鍊?
        try:
            as_date = pd.to_datetime(trade_date).date()
        except Exception:
            # 鑻?trade_date 瑙ｆ瀽澶辫触锛屽垯杩斿洖涓€?
            return self._neutral_block(trade_date)

        look_back_days = self.window + 20
        # 璧峰鏃ユ湡 = trade_date - look_back_days 澶?
        start_dt = as_date - timedelta(days=look_back_days)

        try:
            # 璋冪敤 DBProvider 鑾峰彇鏁版嵁锛堝寘鎷鏀剁洏/娑ㄨ穼骞?鎴愪氦棰濓級
            rows = self.db.query_stock_closes(start_dt, as_date)
        except Exception as exc:
            LOG.error("[DS.LiquidityQuality] oracle fetch error: %s", exc)
            return self._neutral_block(trade_date)

        if not rows:
            LOG.warning("[DS.LiquidityQuality] no data returned for %s", trade_date)
            return self._neutral_block(trade_date)

        # 杞负 DataFrame
        df = pd.DataFrame(rows, columns=["symbol", "exchange", "trade_date", "pre_close", "chg_pct", "close", "amount"])
        # 绫诲瀷杞崲
        try:
            df["trade_date"] = pd.to_datetime(df["trade_date"])
        except Exception:
            df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
        df["chg_pct"] = pd.to_numeric(df["chg_pct"], errors="coerce").fillna(0.0)

        # 鎸?symbol + trade_date 鎺掑簭锛屼互渚胯绠楁粴鍔ㄥ潎鍊?
        df_sorted = df.sort_values(["symbol", "trade_date"]).copy()
        # 璁＄畻姣忎釜 symbol 鐨勮繎 20 鏃ュ钩鍧?amount锛堝寘鍚綋鍓嶆棩锛?
        df_sorted["ma20_amount"] = (
            df_sorted.groupby("symbol")["amount"].transform(lambda x: x.rolling(window=20, min_periods=1).mean())
        )

        # 閫夋嫨鏈€鍚?window 鏃ョ殑鏁版嵁
        # 棣栧厛鎵惧嚭鎵€鏈夋棩鏈燂紝鍗囧簭鎺掑垪
        unique_dates = sorted(df_sorted["trade_date"].unique())
        if not unique_dates:
            return self._neutral_block(trade_date)
        # 鍙栨渶鍚?window 澶?
        selected_dates = unique_dates[-self.window:]

        series: List[Dict[str, Any]] = []
        for dt in selected_dates:
            df_day = df_sorted[df_sorted["trade_date"] == dt]
            total_amount = float(df_day["amount"].sum())
            # Top20 鎴愪氦棰濆崰姣?
            top20_ratio = 0.0
            if total_amount > 0:
                df_sorted_day = df_day.sort_values("amount", ascending=False)
                topn = min(len(df_sorted_day), 20)
                top_amount = float(df_sorted_day.head(topn)["amount"].sum())
                if total_amount > 0:
                    top20_ratio = top_amount / total_amount
            # 澶?灏忕洏鎴愪氦鍗犳瘮
            big_prefixes = ("60", "601", "603")
            # 鍒ゆ柇 symbol 棣栧瓧娈碉紝娉ㄦ剰 symbol 鍙兘涓哄瓧绗︿覆鎴栨暟瀛?
            symbols = df_day["symbol"].astype(str).fillna("")
            big_mask = symbols.str.startswith(big_prefixes)
            big_amount = float(df_day.loc[big_mask, "amount"].sum())
            small_amount = float(df_day.loc[~big_mask, "amount"].sum())
            if small_amount > 0:
                big_small_ratio = big_amount / small_amount
            else:
                # 濡傛灉灏忕洏鎴愪氦棰濅负 0锛屽垯鏃犳硶璁＄畻姣斾緥锛岃涓?None
                big_small_ratio = None
            # 缂╅噺涓嬭穼姣旓細涓嬭穼涓旀垚浜ら浣庝簬 ma20_amount
            df_neg = df_day[df_day["chg_pct"] < 0]
            neg_cnt = len(df_neg)
            if neg_cnt > 0:
                down_low_cnt = (df_neg["amount"] < df_neg["ma20_amount"]).sum()
                down_low_ratio = down_low_cnt / neg_cnt
            else:
                down_low_ratio = None

            series.append(
                {
                    "trade_date": dt.strftime("%Y-%m-%d"),
                    "top20_ratio": round(top20_ratio, 4) if top20_ratio is not None else None,
                    "big_small_ratio": round(big_small_ratio, 4) if big_small_ratio is not None else None,
                    "down_low_ratio": round(down_low_ratio, 4) if down_low_ratio is not None else None,
                }
            )

        # 濡傛湭鐢熸垚 series 鎴栨渶鏂版棩鏈熶笉鍖归厤锛屽垯杩斿洖涓€?
        if not series:
            return self._neutral_block(trade_date)

        # 璁＄畻瓒嬪娍鍜屽姞閫熷害锛堣繎 10 鏃ュ拰杩?3 鏃ュ樊鍊硷級
        def _calc_delta(vals: List[Optional[float]], days: int) -> Optional[float]:
            try:
                if len(vals) > days and vals[-1] is not None and vals[-days - 1] is not None:
                    return round(float(vals[-1]) - float(vals[-days - 1]), 4)
            except Exception:
                pass
            return None

        # 鎻愬彇姣忎竴鍒楃殑鍊?
        top20_vals = [s.get("top20_ratio") for s in series]
        big_small_vals = [s.get("big_small_ratio") for s in series]
        down_low_vals = [s.get("down_low_ratio") for s in series]

        top20_trend_10d = _calc_delta(top20_vals, 10)
        top20_acc_3d = _calc_delta(top20_vals, 3)
        big_small_trend_10d = _calc_delta(big_small_vals, 10)
        big_small_acc_3d = _calc_delta(big_small_vals, 3)
        down_low_trend_10d = _calc_delta(down_low_vals, 10)
        down_low_acc_3d = _calc_delta(down_low_vals, 3)

        latest = series[-1]
        latest_date = latest.get("trade_date")
        top20_ratio_last = latest.get("top20_ratio") if latest else None
        big_small_ratio_last = latest.get("big_small_ratio") if latest else None
        down_low_ratio_last = latest.get("down_low_ratio") if latest else None

        block: Dict[str, Any] = {
            "trade_date": latest_date,
            "top20_ratio": top20_ratio_last,
            "big_small_ratio": big_small_ratio_last,
            "down_low_ratio": down_low_ratio_last,
            "top20_trend_10d": top20_trend_10d,
            "top20_acc_3d": top20_acc_3d,
            "big_small_trend_10d": big_small_trend_10d,
            "big_small_acc_3d": big_small_acc_3d,
            "down_low_trend_10d": down_low_trend_10d,
            "down_low_acc_3d": down_low_acc_3d,
            "series": series,
        }

        # 淇濆瓨鍘嗗彶鍜岀紦瀛?
        try:
            self._save(self.history_file, series)
            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(block, f, ensure_ascii=False, indent=2)
        except Exception as exc:
            LOG.error("[DS.LiquidityQuality] save error: %s", exc)

        return block

    # --------------------------------------------------------------
    def _neutral_block(self, trade_date: str) -> Dict[str, Any]:
        return {
            "trade_date": trade_date,
            "top20_ratio": None,
            "big_small_ratio": None,
            "down_low_ratio": None,
            "top20_trend_10d": None,
            "top20_acc_3d": None,
            "big_small_trend_10d": None,
            "big_small_acc_3d": None,
            "down_low_trend_10d": None,
            "down_low_acc_3d": None,
            "series": [],
        }

