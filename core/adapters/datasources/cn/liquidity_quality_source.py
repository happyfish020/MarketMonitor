# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - Liquidity Quality DataSource (F Block)

閸旂喕鍏橀敍?
    鐠侊紕鐣婚崗銊ョ閸︾儤绁﹂崝銊︹偓褑宸濋柌蹇曟祲閸忚櫕瀵氶弽鍥风礉閸栧懏瀚敍?
      - Top20 閹存劒姘﹂梿鍡曡厬鎼达讣绱欓崜?20 閸氬秵鍨氭禍銈夘杺閸楃姵鐦敍?
      - 婢?鐏忓繒娲忛幋鎰唉閸楃姵鐦敍鍫熷瘻閼诧紕銈ㄦ禒锝囩垳閸撳秶绱戦崚鎺戝瀻婢堆呮磸娑撳骸鐨惄姗堢礆
      - 缂傗晠鍣烘稉瀣┘濮ｆ棑绱欐稉瀣┘閼诧紕銈ㄦ稉顓熷灇娴溿倝顤傛担搴濈艾閼奉亣闊╂潻?20 閺冦儱娼庢０婵堟畱濮ｆ柧绶ラ敍?

鐎圭偟骞囩憰浣哄仯閿?
    1. 娴?Oracle 娑擃厾娈?CN_STOCK_DAILY_PRICE 鐞涖劏顕伴崣鏍ㄥ瘹鐎规艾娲栧┃顖滅崶閸欙絽鍞撮惃鍕亗缁併劍妫╃痪鎸庢殶閹诡噯绱?
         symbol, exchange, trade_date, chg_pct, amount
       闁俺绻?DBOracleProvider.query_stock_closes 閹恒儱褰涢懢宄板絿閵?
    2. 閹稿鍋傜粊銊┾偓鎰鐠侊紕鐣?20 閺冦儲绮撮崝銊ラ挬閸у洦鍨氭禍銈夘杺閿涘牆鎯堣ぐ鎾冲閺冦儻绱氶妴?
    3. 閸︺劍澧嶉柅澶岀崶閸欙絽鍞撮敍鍫ョ帛鐠?60 閺冦儻绱氶敍宀勨偓鎰）缂佺喕顓告稉濠呭牚閹稿洦鐖ｉ敍?
         * Top20 閹存劒姘﹂梿鍡曡厬鎼?= sum(top 20 amount) / sum(total amount)
         * 婢?鐏忓繒娲忛幋鎰唉閸楃姵鐦?= sum(amount of big-cap) / sum(amount of small-cap)
           婢堆呮磸閼诧紕銈ㄩ惃鍕灲閺傤厼鐔€娴滃氦鍋傜粊銊ゅ敩閻礁澧犵紓鈧敍?00/601/603閿涘绱?
               閼?symbol 娴?"60", "601", "603" 瀵偓婢舵潙鍨憴鍡曡礋婢堆呮磸閿涙稑鎯侀崚娆掝潒娑撳搫鐨惄妯糕偓?
         * 缂傗晠鍣烘稉瀣┘濮?= count(chg_pct < 0 & amount < ma20_amount) / count(chg_pct < 0)
    4. 鐠侊紕鐣?10 閺冦儴绉奸崝鍨嫲 3 閺冦儱濮為柅鐔峰閿涘牆褰囬崥鍕瘹閺嶅洩绻?10 閺?3 閺冦儱妯婇崐纭风礆閵?
    5. 鏉堟挸鍤張鈧弬鐗堟）閺堢喓娈戦幐鍥ㄧ垼閿涘奔浜掗崣濠傚坊閸欐彃绨崚妞剧稊娑?evidence閵?

    濞夈劍鍓伴敍?
      - 閺堫剚鏆熼幑顔界爱娑撳秷顔栭梻顔碱樆闁?API閿涘奔绮庢笟婵婄閺堫剙婀?Oracle 閺佺増宓侀妴?
      - 閼汇儴绻戦崶鐐存殶閹诡喕璐熺粚鐑樺灗瀵倸鐖堕敍灞藉灟鏉堟挸鍤稉顓熲偓褍娼￠獮鑸电垼鐠?data_status閵?
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
from core.adapters.providers.db_provider_mysql_market import DBMySQLMarketProvider

LOG = get_logger("DS.LiquidityQuality")


class LiquidityQualityDataSource(DataSourceBase):
    """Liquidity quality DataSource for F block."""

    def __init__(self, config: DataSourceConfig, window: int = 60):
        super().__init__(name="DS.LiquidityQuality")
        self.config = config
        self.window = int(window) if window and window > 0 else 60
        self.db = DBMySQLMarketProvider()

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
        閺嬪嫬缂撳ù浣稿З閹嗗窛闁插繐甯慨瀣殶閹诡喖娼￠妴?

        閸欏倹鏆熼敍?
            trade_date: 鐠囧嫪鍙婇弮銉︽埂閿涘牆鐡х粭锔胯閺嶇厧绱?'YYYY-MM-DD' 閹?'YYYYMMDD'閿?
            refresh_mode: 閸掗攱鏌婄粵鏍殣閿涘本鏁幐?'none'|'readonly'|'full'

        鏉╂柨娲栭敍姘瘶閸氼偅娓堕弬鐗堟）閺堢喐瀵氶弽鍥у挤閸樺棗褰舵惔蹇撳灙閻ㄥ嫬鐡ч崗鎼炩偓?
        """
        # 濞撳懐鎮婄紓鎾崇摠
        apply_refresh_cleanup(
            refresh_mode=refresh_mode,
            cache_path=self.cache_file,
            history_path=self.history_file,
            spot_path=None,
        )

        # 鐏忔繆鐦拠璇插絿缂傛挸鐡?
        if refresh_mode in ("none", "readonly") and os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as exc:
                LOG.error("[DS.LiquidityQuality] load cache error: %s", exc)

        # 鐠侊紕鐣婚崶鐐村嚱閸栨椽妫块敍姘额杺婢舵牕褰?20 婢垛晝鏁ゆ禍搴ゎ吀缁犳绮撮崝銊ユ綆閸?
        try:
            as_date = pd.to_datetime(trade_date).date()
        except Exception:
            # 閼?trade_date 鐟欙絾鐎芥径杈Е閿涘苯鍨潻鏂挎礀娑擃厽鈧?
            return self._neutral_block(trade_date)

        look_back_days = self.window + 20
        # 鐠у嘲顫愰弮銉︽埂 = trade_date - look_back_days 婢?
        start_dt = as_date - timedelta(days=look_back_days)

        try:
            # 鐠嬪啰鏁?DBProvider 閼惧嘲褰囬弫鐗堝祦閿涘牆瀵橀幏顒勵暕閺€鍓佹磸/濞戙劏绌奸獮?閹存劒姘︽０婵撶礆
            rows = self.db.query_stock_closes(start_dt, as_date)
        except Exception as exc:
            LOG.error("[DS.LiquidityQuality] mysql fetch error: %s", exc)
            return self._neutral_block(trade_date)

        if not rows:
            LOG.warning("[DS.LiquidityQuality] no data returned for %s", trade_date)
            return self._neutral_block(trade_date)

        # 鏉烆兛璐?DataFrame
        df = pd.DataFrame(rows, columns=["symbol", "exchange", "trade_date", "pre_close", "chg_pct", "close", "amount"])
        # 缁鐎锋潪顒佸床
        try:
            df["trade_date"] = pd.to_datetime(df["trade_date"])
        except Exception:
            df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
        df["chg_pct"] = pd.to_numeric(df["chg_pct"], errors="coerce").fillna(0.0)

        # 閹?symbol + trade_date 閹烘帒绨敍灞间簰娓氳儻顓哥粻妤佺泊閸斻劌娼庨崐?
        df_sorted = df.sort_values(["symbol", "trade_date"]).copy()
        # 鐠侊紕鐣诲В蹇庨嚋 symbol 閻ㄥ嫯绻?20 閺冦儱閽╅崸?amount閿涘牆瀵橀崥顐㈢秼閸撳秵妫╅敍?
        df_sorted["ma20_amount"] = (
            df_sorted.groupby("symbol")["amount"].transform(lambda x: x.rolling(window=20, min_periods=1).mean())
        )

        # 闁瀚ㄩ張鈧崥?window 閺冦儳娈戦弫鐗堝祦
        # 妫ｆ牕鍘涢幍鎯у毉閹碘偓閺堝妫╅張鐕傜礉閸楀洤绨幒鎺戝灙
        unique_dates = sorted(df_sorted["trade_date"].unique())
        if not unique_dates:
            return self._neutral_block(trade_date)
        # 閸欐牗娓堕崥?window 婢?
        selected_dates = unique_dates[-self.window:]

        series: List[Dict[str, Any]] = []
        for dt in selected_dates:
            df_day = df_sorted[df_sorted["trade_date"] == dt]
            total_amount = float(df_day["amount"].sum())
            # Top20 閹存劒姘︽０婵嗗窗濮?
            top20_ratio = 0.0
            if total_amount > 0:
                df_sorted_day = df_day.sort_values("amount", ascending=False)
                topn = min(len(df_sorted_day), 20)
                top_amount = float(df_sorted_day.head(topn)["amount"].sum())
                if total_amount > 0:
                    top20_ratio = top_amount / total_amount
            # 婢?鐏忓繒娲忛幋鎰唉閸楃姵鐦?
            big_prefixes = ("60", "601", "603")
            # 閸掋倖鏌?symbol 妫ｆ牕鐡у▓纰夌礉濞夈劍鍓?symbol 閸欘垵鍏樻稉鍝勭摟缁楋缚瑕嗛幋鏍ㄦ殶鐎?
            symbols = df_day["symbol"].astype(str).fillna("")
            big_mask = symbols.str.startswith(big_prefixes)
            big_amount = float(df_day.loc[big_mask, "amount"].sum())
            small_amount = float(df_day.loc[~big_mask, "amount"].sum())
            if small_amount > 0:
                big_small_ratio = big_amount / small_amount
            else:
                # 婵″倹鐏夌亸蹇曟磸閹存劒姘︽０婵呰礋 0閿涘苯鍨弮鐘崇《鐠侊紕鐣诲В鏂剧伐閿涘矁顔曟稉?None
                big_small_ratio = None
            # 缂傗晠鍣烘稉瀣┘濮ｆ棑绱版稉瀣┘娑撴梹鍨氭禍銈夘杺娴ｅ簼绨?ma20_amount
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

        # 婵″倹婀悽鐔稿灇 series 閹存牗娓堕弬鐗堟）閺堢喍绗夐崠褰掑帳閿涘苯鍨潻鏂挎礀娑擃厽鈧?
        if not series:
            return self._neutral_block(trade_date)

        # 鐠侊紕鐣荤搾瀣◢閸滃苯濮為柅鐔峰閿涘牐绻?10 閺冦儱鎷版潻?3 閺冦儱妯婇崐纭风礆
        def _calc_delta(vals: List[Optional[float]], days: int) -> Optional[float]:
            try:
                if len(vals) > days and vals[-1] is not None and vals[-days - 1] is not None:
                    return round(float(vals[-1]) - float(vals[-days - 1]), 4)
            except Exception:
                pass
            return None

        # 閹绘劕褰囧В蹇庣閸掓娈戦崐?
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

        # 娣囨繂鐡ㄩ崢鍡楀蕉閸滃瞼绱︾€?
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


