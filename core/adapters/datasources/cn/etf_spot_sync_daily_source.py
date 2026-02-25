# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 FULL
ETF 脳 Spot Synchronization Daily DataSource (T-1 Confirmed)

璁捐閾佸緥锛?
- Phase-1 DataSource锛堜簨瀹炲眰锛?
- 鍙鍙?oracle DB锛圱-1 宸叉敹鐩樺叏琛屾儏锛?
- snapshot_type 姘歌繙涓?EOD
- 鍙洖鏀俱€佸彲瀹¤
- 涓撲緵 Gate / Phase-3 浣跨敤
"""

import os
import json
from typing import Dict, Any
from datetime import datetime
import pandas as pd
from core.adapters.providers.db_provider_mysql_market import DBOracleProvider

from core.utils.logger import get_logger
from core.datasources.datasource_base import (
    DataSourceConfig,
    DataSourceBase,
)

LOG = get_logger("DS.ETFSpotSyncDaily")


class ETFSpotSyncDailyDataSource(DataSourceBase):
    """
    ETF 脳 甯傚満妯埅闈㈠悓姝ユ€?DataSource锛圱-1 纭鎬侊級

    杈撳嚭浜嬪疄锛堜笉瑙ｉ噴锛夛細
    - 涓婃定 / 涓嬭穼姣斾緥
    - 鎴愪氦棰濋泦涓害锛圱op 20%锛?
    - 娑ㄨ穼骞呭垎鍖栫▼搴︼紙dispersion锛?
    """

    def __init__(self, config: DataSourceConfig):
        super().__init__(name="etf_spot")

        self.config = config
        self.cache_root = config.cache_root
        os.makedirs(self.cache_root, exist_ok=True)

        self.db = DBOracleProvider()
        if self.db is None:
            raise RuntimeError("oracle provider not configured")

        LOG.info(
            "[DS.ETFSpotSyncDaily] Init: market=%s ds=%s cache_root=%s",
            config.market,
            config.ds_name,
            self.cache_root,
        )

    # ------------------------------------------------------------------
    def build_block(self, trade_date: str, refresh_mode: str = "none") -> Dict[str, Any]:
        """
        trade_date: T-1锛堢‘璁ゆ€侊級
        """

        cache_file = os.path.join(
            self.cache_root, f"etf_spot_sync_daily_{trade_date}.json"
        )

        # 鍛戒腑 cache锛坉aily 姘歌繙鍏佽锛?
        if refresh_mode in ("none", "readonly") and os.path.exists(cache_file):
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                LOG.error("[DS.ETFSpotSyncDaily] load cache error: %s", e)

        # ------------------------------------------------------------
        # 1锔忊儯 浠?oracle 璇诲彇 T-1 鍏ㄨ鎯咃紙宸叉敹鐩橈級
        # ------------------------------------------------------------
        
        #last_td = self.oracle.query_last_trade_date(trade_date)  # trade_date is T (as-of)
        #snapshot = self.oracle.load_full_market_eod_snapshot(last_td)
        snapshot = self.db.load_latest_full_market_eod_snapshot(trade_date)
        
        
        market = snapshot.get("market")
        if not market:
            LOG.error("[DS.ETFSpotSyncDaily] empty market snapshot")
            return self._neutral_block(trade_date)

        # 杞负 DataFrame锛堢粺涓€璁＄畻鍙ｅ緞锛?
        df = pd.DataFrame.from_dict(market, orient="index")
        
        NUMERIC_COLS = ["chg_pct", "amount", "close", "prev_close"]
        
        for c in NUMERIC_COLS:
            if c in df.columns:
                df[c] = df[c].astype(float)


        if "close" not in df.columns or "symbol" not in df.columns:
            LOG.error("[DS.ETFSpotSyncDaily] required columns missing in market snapshot")
            return self._neutral_block(trade_date)
         
        # ------------------------------------------------------------
        # 2锔忊儯 璁＄畻娑ㄨ穼骞咃紙T-1 鍙兘鐢?close / prev_close锛?
        # 杩欓噷鍋囪 oracle 琛ㄤ腑宸叉湁 prev_close
        # ------------------------------------------------------------
        if "pre_close" not in df.columns:
            LOG.error("[DS.ETFSpotSyncDaily] prev_close missing")
            return self._neutral_block(trade_date)

        #df["chg_pct"] = (df["close"] - df["prev_close"]) / df["prev_close"] * 100.0
        df["chg_pct"] = df["chg_pct"].astype(float)

        total = len(df)
        if total == 0:
            return self._neutral_block(trade_date)

        adv = int((df["chg_pct"] > 0).sum())
        dec = int((df["chg_pct"] < 0).sum())
        flat = int(total - adv - dec)

        adv_ratio = round(adv / total, 4)
        dec_ratio = round(dec / total, 4)

        # ------------------------------------------------------------
        # 3锔忊儯 鎴愪氦棰濋泦涓害锛圱op 20%锛?
        # ------------------------------------------------------------
        if "amount" in df.columns:
            df_turn = df[df["amount"] > 0]
            if not df_turn.empty:
                df_turn_sorted = df_turn.sort_values("amount", ascending=False)
                top_n = max(int(len(df_turn_sorted) * 0.2), 1)
                top_amount = float(df_turn_sorted.head(top_n)["amount"].sum())
                total_amount = float(df_turn_sorted["amount"].sum())
                top20_amount_ratio = (
                    round(top_amount / total_amount, 4)
                    if total_amount > 0
                    else 0.0
                )
            else:
                top20_amount_ratio = 0.0
                total_amount = 0.0
        else:
            top20_amount_ratio = 0.0
            total_amount = 0.0

        # ------------------------------------------------------------
        # 4锔忊儯 鍒嗗寲绋嬪害
        # ------------------------------------------------------------
        dispersion = round(float(df["chg_pct"].std(ddof=0)), 4)

        block: Dict[str, Any] = {
            "trade_date": trade_date,
            "snapshot_type": "EOD",          # 寮哄埗纭鎬?
            "amount_stage": "FULL",        # daily 鍥哄畾 FULL

            "total_stocks": total,
            "adv_count": adv,
            "dec_count": dec,
            "flat_count": flat,
            "adv_ratio": adv_ratio,
            "dec_ratio": dec_ratio,
            "top20_amount_ratio": top20_amount_ratio,
            "dispersion": dispersion,

            "_meta": {
                "source": "oracle",
                "confirmed": True,
                "total_amount": round(total_amount, 2),
                "generated_at": datetime.now().isoformat(timespec="seconds"),
            },
        }

        # 鍐?cache
        try:
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(block, f, ensure_ascii=False, indent=2)
        except Exception as e:
            LOG.error("[DS.ETFSpotSyncDaily] save cache error: %s", e)

        return block

    # ------------------------------------------------------------------
    def _neutral_block(self, trade_date: str) -> Dict[str, Any]:
        return {
            "trade_date": trade_date,
            "snapshot_type": "EOD",
            "amount_stage": "FULL",
            "total_stocks": 0,
            "adv_ratio": 0.0,
            "dec_ratio": 0.0,
            "top20_amount_ratio": 0.0,
            "dispersion": 0.0,
        }
