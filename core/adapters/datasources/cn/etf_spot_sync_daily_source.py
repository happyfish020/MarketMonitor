# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 FULL
ETF 鑴?Spot Synchronization Daily DataSource (T-1 Confirmed)

鐠佹崘顓搁柧浣哥伐閿?
- Phase-1 DataSource閿涘牅绨ㄧ€圭偛鐪伴敍?
- 閸欘亣顕伴崣?oracle DB閿涘湵-1 瀹稿弶鏁归惄妯哄弿鐞涘本鍎忛敍?
- snapshot_type 濮樻瓕绻欐稉?EOD
- 閸欘垰娲栭弨淇扁偓浣稿讲鐎孤ゎ吀
- 娑撴挷绶?Gate / Phase-3 娴ｈ法鏁?
"""

import os
import json
from typing import Dict, Any
from datetime import datetime
import pandas as pd
from core.adapters.providers.db_provider_mysql_market import DBMySQLMarketProvider

from core.utils.logger import get_logger
from core.datasources.datasource_base import (
    DataSourceConfig,
    DataSourceBase,
)

LOG = get_logger("DS.ETFSpotSyncDaily")


class ETFSpotSyncDailyDataSource(DataSourceBase):
    """
    ETF 鑴?鐢倸婧€濡亝鍩呴棃銏犳倱濮濄儲鈧?DataSource閿涘湵-1 绾喛顓婚幀渚婄礆

    鏉堟挸鍤禍瀣杽閿涘牅绗夌憴锝夊櫞閿涘绱?
    - 娑撳﹥瀹?/ 娑撳绌煎В鏂剧伐
    - 閹存劒姘︽０婵嬫肠娑擃厼瀹抽敍鍦眔p 20%閿?
    - 濞戙劏绌奸獮鍛瀻閸栨牜鈻兼惔锔肩礄dispersion閿?
    """

    def __init__(self, config: DataSourceConfig):
        super().__init__(name="etf_spot")

        self.config = config
        self.cache_root = config.cache_root
        os.makedirs(self.cache_root, exist_ok=True)

        self.db = DBMySQLMarketProvider()
        if self.db is None:
            raise RuntimeError("mysql provider not configured")

        LOG.info(
            "[DS.ETFSpotSyncDaily] Init: market=%s ds=%s cache_root=%s",
            config.market,
            config.ds_name,
            self.cache_root,
        )

    # ------------------------------------------------------------------
    def build_block(self, trade_date: str, refresh_mode: str = "none") -> Dict[str, Any]:
        """
        trade_date: T-1閿涘牏鈥樼拋銈嗏偓渚婄礆
        """

        cache_file = os.path.join(
            self.cache_root, f"etf_spot_sync_daily_{trade_date}.json"
        )

        # 閸涙垝鑵?cache閿涘潐aily 濮樻瓕绻欓崗浣筋啅閿?
        if refresh_mode in ("none", "readonly") and os.path.exists(cache_file):
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                LOG.error("[DS.ETFSpotSyncDaily] load cache error: %s", e)

        # ------------------------------------------------------------
        # 1閿斿繆鍎?娴?oracle 鐠囪褰?T-1 閸忋劏顢戦幆鍜冪礄瀹稿弶鏁归惄姗堢礆
        # ------------------------------------------------------------
        
        #last_td = self.oracle.query_last_trade_date(trade_date)  # trade_date is T (as-of)
        #snapshot = self.oracle.load_full_market_eod_snapshot(last_td)
        snapshot = self.db.load_latest_full_market_eod_snapshot(trade_date)
        
        
        market = snapshot.get("market")
        if not market:
            LOG.error("[DS.ETFSpotSyncDaily] empty market snapshot")
            return self._neutral_block(trade_date)

        # 鏉烆兛璐?DataFrame閿涘牏绮烘稉鈧拋锛勭暬閸欙絽绶為敍?
        df = pd.DataFrame.from_dict(market, orient="index")
        
        NUMERIC_COLS = ["chg_pct", "amount", "close", "prev_close"]
        
        for c in NUMERIC_COLS:
            if c in df.columns:
                df[c] = df[c].astype(float)


        if "close" not in df.columns or "symbol" not in df.columns:
            LOG.error("[DS.ETFSpotSyncDaily] required columns missing in market snapshot")
            return self._neutral_block(trade_date)
         
        # ------------------------------------------------------------
        # 2閿斿繆鍎?鐠侊紕鐣诲☉銊ㄧ┘楠炲拑绱橳-1 閸欘亣鍏橀悽?close / prev_close閿?
        # 鏉╂瑩鍣烽崑鍥啎 oracle 鐞涖劋鑵戝鍙夋箒 prev_close
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
        # 3閿斿繆鍎?閹存劒姘︽０婵嬫肠娑擃厼瀹抽敍鍦眔p 20%閿?
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
        # 4閿斿繆鍎?閸掑棗瀵茬粙瀣
        # ------------------------------------------------------------
        dispersion = round(float(df["chg_pct"].std(ddof=0)), 4)

        block: Dict[str, Any] = {
            "trade_date": trade_date,
            "snapshot_type": "EOD",          # 瀵搫鍩楃涵顔款吇閹?
            "amount_stage": "FULL",        # daily 閸ュ搫鐣?FULL

            "total_stocks": total,
            "adv_count": adv,
            "dec_count": dec,
            "flat_count": flat,
            "adv_ratio": adv_ratio,
            "dec_ratio": dec_ratio,
            "top20_amount_ratio": top20_amount_ratio,
            "dispersion": dispersion,

            "_meta": {
                "source": "mysql",
                "confirmed": True,
                "total_amount": round(total_amount, 2),
                "generated_at": datetime.now().isoformat(timespec="seconds"),
            },
        }

        # 閸?cache
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

