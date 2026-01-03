# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 FULL
ETF × Spot Synchronization Daily DataSource (T-1 Confirmed)

设计铁律：
- Phase-1 DataSource（事实层）
- 只读取 oracle DB（T-1 已收盘全行情）
- snapshot_type 永远为 EOD
- 可回放、可审计
- 专供 Gate / Phase-3 使用
"""

import os
import json
from typing import Dict, Any
from datetime import datetime
import pandas as pd
from core.adapters.providers.db_provider_oracle import DBOracleProvider

from core.utils.logger import get_logger
from core.datasources.datasource_base import (
    DataSourceConfig,
    DataSourceBase,
)

LOG = get_logger("DS.ETFSpotSyncDaily")


class ETFSpotSyncDailyDataSource(DataSourceBase):
    """
    ETF × 市场横截面同步性 DataSource（T-1 确认态）

    输出事实（不解释）：
    - 上涨 / 下跌比例
    - 成交额集中度（Top 20%）
    - 涨跌幅分化程度（dispersion）
    """

    def __init__(self, config: DataSourceConfig):
        super().__init__(name="etf_index_sync_daily")

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
        trade_date: T-1（确认态）
        """

        cache_file = os.path.join(
            self.cache_root, f"etf_spot_sync_daily_{trade_date}.json"
        )

        # 命中 cache（daily 永远允许）
        if refresh_mode in ("none", "readonly") and os.path.exists(cache_file):
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                LOG.error("[DS.ETFSpotSyncDaily] load cache error: %s", e)

        # ------------------------------------------------------------
        # 1️⃣ 从 oracle 读取 T-1 全行情（已收盘）
        # ------------------------------------------------------------
        
        #last_td = self.oracle.query_last_trade_date(trade_date)  # trade_date is T (as-of)
        #snapshot = self.oracle.load_full_market_eod_snapshot(last_td)
        snapshot = self.db.load_latest_full_market_eod_snapshot(trade_date)
        
        
        market = snapshot.get("market")
        if not market:
            LOG.error("[DS.ETFSpotSyncDaily] empty market snapshot")
            return self._neutral_block(trade_date)

        # 转为 DataFrame（统一计算口径）
        df = pd.DataFrame.from_dict(market, orient="index")
        
        NUMERIC_COLS = ["chg_pct", "amount", "close", "prev_close"]
        
        for c in NUMERIC_COLS:
            if c in df.columns:
                df[c] = df[c].astype(float)


        if "close" not in df.columns or "symbol" not in df.columns:
            LOG.error("[DS.ETFSpotSyncDaily] required columns missing in market snapshot")
            return self._neutral_block(trade_date)
         
        # ------------------------------------------------------------
        # 2️⃣ 计算涨跌幅（T-1 只能用 close / prev_close）
        # 这里假设 oracle 表中已有 prev_close
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
        # 3️⃣ 成交额集中度（Top 20%）
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
        # 4️⃣ 分化程度
        # ------------------------------------------------------------
        dispersion = round(float(df["chg_pct"].std(ddof=0)), 4)

        block: Dict[str, Any] = {
            "trade_date": trade_date,
            "snapshot_type": "EOD",          # 强制确认态
            "amount_stage": "FULL",        # daily 固定 FULL

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

        # 写 cache
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