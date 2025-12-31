# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 FULL
ETF × Spot Synchronization DataSource

设计铁律：
- Phase-1 DataSource（事实层）
- 读取全行情 spot（支持盘中 / 收盘后）
- 只做“横截面统计”，不做任何判断
- 不依赖其它 DS 的输出
- 不做 history，不做 trend
- 明确标注 snapshot 时间态（INTRADAY / EOD）
"""

import os
import json
from typing import Dict, Any
from datetime import datetime, time
from core.utils.time_utils import now_bj,is_trade_date_for_now
import pandas as pd

from core.utils.logger import get_logger
from core.datasources.datasource_base import (
    DataSourceConfig,
    DataSourceBase,
)
from core.utils.spot_store import get_spot_daily
from core.utils.ds_refresh import apply_refresh_cleanup

LOG = get_logger("DS.ETFSpotSync")


class ETFSpotSyncDataSource(DataSourceBase):
    """
    ETF × 市场横截面同步性 DataSource（事实层）

    输出事实（不解释）：
    - 上涨 / 下跌比例
    - 成交额集中度（Top 20%）
    - 涨跌幅分化程度（dispersion）
    - snapshot 时间态（盘中 / 收盘）
    """

    def __init__(self, config: DataSourceConfig , is_intraday:bool =False):
        super().__init__(name="DS.ETFSpotSync")

        self.config = config
        self.cache_root = config.cache_root
        self.history_root = config.history_root
        self.is_intraday = is_intraday
        os.makedirs(self.cache_root, exist_ok=True)
        os.makedirs(self.history_root, exist_ok=True)

        LOG.info(
            "[DS.ETFSpotSync] Init: market=%s ds=%s cache_root=%s history_root=%s",
            config.market,
            config.ds_name,
            self.cache_root,
            self.history_root,
        )

    # ------------------------------------------------------------------
    def build_block(self, trade_date: str, refresh_mode: str = "none") -> Dict[str, Any]:
        cache_file = os.path.join(self.cache_root, f"etf_spot_sync_{trade_date}.json")
        self.trade_date = trade_date

        # refresh 控制（仅 cache）
        _ = apply_refresh_cleanup(
            refresh_mode=refresh_mode,
            cache_path=cache_file,
            history_path=None,
            spot_path=None,
        )

        # 命中 cache（只允许非 intraday）
        if refresh_mode in ("none", "readonly") and os.path.exists(cache_file) and not self.is_intraday:
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                LOG.error("[DS.ETFSpotSync] load cache error: %s", e)

        # ------------------------------------------------------------
        # 1️⃣ 获取全行情 spot（盘中 or 收盘）
        # ------------------------------------------------------------
        try: 
            
            df: pd.DataFrame = get_spot_daily(
                trade_date=trade_date,
                refresh_mode=refresh_mode,
            )
        except Exception as e:
            LOG.error("[DS.ETFSpotSync] get_spot_daily error: %s", e)
            return self._neutral_block(trade_date, snapshot_type="UNKNOWN")

        if df is None or df.empty:
            LOG.error("[DS.ETFSpotSync] Spot DF empty")
            return self._neutral_block(trade_date, snapshot_type="UNKNOWN")

        df = df.rename(columns={"昨收": "pre_close", "最新价": "close",  "成交额": "turnover" ,"涨跌幅": "chg_pct"})
        NUMERIC_COLS = ["chg_pct", "turnover", "close", "prev_close"]
        
        for c in NUMERIC_COLS:
            if c in df.columns:
                df[c] = df[c].astype(float)


        # 必要字段检查
        if "chg_pct" not in df.columns or "turnover" not in df.columns:
            LOG.error("[DS.ETFSpotSync] required columns missing")
            return self._neutral_block(trade_date, snapshot_type="UNKNOWN")

        total = len(df)
        if total == 0:
            return self._neutral_block(trade_date, snapshot_type="UNKNOWN")

        # ------------------------------------------------------------
        # 2️⃣ snapshot 时间态判断（事实标注，不做判断）
        # ------------------------------------------------------------
        snapshot_type = self._detect_snapshot_type(refresh_mode)
           
        # ------------------------------------------------------------
        # 3️⃣ 横截面统计（纯事实）
        # ------------------------------------------------------------
        adv = int((df["chg_pct"] > 0).sum())
        dec = int((df["chg_pct"] < 0).sum())

        adv_ratio = round(adv / total, 4)
        dec_ratio = round(dec / total, 4)

        # turnover集中度（Top 20%）
        df_turn = df[["turnover"]].copy()
        df_turn = df_turn[df_turn["turnover"] > 0]

        if not df_turn.empty:
            df_turn_sorted = df_turn.sort_values("turnover", ascending=False)
            top_n = max(int(len(df_turn_sorted) * 0.2), 1)
            top_turnover = float(df_turn_sorted.head(top_n)["turnover"].sum())
            total_turnover = float(df_turn_sorted["turnover"].sum())
            top20_turnover_ratio = (
                round(top_turnover / total_turnover, 4)
                if total_turnover > 0
                else 0.0
            )
        else:
            top20_turnover_ratio = 0.0
            total_turnover = 0.0

        # 涨跌幅分化（标准差）
        dispersion = round(float(df["chg_pct"].std(ddof=0)), 4)

        # ------------------------------------------------------------
        # 4️⃣ 成交额阶段标注（仅事实，用于盘中解释）
        # ------------------------------------------------------------
        turnover_stage = self._infer_turnover_stage(snapshot_type)

        block: Dict[str, Any] = {
            "trade_date": trade_date,
            "snapshot_type": snapshot_type,      # INTRADAY / EOD / UNKNOWN
            "turnover_stage": turnover_stage,    # EARLY / MID / LATE / FULL / UNKNOWN

            "total_stocks": total,
            "adv_ratio": adv_ratio,
            "dec_ratio": dec_ratio,
            "top20_turnover_ratio": top20_turnover_ratio,
            "dispersion": dispersion,

            # 仅用于审计，不做解释
            "_meta": {
                "refresh_mode": refresh_mode,
                "total_turnover": round(total_turnover, 2),
                "generated_at": datetime.now().isoformat(timespec="seconds"),
            },
        }

        # ------------------------------------------------------------
        # 5️⃣ 写 cache（仅 EOD / 非 intraday）
        # ------------------------------------------------------------
        if snapshot_type == "EOD":
            try:
                with open(cache_file, "w", encoding="utf-8") as f:
                    json.dump(block, f, ensure_ascii=False, indent=2)
            except Exception as e:
                LOG.error("[DS.ETFSpotSync] save cache error: %s", e)

        return block

    # ------------------------------------------------------------------
    def _detect_snapshot_type(self, refresh_mode: str) -> str:
        """
        判断当前 snapshot 类型（事实标注）
        """
       
        if self.is_intraday:
            return "INTRADAY"
    
        return "EOD"

    def _infer_turnover_stage(self, snapshot_type: str) -> str:
        """
        成交额阶段标注（仅事实，不做判断）
        """

        if snapshot_type == "EOD":
            return "FULL"
      
        now = now_bj() 
        
        if is_trade_date_for_now(self.trade_date):

            if now.time() > time(15,0):
                return "EOD"

  
            if now.time() < time(9, 0):
                return "EARLY"
            if now.time() < time(13, 30):
                return "MID"
            if now.time() < time(15, 0):
                return "LATE"

        return "UNKNOWN"

    # ------------------------------------------------------------------
    def _neutral_block(self, trade_date: str, snapshot_type: str) -> Dict[str, Any]:
        return {
            "trade_date": trade_date,
            "snapshot_type": snapshot_type,
            "turnover_stage": "UNKNOWN",
            "total_stocks": 0,
            "adv_ratio": 0.0,
            "dec_ratio": 0.0,
            "top20_turnover_ratio": 0.0,
            "dispersion": 0.0,
        }
