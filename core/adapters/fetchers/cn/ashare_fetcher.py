# core/adapters/fetchers/cn/ashare_fetcher.py

"""
UnifiedRisk V12 - AshareFetcher
负责：
  - 按 refresh_controller 决定刷新策略
  - 遍历 symbols.yaml 中的指数 / ETF / GlobalLead
  - 调用 datasources 构建 snapshot_v12
"""

from datetime import datetime, date
from typing import Dict, Any

from core.utils.logger import get_logger
from core.utils.config_loader import load_symbols
from core.utils.time_utils import BJ_TZ
import os 
# DataSources
from core.adapters.datasources.glo.index_series_source import IndexSeriesSource
from core.adapters.datasources.cn.turnover_source import TurnoverDataSource

from core.adapters.datasources.cn.margin_source import MarginDataSource

from core.adapters.datasources.cn.zh_spot_source import ZhSpotSource
from core.adapters.datasources.glo.global_lead_source import GlobalLeadSource


# RefreshController
from core.adapters.fetchers.cn.refresh_controller_cn import RefreshControllerCN
from core.adapters.datasources.cn.north_nps_source import NorthNpsDataSource
from core.adapters.datasources.cn.unified_emotion_source import UnifiedEmotionDataSource
from core.utils.config_loader import load_paths

LOG = get_logger("AshareFetcher")
#_paths = load_paths()

#DAY_CACHE_ROOT = os.path.join(_paths.get("cache_dir", "data/cache/"), "day_cn")
#INTRADAY_CACHE_ROOT = os.path.join(_paths.get("cache_dir", "data/cache/"), "intraday_cn")
#ASHARE_ROOT = _paths.get("ashare_root", "data/ashare")

 


class AshareDataFetcher:

    def __init__(self, trade_date: str, refresh_mode: str = "readonly"):
        """
        refresh_mode: readonly / snapshot / full
        """
        LOG.info("初始化 AshareDataFetcher, refresh_mode=%s", refresh_mode)

        # 载入 symbol YAML
        self.symbols = load_symbols()

        # 刷新策略
        self.rc = RefreshControllerCN(refresh_mode)

        self.trade_date = trade_date
        # Datasources
        self.index_source = IndexSeriesSource(market="glo")
        self.turnover_source = TurnoverDataSource(self.trade_date)
         
        self.margin_source = MarginDataSource(self.trade_date)
        
        self.north_nps_source = NorthNpsDataSource(self.trade_date)
        self.emotion_ds = UnifiedEmotionDataSource(trade_date)         
        #self.sentiment_source = MarketSentimentDataSource(self.trade_date)
        #self.emotion_source = EmotionDataSource()        
        #self.global_lead_source = GlobalLeadSource()
        
        #self.force_refresh = force_refresh
    # ==========================================================
    # Snapshot V12 主入口
    # ==========================================================
    def build_daily_snapshot(self ) -> Dict[str, Any]:
        LOG.info("开始构建 Snapshot V12" )

        snapshot = {
            "meta": {
                #"date": now.strftime("%Y-%m-%d"),
                "trade_date": self.trade_date,
            },
            "index_core": {},
            "turnover": {},
            "sentiment": {},
            "emotion": {},
            #"northbound": {},
            "north_nps": {},
            "margin": {},
            "spot": {},
            "global_lead": {},
        }

        # ---------------------------
        # 1. 指数数据（从 YAML cn_index 读取）
        # ---------------------------
        LOG.info("[1] 开始获取 cn_index (指数) ...")
        for name, symbol in self.symbols.get("cn_index", {}).items():
            LOG.info("获取指数 %s (%s)", name, symbol)

            refresh = self.rc.should_refresh(symbol)
            df = self.index_source.get_series(symbol, refresh=refresh)

            snapshot["index_core"][name] = self._parse_index(df, symbol)

        # ---------------------------
        # 2. Turnover（成交额）
        # ---------------------------
        LOG.info("[2] 获取成交额 Turnover ...")

        snapshot["turnover"] = self.turnover_source.get_turnover_block(
            refresh_mode=self.rc.refresh_mode
        )
 
       
    
        # ---------------------------
        # 4. 北向资金
        # ---------------------------
        LOG.info("[4] 获取 Northbound ...")

        #snapshot["northbound"] = self.north_etf_source.get_northbound_snapshot(
        #    refresh=self.rc.refresh_flag
        #)
        snapshot["north_nps"] = self.north_nps_source.build_block(refresh=self.rc.refresh_flag)


        # ---------------------------
        # 5. 两融
        # ---------------------------
        LOG.info("[5] 获取 Margin 融资融券 ...")

        snapshot["margin"] = self.margin_source.get_margin_block(
            refresh=self.rc.refresh_flag
        )

        # ---------------------------
        # 6. Spot（A股情绪，涨跌结构、涨停板等）
        # ---------------------------
        LOG.info("[6] 获取 Spot (情绪结构) ...")
 
        
        #snapshot["sentiment"] = self.sentiment_source.get_sentiment_block( refresh_mode=self.rc.refresh_mode)
        #snapshot["emotion"] = self.emotion_source.get_block(snapshot)


        sent_block, emo_block = self.emotion_ds.get_blocks(snapshot, refresh_mode=self.rc.refresh_mode)
        # ---------------------------
        # 7. Global Lead（美股、商品、VIX、美元）
        # ---------------------------
        LOG.info("[7] 获取 Global Lead ...")

        snapshot["global_lead"] = self._build_global_lead_snapshot()

        LOG.info("Snapshot V12 构建完成")

        return snapshot

    # ==========================================================
    # Global Lead Snapshot 构建
    # ==========================================================
    def _build_global_lead_snapshot(self):
        LOG.info("开始构建 GlobalLead Snapshot ...")

        result = {}
        glead = self.symbols.get("global_lead", {})

        for group, symbols in glead.items():
            LOG.info("GlobalLead group=%s", group)
            result[group] = {}

            for sym in symbols:
                LOG.info("获取 GlobalLead: %s", sym)
                refresh = self.rc.should_refresh(sym)

                quote = self.global_lead_source.get_last_quote(
                    sym,
                    refresh=refresh  # 你 global_lead_source 应接受 refresh 参数
                )

                result[group][sym] = quote

        return result

    # ==========================================================
    # 解析指数（统一结构：last, pct）
    # ==========================================================
    def _parse_index(self, df, symbol):
        if df is None or df.empty:
            LOG.warning("指数数据为空: %s", symbol)
            return {"last": None, "pct": None}

        last = float(df.iloc[-1]["close"])
        pct = df.iloc[-1].get("pct")

        LOG.info("指数解析完成 %s: last=%.3f pct=%s", symbol, last, pct)
        return {"last": last, "pct": pct}
