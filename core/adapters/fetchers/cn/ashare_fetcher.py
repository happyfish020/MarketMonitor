# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - CN AShare Fetcher

职责（冻结）：
- 调用各 DataSource
- 将 DS raw 数据完整写入 snapshot
- 不引入 FactorResult
- 不做结构判断
"""

from __future__ import annotations

from typing import Dict, Any

from core.utils.logger import get_logger
from core.adapters.fetchers.fetcher_base import FetcherBase
from core.datasources.datasource_base import DataSourceConfig

# === DataSources ===
from core.adapters.datasources.cn.index_core_source import IndexCoreDateSource
from core.adapters.datasources.cn.breadth_source import BreadthDataSource
from core.adapters.datasources.cn.north_nps_source import NorthNPSDataSource
from core.adapters.datasources.cn.turnover_source import TurnoverDataSource
from core.adapters.datasources.cn.margin_source import MarginDataSource
from core.adapters.datasources.cn.market_sentiment_source import MarketSentimentDataSource
from core.adapters.datasources.cn.core_theme_source import CoreThemeDataSource
from core.adapters.datasources.cn.etf_spot_sync_source  import ETFSpotSyncDataSource
from core.adapters.datasources.glo.global_macro_source import GlobalMacroDataSource
from core.adapters.datasources.glo.global_lead_source import GlobalLeadDataSource
from core.adapters.datasources.glo.index_global_source import IndexGlobalDataSource


# === BlockBuilders / existing producers ===

from core.adapters.block_builder.cn.unified_emotion_blkbd import UnifiedEmotionBlockBuilder
from core.adapters.block_builder.cn.index_tech_blkbd import IndexTechBlockBuilder
from core.adapters.block_builder.cn.sector_rotation_blkbd import SectorRotationBlockBuilder


LOG = get_logger("Fetcher.Ashare")


class AshareDataFetcher(FetcherBase):
    def __init__(self, trade_date: str, refresh_mode:str):
        super().__init__(market="cn", trade_date=trade_date, refresh_mode=refresh_mode)

        # 刷新策略控制器
        #self.rc = RefreshControllerCN(refresh_mode)

        # trade_date 允许显式传入，优先级高于 RefreshController.today
        self.trade_date = trade_date

        LOG.info("[AshareFetcher] 使用 trade_date=%s", self.trade_date)

        # === 初始化数据源（全部使用 DataSourceConfig） ===
        self.index_core_ds = IndexCoreDateSource(
            DataSourceConfig(market="cn", ds_name="index_core")
        )
#
        self.turnover_ds = TurnoverDataSource(
            DataSourceConfig(market="cn", ds_name="turnover")
        )

        self.market_sentiment_ds = MarketSentimentDataSource(
            DataSourceConfig(market="cn", ds_name="market_sentiment")
        )

        self.margin_ds = MarginDataSource(
            DataSourceConfig(market="cn", ds_name="margin")
        )
#
        self.north_nps_ds = NorthNPSDataSource(
            DataSourceConfig(market="cn", ds_name="north_nps")
        )

        

        self.global_lead_ds = GlobalLeadDataSource(
            DataSourceConfig(market="glo", ds_name="global_lead")
        )

        self.index_global_ds = IndexGlobalDataSource(
            DataSourceConfig(market="glo", ds_name="index_global")
        ) 

        self.core_theme_ds =  CoreThemeDataSource(
            DataSourceConfig(market="cn", ds_name="core_theme")
        )     

        self.etf_spot_sync_ds =  ETFSpotSyncDataSource(
            DataSourceConfig(market="cn", ds_name="core_theme")
        )     
        self.global_macro_ds = GlobalMacroDataSource(
                DataSourceConfig(market="glo", ds_name="global_macro")
            ) 

        self.breadth_ds = BreadthDataSource(
            DataSourceConfig(market="cn", ds_name="breadth") )
        # ------------------------------------------------------------------
        # BlockBuilders（结构/解释层，原系统已有）
        # ------------------------------------------------------------------
 
        self.unified_emotion_bb = UnifiedEmotionBlockBuilder()
        self.index_tech_bb = IndexTechBlockBuilder()
        #self.sector_rotation_bb = SectorRotationBlockBuilder()

        #self.sector_rotation_bb = SectorRotationBlockBuilder()
        #self.index_sector_corr_ds = IndexSectorCorrSource(
        #    DataSourceConfig(market="cn", ds_name="index_sector_corr"),
        #    window=20,
        #)
        #self.index_sector_corr_bb = IndexSectorCorrBlockBuilder(window=20)

    def prepare_daily_market_snapshot(self) -> Dict[str, Any]:
        snapshot: Dict[str, Any] = {
            "trade_date": self.trade_date,
        }

        # ==========================================================
        # 1️⃣ Breadth (DS raw)
        # ==========================================================
        snapshot["breadth_raw"] = self.breadth_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.refresh_mode,
        )
        # weekend Sat no data --- to be handle !!! 
        # # Todo assert snapshot.get("breadth_raw"), "Breadth DS missing"

        # ==========================================================
        # 2️⃣ Northbound Proxy (DS raw)
        # ==========================================================
        snapshot["north_nps_raw"] = self.north_nps_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.refresh_mode,
        )
        assert snapshot.get("north_nps_raw"), "North DS missing"

        # ==========================================================
        # 3️⃣ Turnover (DS raw)
        # ==========================================================
        snapshot["turnover_raw"] = self.turnover_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.refresh_mode,
        )
        assert snapshot.get("turnover_raw"), "Turnover DS missing"

        snapshot["market_sentiment_raw"] = self.market_sentiment_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.refresh_mode,
        )
        assert snapshot.get("market_sentiment_raw"), "market_sentiment DS missing"

        snapshot["index_core_raw"] = self.index_core_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.refresh_mode
        )
        assert snapshot.get("index_core_raw"), "index_core DS missing"
 

 

        # ==========================================================
        # 4️⃣ Margin (DS raw)
        # ==========================================================
        snapshot["margin_raw"] = self.margin_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.refresh_mode,
        )
        assert snapshot.get("margin_raw"), "Margin DS missing"

        # ==========================================================
        # 5️⃣ Global / Macro (DS raw)
        # ==========================================================
        snapshot["global_macro_raw"] = self.global_macro_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.refresh_mode,
        )
        assert snapshot.get("global_macro_raw"), "global_macro DS missing"


        snapshot["global_lead_raw"] = self.global_lead_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.refresh_mode,
        )
        assert snapshot.get("global_lead_raw"), "global_lead DS missing"


        snapshot["index_global_raw"] = self.index_global_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.refresh_mode,
        )
        assert snapshot.get("index_global_raw"), "index_global raw missing"   

        snapshot["core_theme_raw"] = self.core_theme_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.refresh_mode,
        )
        assert snapshot.get("core_theme_raw"), "core_theme_raw missing"   


        snapshot["etf_spot_sync_raw"] = self.etf_spot_sync_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.refresh_mode,
        )
        assert snapshot.get("etf_spot_sync_raw"), "etf_spot_sync_raw missing"   


        snapshot["unified_emotion_raw"] = self.unified_emotion_bb.build_block(snapshot, refresh_mode=self.refresh_mode)
        assert snapshot.get("unified_emotion_raw"), "unified_emotion raw missing"


        snapshot["index_tech_raw"] = self.index_tech_bb.build_block(snapshot, refresh_mode=self.refresh_mode)
        assert snapshot.get("index_tech_raw"), "index_tech bb missing"



        LOG.info("[AshareFetcher] Snapshot 数据源加载完成")
        return snapshot
