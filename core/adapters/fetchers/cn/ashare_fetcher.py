# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - CN AShare Fetcher

鑱岃矗锛堝喕缁擄級锛?
- 璋冪敤鍚?DataSource
- 灏?DS raw 鏁版嵁瀹屾暣鍐欏叆 snapshot
- 涓嶅紩鍏?FactorResult
- 涓嶅仛缁撴瀯鍒ゆ柇
"""

from __future__ import annotations

from typing import Dict, Any

from core.adapters.block_builder.cn.watchlist_lead_blkbd import WatchlistLeadBlockBuilder
from core.adapters.datasources.cn.watchlist_supply_source import WatchlistSupplyDataSource
from core.utils.logger import get_logger
from core.adapters.fetchers.fetcher_base import FetcherBase
from core.datasources.datasource_base import DataSourceConfig

# === DataSources ===
from core.adapters.datasources.cn.index_core_source import IndexCoreDateSource
from core.adapters.datasources.cn.breadth_source import BreadthDataSource
from core.adapters.datasources.cn.north_nps_source import NorthNPSDataSource
from core.adapters.datasources.cn.amount_source import AmountDataSource
from core.adapters.datasources.cn.margin_source import MarginDataSource
from core.adapters.datasources.cn.market_sentiment_source import MarketSentimentDataSource
from core.adapters.datasources.cn.core_theme_source import CoreThemeDataSource
from core.adapters.datasources.cn.participation_source import ParticipationDataSource
#from core.adapters.datasources.cn.etf_spot_sync_intraday_source  import ETFSpotSyncIntradayDataSource
from core.adapters.datasources.cn.etf_spot_sync_daily_source  import  ETFSpotSyncDailyDataSource
from core.adapters.datasources.cn.etf_flow_source import ETFFlowDataSource
from core.adapters.datasources.cn.sector_proxy_source import SectorProxyDataSource
from core.adapters.datasources.cn.watchlist_lead_source import WatchlistLeadDataSource
from core.adapters.datasources.glo.global_macro_source import GlobalMacroDataSource
from core.adapters.datasources.glo.global_lead_source import GlobalLeadDataSource
from core.adapters.datasources.glo.index_global_source import IndexGlobalDataSource
from core.adapters.datasources.cn.breadth_plus_source import BreadthPlusDataSource
from core.adapters.datasources.cn.liquidity_quality_source import LiquidityQualityDataSource
from core.adapters.datasources.cn.rotation_snapshot_source import RotationSnapshotDataSource

# Options Risk DataSource (E Block)
from core.adapters.datasources.cn.options_risk_source import OptionsRiskDataSource

# === BlockBuilders / existing producers ===

from core.adapters.block_builder.cn.unified_emotion_blkbd import UnifiedEmotionBlockBuilder
from core.adapters.block_builder.cn.index_tech_blkbd import IndexTechBlockBuilder
from core.adapters.block_builder.cn.sector_rotation_blkbd import SectorRotationBlockBuilder
from core.adapters.block_builder.cn.trend_facts_blkbd import TrendFactsBlockBuilder
import json

LOG = get_logger("Fetcher.Ashare")


class AshareDataFetcher(FetcherBase):
    def __init__(self, trade_date: str, is_intraday:bool= False, refresh_mode:str="none"):
        super().__init__(market="cn", trade_date=trade_date, refresh_mode=refresh_mode)

        # 鍒锋柊绛栫暐鎺у埗鍣?
        #self.rc = RefreshControllerCN(refresh_mode)

        # trade_date 鍏佽鏄惧紡浼犲叆锛屼紭鍏堢骇楂樹簬 RefreshController.today
        self.trade_date = trade_date
        self.is_intraday  = is_intraday
        LOG.info("[AshareFetcher] 浣跨敤 trade_date=%s", self.trade_date)

        # === 鍒濆鍖栨暟鎹簮锛堝叏閮ㄤ娇鐢?DataSourceConfig锛?===
        self.index_core_ds = IndexCoreDateSource(
            DataSourceConfig(market="cn", ds_name="index_core")
        )
#
        self.amount_ds = AmountDataSource(
            DataSourceConfig(market="cn", ds_name="amount")
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
        
        self.sector_proxy_ds = SectorProxyDataSource(
            DataSourceConfig(market="cn", ds_name="sector_proxy")
        )     

        # ----------------------------------------------------------
        # Rotation Snapshot (read-only)
        # - Report 灞傚彧璇?snapshot 琛紱姝ゅ浠呭皢 snapshot 缁撴灉瑁呴厤杩?daily snapshot
        # ----------------------------------------------------------
        self.rotation_snapshot_ds = RotationSnapshotDataSource(
            DataSourceConfig(market="cn", ds_name="rotation_snapshot")
        )
 

        
        self.participation_ds = ParticipationDataSource(
            DataSourceConfig(market="cn", ds_name="participation")
        )    

  

        self.etf_spot_sync_daily_ds =  ETFSpotSyncDailyDataSource(
            DataSourceConfig(market="cn", ds_name="etf_spot_sync_daily")
        )     

        # ETF Flow DataSource (C Block)
        self.etf_flow_ds = ETFFlowDataSource(
            DataSourceConfig(market="cn", ds_name="etf_flow")
        )

        # Futures Basis DataSource (D Block)
        from core.adapters.datasources.cn.futures_basis_source import FuturesBasisDataSource
        self.futures_basis_ds = FuturesBasisDataSource(
            DataSourceConfig(market="cn", ds_name="futures_basis")
        )

        # Liquidity Quality DataSource (F Block)
        self.liquidity_quality_ds = LiquidityQualityDataSource(
            DataSourceConfig(market="cn", ds_name="liquidity_quality")
        )

        # Options Risk DataSource (E Block)
        self.options_risk_ds = OptionsRiskDataSource(
            DataSourceConfig(market="cn", ds_name="options_risk")
        )
 

        self.global_macro_ds = GlobalMacroDataSource(
                DataSourceConfig(market="glo", ds_name="global_macro")
            ) 

        self.breadth_ds = BreadthDataSource(
            DataSourceConfig(market="cn", ds_name="breadth") )

        # ------------------------------------------------------------------
        # WatchlistLead锛堢嫭绔嬭瀵熷眰 raw锛屼笉鍙備笌 Gate/DRS锛?
        # - 涓嶅仛 assert锛氱己鏁版嵁鍏佽锛堣緭鍑?MISSING + warnings锛?
        # ------------------------------------------------------------------
        self.watchlist_lead_ds = WatchlistLeadDataSource(
            DataSourceConfig(market="cn", ds_name="watchlist_lead")
        )

        self.watchlist_supply_ds = WatchlistSupplyDataSource(
            DataSourceConfig(market="cn", ds_name="watchlist_supply")
        )
        
        
        # ------------------------------------------------------------------
        # BlockBuilders锛堢粨鏋?瑙ｉ噴灞傦紝鍘熺郴缁熷凡鏈夛級
        # ------------------------------------------------------------------
 
        self.unified_emotion_bb = UnifiedEmotionBlockBuilder()
        self.index_tech_bb = IndexTechBlockBuilder()

        self.trend_facts_bb = TrendFactsBlockBuilder() 

        self.breadth_plus_ds = BreadthPlusDataSource(
            DataSourceConfig(market="cn", ds_name="breadth_plus"),
            
        )

    
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
        # 1锔忊儯 Breadth (DS raw)
        # ==========================================================
        snapshot["breadth_raw"] = self.breadth_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.refresh_mode,
        )
        # weekend Sat no data --- to be handle !!! 
        # # Todo 
        assert snapshot.get("breadth_raw"), "Breadth DS missing"
       
        # ==========================================================
        # 2锔忊儯 Northbound Proxy (DS raw)
        # ==========================================================
        snapshot["north_nps_raw"] = self.north_nps_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.refresh_mode,
        )
        assert snapshot.get("north_nps_raw"), "North DS missing"

        # ==========================================================
        # 3锔忊儯 Amount (DS raw)
        # ==========================================================
        snapshot["amount_raw"] = self.amount_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.refresh_mode,
        )
        assert snapshot.get("amount_raw"), "Amount DS missing"

        snapshot["market_sentiment_raw"] = self.market_sentiment_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.refresh_mode,
        )

        
      
        assert snapshot.get("market_sentiment_raw"), "market_sentiment DS missing"
        with open(r"run\\temp\\market_sentiment_raw.json", "w", encoding="utf-8") as f:
            json.dump(snapshot.get("market_sentiment_raw"), f, ensure_ascii=False, indent=2)
        


        snapshot["index_core_raw"] = self.index_core_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.refresh_mode
        )
        assert snapshot.get("index_core_raw"), "index_core DS missing"
        
        with open(r"run\\temp\\index_core_rawtch.json", "w", encoding="utf-8") as f:
            json.dump(snapshot.get("index_core_raw"), f, ensure_ascii=False, indent=2)
        
        # ==========================================================
        # C锔忊儯 ETF Flow (DS raw)
        # ==========================================================
        snapshot["etf_flow_raw"] = self.etf_flow_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.refresh_mode,
        )
        assert snapshot.get("etf_flow_raw"), "ETF Flow DS missing"
        with open(r"run\\temp\\etf_flow_raw.json", "w", encoding="utf-8") as f:
            json.dump(snapshot.get("etf_flow_raw"), f, ensure_ascii=False, indent=2)
        
        # ==========================================================
        # D锔忊儯 Futures Basis (DS raw)
        # ==========================================================
        snapshot["futures_basis_raw"] = self.futures_basis_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.refresh_mode,
        )
        assert snapshot.get("futures_basis_raw"), "Futures Basis DS missing"
        with open(r"run\\temp\\futures_basis_raw.json", "w", encoding="utf-8") as f:
            json.dump(snapshot.get("futures_basis_raw"), f, ensure_ascii=False, indent=2)
        
        # ==========================================================
        # F锔忊儯 Liquidity Quality (DS raw)
        # ==========================================================
        snapshot["liquidity_quality_raw"] = self.liquidity_quality_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.refresh_mode,
        )
        assert snapshot.get("liquidity_quality_raw"), "Liquidity Quality DS missing"
        with open(r"run\\temp\\liquidity_quality_raw.json", "w", encoding="utf-8") as f:
            json.dump(snapshot.get("liquidity_quality_raw"), f, ensure_ascii=False, indent=2)
        
        # ==========================================================
        # E锔忊儯 Options Risk (DS raw)
        # ==========================================================
        snapshot["options_risk_raw"] = self.options_risk_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.refresh_mode,
        )
        assert snapshot.get("options_risk_raw"), "Options Risk DS missing"
        with open(r"run\\temp\\options_risk_raw.json", "w", encoding="utf-8") as f:
            json.dump(snapshot.get("options_risk_raw"), f, ensure_ascii=False, indent=2)
      

             

        # ==========================================================
        # 4锔忊儯 Margin (DS raw)
        # ==========================================================
        snapshot["margin_raw"] = self.margin_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.refresh_mode,
        )
        assert snapshot.get("margin_raw"), "Margin DS missing"
        # margin_intensity_raw: compatibility alias for Leading-Structure panel G
        # Prefer explicit margin_intensity_raw if upstream provides it; otherwise fallback to margin_raw.
        _mi = snapshot.get("margin_intensity_raw")
        
        if not (isinstance(_mi, dict) and bool(_mi)):
            _mr = snapshot.get("margin_raw")
            snapshot["margin_intensity_raw"] = _mr if isinstance(_mr, dict) else {}
            with open(r"run\\temp\\margin_intensity_raw.json", "w", encoding="utf-8") as f:
                json.dump(snapshot["margin_intensity_raw"], f, ensure_ascii=False, indent=2)
        
        with open(r"run\\temp\\margin_raw.json", "w", encoding="utf-8") as f:
            json.dump(snapshot["margin_raw"], f, ensure_ascii=False, indent=2)
        
          
        # ==========================================================
        # 5锔忊儯 Global / Macro (DS raw)
        # ==========================================================
        snapshot["global_macro_raw"] = self.global_macro_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.refresh_mode,
        )
        assert snapshot.get("global_macro_raw"), "global_macro DS missing"
        with open(r"run\\temp\\global_macro_raw.json", "w", encoding="utf-8") as f:
            json.dump(snapshot.get("global_macro_raw"), f, ensure_ascii=False, indent=2)



        snapshot["global_lead_raw"] = self.global_lead_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.refresh_mode,
        )
        assert snapshot.get("global_lead_raw"), "global_lead DS missing"
        with open(r"run\\temp\\global_lead_raw.json", "w", encoding="utf-8") as f:
            json.dump(snapshot.get("global_lead_raw"), f, ensure_ascii=False, indent=2)


        snapshot["index_global_raw"] = self.index_global_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.refresh_mode,
        )
        assert snapshot.get("index_global_raw"), "index_global raw missing"   
        with open(r"run\\temp\\index_global_raw.json", "w", encoding="utf-8") as f:
            json.dump(snapshot.get("index_global_raw"), f, ensure_ascii=False, indent=2)
         

        snapshot["core_theme_raw"] = self.core_theme_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.refresh_mode,
        )
        assert snapshot.get("core_theme_raw"), "core_theme_raw missing"   
        with open(r"run\\temp\\core_theme_raw.json", "w", encoding="utf-8") as f:
            json.dump(snapshot.get("core_theme_raw"), f, ensure_ascii=False, indent=2)
         

        snapshot["sector_proxy_raw"] = self.sector_proxy_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.refresh_mode,
        )
        assert snapshot.get("sector_proxy_raw"), "sector_proxy_raw  missing" 
        with open(r"run\\temp\\sector_proxy_raw.json", "w", encoding="utf-8") as f:
            json.dump(snapshot["sector_proxy_raw"] , f, ensure_ascii=False, indent=2)    

        # ==========================================================
        # Rotation Snapshot (DB snapshot tables; report-only evidence)
        # ==========================================================
        snapshot["rotation_snapshot_raw"] = self.rotation_snapshot_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.refresh_mode,
        )
        # This DS is expected to always return a dict (at least EMPTY with meta)
        assert snapshot.get("rotation_snapshot_raw") is not None, "rotation_snapshot_raw missing"
        with open(r"run\\temp\\rotation_snapshot_raw.json", "w", encoding="utf-8") as f:
            json.dump(snapshot["rotation_snapshot_raw"], f, ensure_ascii=False, indent=2)
            
        snapshot["unified_emotion_raw"] = self.unified_emotion_bb.build_block(snapshot, refresh_mode=self.refresh_mode)
        assert snapshot.get("unified_emotion_raw"), "unified_emotion raw missing"
        with open(r"run\\temp\\unified_emotion_raw.json", "w", encoding="utf-8") as f:
            json.dump(snapshot["unified_emotion_raw"] , f, ensure_ascii=False, indent=2)    

        snapshot["participation_raw"] = self.participation_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.refresh_mode,
        )
        assert snapshot.get("participation_raw"), "participation_raw missing"  
        with open(r"run\\temp\\participation_raw.json", "w", encoding="utf-8") as f:
            json.dump(snapshot["participation_raw"] , f, ensure_ascii=False, indent=2)    



        #snapshot["etf_spot_sync"] = self.etf_spot_sync_ds.build_block(
        #    trade_date=self.trade_date,
        #    refresh_mode=self.refresh_mode,
        #)
        #assert snapshot.get("etf_spot_sync"), "etf_spot_sync missing"   

        snapshot["etf_spot_sync_daily"] = self.etf_spot_sync_daily_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.refresh_mode,
        )
        assert snapshot.get("etf_spot_sync_daily"), "etf_spot_synetf_spot_sync_dailyc missing"   
        with open(r"run\\temp\\etf_spot_sync_daily.json", "w", encoding="utf-8") as f:
            json.dump(snapshot["etf_spot_sync_daily"] , f, ensure_ascii=False, indent=2)    


        # ==========================================================
        # WatchlistLead (DS raw) - best effort (DS handles fallback/neutral)
        # ==========================================================
        snapshot["watchlist_lead_raw"] = self.watchlist_lead_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.refresh_mode,
           
        )
        assert snapshot.get("watchlist_lead_raw"), "watchlist_lead_raw bb missing"
        with open(r"run\\temp\\watchlist_lead_raw.json", "w", encoding="utf-8") as f:
            json.dump(snapshot["watchlist_lead_raw"] , f, ensure_ascii=False, indent=2)    

        LOG.info("Done - watchlist_lead_raw")
        snapshot["watchlist_supply_raw"] = self.watchlist_supply_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.refresh_mode,
        )
        LOG.info("Done - watchlist_supply_raw")
        assert snapshot.get("watchlist_supply_raw"), "watchlist_supply_raw bb missing"
        with open(r"run\\temp\\watchlist_supply_raw.json", "w", encoding="utf-8") as f:
            json.dump(snapshot["watchlist_supply_raw"] , f, ensure_ascii=False, indent=2)    

        snapshot["breadth_plus_raw"] = self.breadth_plus_ds.build_block(trade_date=self.trade_date,
            refresh_mode=self.refresh_mode,)
        assert snapshot.get("breadth_plus_raw"), "breadth_plus_raw bb missing"
        with open(r"run\\temp\\breadth_plus_raw.json", "w", encoding="utf-8") as f:
            json.dump(snapshot["breadth_plus_raw"] , f, ensure_ascii=False, indent=2)

        snapshot["watchlist_lead_input_raw"] = WatchlistLeadBlockBuilder().build_block(snapshot )
        assert snapshot.get("watchlist_lead_input_raw"), "watchlist_lead_input_raw bb missing"
        with open(r"run\\temp\\watchlist_lead_input_raw.json", "w", encoding="utf-8") as f:
            json.dump(snapshot["watchlist_lead_input_raw"] , f, ensure_ascii=False, indent=2)
        
          


        snapshot["index_tech"] = self.index_tech_bb.build_block(snapshot)
        assert snapshot.get("index_tech"), "index_tech bb missing"
        with open(r"run\\temp\\index_tech.json", "w", encoding="utf-8") as f:
            json.dump(snapshot["index_tech"] , f, ensure_ascii=False, indent=2)




        snapshot["trend_in_force"] = self.trend_facts_bb.build_block(snapshot)
        assert snapshot.get("trend_in_force"), "trend_in_force_raw bb missing"
        with open(r"run\\temp\\trend_in_force.json", "w", encoding="utf-8") as f:
            json.dump(snapshot["trend_in_force"] , f, ensure_ascii=False, indent=2)
        
        
        


        #json_path = os.path.join( "scripts/trend_in_force", f"trend_in_force_{self.trade_date}.json")
        #with open(json_path, "w", encoding="utf-8") as f:
        #    json.dump(snapshot["trend_in_force"] , f, ensure_ascii=False, indent=2)

        LOG.info("[AshareFetcher] snapshot build completed")
        return snapshot

