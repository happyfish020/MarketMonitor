import os
from datetime import datetime
from typing import Dict, Any

from core.utils.logger import get_logger
from core.datasources.datasource_base import DataSourceConfig,BaseDataSource
from core.snapshot.ashare_snapshot  import  AshareSnapshotBuilder
from core.adapters.fetchers.cn.refresh_controller_cn import RefreshControllerCN

# ==== DataSources (V12 标准) ====
from core.adapters.datasources.cn.index_core_source import IndexCoreSource
from core.adapters.datasources.cn.turnover_source import TurnoverDataSource
from core.adapters.datasources.cn.market_sentiment_source import MarketSentimentDataSource
from core.adapters.datasources.cn.margin_source import MarginDataSource
from core.adapters.datasources.cn.north_nps_source import NorthNPSSource
#from core.adapters.datasources.cn.unified_emotion_source import UnifiedEmotionDataSource
from core.adapters.datasources.glo.global_lead_source import GlobalLeadSource
from core.adapters.datasources.glo.index_global_source import IndexGlobalSource
from core.adapters.datasources.glo.global_macro_source import GlobalMacroSource
#from core.adapters.datasources.cn.index_tech_source import IndexTechDataSource


from core.adapters.transformers.cn.index_tech_tr import IndexTechTransformer
from core.adapters.transformers.cn.unified_emotion_tr import UnifiedEmotionTransformer
from core.adapters.transformers.cn.sector_rotation_tr import SectorRotationTransformer

LOG = get_logger("AshareFetcher")


class AshareDataFetcher:
    """
    V12 A 股日度数据抓取总入口
    负责：
      - 初始化全部数据源（DataSourceConfig 注入）
      - 使用 RefreshController 判断刷新策略
      - 逐个数据源获取原始数据块
      - 统一交给 SnapshotBuilderV12 生成最终 snapshot
    """

    def __init__(self, trade_date: str, refresh_mode: str = "auto"):
            """
            V12 标准构造：
              - trade_date 由 engine 显式传入
              - refresh_mode 用于 RefreshController 判断刷新策略
            """
            LOG.info(
                "[AshareFetcher] 初始化 AshareDataFetcher, trade_date=%s refresh_mode=%s",
                trade_date, refresh_mode
            )
    
            # 刷新策略控制器
            self.rc = RefreshControllerCN(refresh_mode)
    
            # trade_date 允许显式传入，优先级高于 RefreshController.today
            self.trade_date = trade_date
    
            LOG.info("[AshareFetcher] 使用 trade_date=%s", self.trade_date)
    
            # === 初始化数据源（全部使用 DataSourceConfig） ===
            self.index_core = IndexCoreSource(
                DataSourceConfig(market="cn", ds_name="index_core")
            )
    #
            self.turnover = TurnoverDataSource(
                DataSourceConfig(market="cn", ds_name="turnover")
            )
    
            self.market_sentiment = MarketSentimentDataSource(
                DataSourceConfig(market="cn", ds_name="market_sentiment")
            )
    

            self.margin_source = MarginDataSource(
                DataSourceConfig(market="cn", ds_name="margin")
            )
    #
            self.north_nps_source = NorthNPSSource(
                DataSourceConfig(market="cn", ds_name="north_nps")
            )
    
 

            #self.emotion_ds = UnifiedEmotionDataSource(
            #    DataSourceConfig(market="cn", ds_name="emotion")
            #)
    
            self.global_lead = GlobalLeadSource(
                DataSourceConfig(market="glo", ds_name="global_lead")
            )
    
            self.index_global = IndexGlobalSource(
                DataSourceConfig(market="glo", ds_name="index_global")
            ) 

            self.macro_source = GlobalMacroSource(
                DataSourceConfig(market="glo", ds_name="global_macro")
            ) 

            self.index_tech_tr = IndexTechTransformer(window=60)
            self.unified_emotion_tr = UnifiedEmotionTransformer()
            self.sector_rotation_tr = SectorRotationTransformer()
            #self.index_tech_ds = IndexTechDataSource(
            #        DataSourceConfig(market="cn", ds_name="index_tech")
            #    )
    # ==============================================================
    # 主流程：构建 Snapshot（提供给 engine → factor → reporter）
    # ==============================================================
    def prepare_daily_market_snapshot(self) -> Dict[str, Any]:
        LOG.info("[AshareFetcher] 开始构建 Snapshot V12")

        snapshot: Dict[str, Any] = {}

        # ------------------------------------------------------------
        # ① 核心指数（上证 / 深成 / HS300 / ZZ500 / KC50）
        # ------------------------------------------------------------
        LOG.info("[AshareFetcher] [1] 获取核心指数 index_core ...")
 

        snapshot["index_core"] = self.index_core.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.rc.refresh_mode
        )
    
    

        # ------------------------------------------------------------
        # ② 成交额 Turnover（沪深北）
        # ------------------------------------------------------------
        #LOG.info("[AshareFetcher] [2] 获取成交额 Turnover ...")
        #snapshot["turnover"] = self.turnover_source.get_turnover_block(
        #    trade_date=self.trade_date,
        #    refresh_mode=self.rc.refresh_mode
        #)

        # ------------------------------------------------------------
        # ③ 北向 NPS
        # ------------------------------------------------------------
        LOG.info("[AshareFetcher] [3] 获取北向 NorthNPS ...")
        snapshot["north_nps"] = self.north_nps_source.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.rc.refresh_mode
        )

        # ------------------------------------------------------------
        # ④ 两融 Margin
        # ------------------------------------------------------------
        LOG.info("[AshareFetcher] [4] 获取两融 Margin ...")
        snapshot["margin"] = self.margin_source.build_block(
            trade_date=self.trade_date,
            refresh_mode    =self.rc.refresh_mode
        )

        # ------------------------------------------------------------
        # ⑤ 情绪（市场内部 MarketSentiment + 行为因子 Behavior）
        # ------------------------------------------------------------
        #LOG.info("[AshareFetcher] [5] 获取情绪结构 UnifiedEmotion ...")
        #sentiment_block, emotion_block = self.emotion_ds.get_blocks(
        #    snapshot=snapshot,
        #    trade_date=self.trade_date,
        #    refresh_mode=self.rc.refresh_mode
        #)
        snapshot["turnover"] = self.turnover.build_block(
            trade_date=self.trade_date,
            refresh_mode    =self.rc.refresh_mode
        )
        
        snapshot["market_sentiment"] = self.market_sentiment.build_block(
            trade_date=self.trade_date,
            refresh_mode    =self.rc.refresh_mode
        )

       

        #snapshot["emotion"] = emotion_block




        # ------------------------------------------------------------
        # ⑥ 全球引导 GlobalLead
        # ------------------------------------------------------------
        LOG.info("[AshareFetcher] [6] 获取 Global Lead ...")
        snapshot["global_lead"] = self.global_lead.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.rc.refresh_mode
        )

        # ------------------------------------------------------------
        # ⑦ 全球指数强弱（美股、日经、恒生等）
        # ------------------------------------------------------------
        LOG.info("[AshareFetcher] [7] 获取全球指数 index_global ...")
        snapshot["index_global"] = self.index_global.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.rc.refresh_mode
        )

        
        snapshot["global_macro"] = self.macro_source.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.rc.refresh_mode
        )
 
        # === IndexTech （技术面数据源）
        

        
        snapshot["index_tech"] = self.index_tech_tr.transform(snapshot, refresh_mode=self.rc.refresh_mode)
        
        
        snapshot["unified_emotion"] = self.unified_emotion_tr.transform(snapshot, refresh_mode=self.rc.refresh_mode)

        snapshot["sector_rotation"] = self.sector_rotation_tr.transform(snapshot, refresh_mode=self.rc.refresh_mode)

        #LOG.info("[AshareFetcher] unified_emotion snapshot: %s", snapshot["unified_emotion"])
        #snapshot["index_tech"] = self.index_tech_ds.build_index_tech(snapshot)
        #LOG.info("[AshareFetcher] index_tech snapshot: %s", snapshot["index_tech"])
#
        # ------------------------------------------------------------
        # ⑧ SnapshotBuilderV12 构建最终结构
        # ------------------------------------------------------------
        LOG.info("[AshareFetcher] Snapshot 数据源加载完成，开始组装 snapshot ...")
        builder = AshareSnapshotBuilder()
        final_snapshot = builder.build(snapshot)

        LOG.info("[AshareFetcher] Snapshot 构建完成 trade_date=%s", self.trade_date)
        return final_snapshot
