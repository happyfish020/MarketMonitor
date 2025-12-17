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
from core.adapters.fetchers.base_fetcher import FetcherBase
from core.datasources.datasource_base import DataSourceConfig

# === DataSources ===
from core.adapters.datasources.cn.breadth_source import BreadthDataSource
from core.adapters.datasources.cn.north_nps_source import NorthNPSDataSource
from core.adapters.datasources.cn.turnover_source import TurnoverDataSource
from core.adapters.datasources.cn.margin_source import MarginDataSource
from core.adapters.datasources.cn.global_macro_source import GlobalMacroDataSource
from core.adapters.datasources.cn.global_lead_source import GlobalLeadDataSource
from core.adapters.datasources.cn.index_global_source import IndexGlobalSource

LOG = get_logger("Fetcher.Ashare")


class AshareFetcher(FetcherBase):
    def __init__(self, trade_date: str, refresh_controller):
        super().__init__(market="cn", trade_date=trade_date, refresh_controller=refresh_controller)

        # === DataSourceConfig ===
        self.cfg_breadth = DataSourceConfig(market="cn", ds_name="breadth")
        self.cfg_north = DataSourceConfig(market="cn", ds_name="north_nps_raw")
        self.cfg_turnover = DataSourceConfig(market="cn", ds_name="turnover")
        self.cfg_margin = DataSourceConfig(market="cn", ds_name="margin")
        self.cfg_global_macro = DataSourceConfig(market="glo", ds_name="global_macro")
        self.cfg_global_lead = DataSourceConfig(market="glo", ds_name="global_lead")
        self.cfg_index_global = DataSourceConfig(market="glo", ds_name="index_global")

        # === DataSources ===
        self.breadth_ds = BreadthDataSource(self.cfg_breadth)
        self.north_nps_ds = NorthNPSDataSource(self.cfg_north)
        self.turnover_ds = TurnoverDataSource(self.cfg_turnover)
        self.margin_ds = MarginDataSource(self.cfg_margin)
        self.global_macro_ds = GlobalMacroDataSource(self.cfg_global_macro)
        self.global_lead_ds = GlobalLeadDataSource(self.cfg_global_lead)
        self.index_global_ds = IndexGlobalSource(self.cfg_index_global)

    def prepare_daily_market_snapshot(self) -> Dict[str, Any]:
        snapshot: Dict[str, Any] = {
            "trade_date": self.trade_date,
        }

        # ==========================================================
        # 1️⃣ Breadth (DS raw)
        # ==========================================================
        snapshot["breadth"] = self.breadth_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.rc.refresh_mode,
        )
        assert snapshot.get("breadth"), "Breadth DS missing"

        # ==========================================================
        # 2️⃣ Northbound Proxy (DS raw)
        # ==========================================================
        snapshot["north_nps_raw"] = self.north_nps_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.rc.refresh_mode,
        )
        assert snapshot.get("north_nps_raw"), "North DS missing"

        # ==========================================================
        # 3️⃣ Turnover (DS raw)
        # ==========================================================
        snapshot["turnover_raw"] = self.turnover_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.rc.refresh_mode,
        )
        assert snapshot.get("turnover_raw"), "Turnover DS missing"

        # ==========================================================
        # 4️⃣ Margin (DS raw)
        # ==========================================================
        snapshot["margin_raw"] = self.margin_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.rc.refresh_mode,
        )
        assert snapshot.get("margin_raw"), "Margin DS missing"

        # ==========================================================
        # 5️⃣ Global / Macro (DS raw)
        # ==========================================================
        snapshot["global_macro_raw"] = self.global_macro_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.rc.refresh_mode,
        )

        snapshot["global_lead_raw"] = self.global_lead_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.rc.refresh_mode,
        )

        snapshot["index_global_raw"] = self.index_global_ds.build_block(
            trade_date=self.trade_date,
            refresh_mode=self.rc.refresh_mode,
        )

        LOG.info("[AshareFetcher] Snapshot 数据源加载完成")
        #return snapshot

         # ------------------------------------------------------------
        # ⑧ SnapshotBuilderV12 构建最终结构
        # ------------------------------------------------------------
        builder = AshareSnapshotBuilder()
        final_snapshot = builder.build(snapshot)
        
        assert snapshot.get("breadth"), "Breadth DS missing"
        assert snapshot.get("north_nps_raw"), "North DS missing"
        assert snapshot.get("turnover_raw"), "Turnover DS missing"
        
        
        # ------------------------------------------------------------
        # Phase-2 pillar output: Index–Sector Correlation (pillar object)
        # Append-only (do NOT overwrite existing)
        # ------------------------------------------------------------
        if final_snapshot.get("index_sector_corr") is None:
            final_snapshot["index_sector_corr"] = self.index_sector_corr_tr.transform(
                final_snapshot,
                trade_date=self.trade_date,
                refresh_mode=self.rc.refresh_mode,
                ds=self.index_sector_corr_ds,
            ) 
        try:
            LOG.info("[AshareFetcher] Snapshot 数据源加载完成，开始组装 snapshot ...")
            LOG.info("[AshareFetcher] Snapshot 构建完成 trade_date=%s", self.trade_date)
        except Exception:
            pass

        return final_snapshot
