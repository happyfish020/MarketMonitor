# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - IndexSectorCorrBlockBuilder

职责：
- 基于 snapshot + IndexSectorCorrDataSource 输出
- 构建“指数-行业相关性”结构 pillar
- 不在构造器中绑定 DataSource
"""

from typing import Dict, Any, List
import numpy as np

from core.utils.logger import get_logger
from core.adapters.datasources.cn.index_sector_corr_source import IndexSectorCorrDataSource
from core.adapters.block_builder.block_builder_base import FactBlockBuilderBase

LOG = get_logger("BlockBuilder.IndexSectorCorr")


class IndexSectorCorrBlockBuilder(FactBlockBuilderBase):
    """
    参数：
    - window: 相关性计算窗口（交易日）
    """

    def __init__(self, window: int = 20):
        self.window = int(window)

##
    def transform(
        self,
        snapshot: Dict[str, Any],
        trade_date: str,
        refresh_mode: str = "auto",
        ds: IndexSectorCorrDataSource | None = None,
    ) -> Dict[str, Any]:
    
        if ds is None:
            LOG.warning("[IndexSectorCorrTR] no datasource provided, skip")
            return {}
    
        raw = ds.build_block(trade_date=trade_date, refresh_mode=refresh_mode)
        if not raw:
            LOG.info("[IndexSectorCorrTR] empty raw block")
            return {}
    
        index_ret = raw.get("index_returns")
        sector_ret = raw.get("sector_returns")
    
        # === 🔴 关键防御：必须是可迭代序列 ===
        if not isinstance(index_ret, (list, tuple)) or not isinstance(sector_ret, (list, tuple)):
            LOG.warning(
                "[IndexSectorCorrTR] invalid raw types: index_returns=%s sector_returns=%s",
                type(index_ret),
                type(sector_ret),
            )
            return {}
    
        if len(index_ret) < self.window or len(sector_ret) < self.window:
            LOG.warning(
                "[IndexSectorCorrTR] insufficient data len(index)=%s len(sector)=%s window=%s",
                len(index_ret),
                len(sector_ret),
                self.window,
            )
            return {}
    
        # === 正常计算 ===
        corr = float(
            np.corrcoef(
                index_ret[-self.window :],
                sector_ret[-self.window :],
            )[0, 1]
        )
    
        return {
            "window": self.window,
            "corr": corr,
            "detail": {
                "len_index": len(index_ret),
                "len_sector": len(sector_ret),
            },
        }
    

##