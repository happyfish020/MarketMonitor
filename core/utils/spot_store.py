# core/utils/spot_store.py
# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - A 股全行情本地存取工具
职责：
    - 按交易日管理 zh_spot parquet
    - 有本地文件 -> 读 parquet
    - 无本地文件 -> 调 ak.stock_zh_a_spot() 并写 parquet
    - 提供 DataFrame 给 turnover / sentiment / 其它因子使用
"""

from __future__ import annotations
import os
from functools import lru_cache

import pandas as pd
import akshare as ak

from core.utils.datasource_config import DataSourceConfig
from core.utils.logger import get_logger

LOG = get_logger("SpotStore")


def _get_spot_path(trade_date: str) -> str:
    """
    根据 trade_date 返回 zh_spot parquet 完整路径：
        data/cn/market/spot/zh_spot_YYYYMMDD.parquet
    """
    cfg = DataSourceConfig(market="cn", ds_name="spot")
    cfg.ensure_dirs()  # 确保 market_root 存在

    base_dir = os.path.join(cfg.market_root, "spot")
    if not os.path.exists(base_dir):
        os.makedirs(base_dir, exist_ok=True)

    filename = f"zh_spot_{trade_date}.parquet"
    return os.path.join(base_dir, filename)


@lru_cache(maxsize=16)
def get_spot_daily(trade_date: str, refresh: bool = False, mode ="snapshot") -> pd.DataFrame:
    """
    获取指定交易日的全行情数据（DataFrame）：
        - 默认优先读本地 parquet
        - refresh=True 时强制用 ak 重新拉取并覆盖本地
        - 单进程内用 lru_cache 防止重复 I/O
    """
    path = _get_spot_path(trade_date)

    LOG.info(f"[SpotStore] Fetch spot via akshare for {trade_date}, mode=={mode}")
    # 1) 有本地文件且不强制刷新 -> 直接读
    df = None
    if mode == "full" or not os.path.exists(path): 
        
        df = ak.stock_zh_a_spot()
    
        # 3) 写 parquet
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_parquet(path, index=False)
        LOG.info(f"[SpotStore] Saved spot parquet: {path}, rows={len(df)}")
    else:    
        if os.path.exists(path): 
            LOG.info(f"[SpotStore] Load spot parquet: {path}")
            df =  pd.read_parquet(path)

            if df is None or df.empty:
                LOG.warning("[SpotStore] 没有本地缓存， 返回空 DataFrame, 强制再 ak spot")
                df = ak.stock_zh_a_spot()

    

    if df is None or df.empty:
        LOG.warning( f"Trade_date: {trade_date}  SpotStore: 无法获取 A 股全行情数据 ak 为空且本地无备份） ")
 


    return df