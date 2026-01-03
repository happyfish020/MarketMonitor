# core/utils/spot_store.py
# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - A 股全行情本地存取工具
职责：
    - 按交易日管理 zh_spot parquet
    - 有本地文件 -> 读 parquet
    - 无本地文件 / full 刷新 -> 调 ak.stock_zh_a_spot() 并写 parquet
    - 提供 DataFrame 给 amount / sentiment / 其它因子使用
"""

from __future__ import annotations

import os
from functools import lru_cache

import pandas as pd
import akshare as ak

from core.datasources.datasource_base import DataSourceConfig,DataSourceBase
from core.utils.logger import get_logger
from core.adapters.cache.symbol_cache import normalize_ashare_symbol

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
def get_spot_daily(trade_date: str, refresh_mode: str = "none") -> pd.DataFrame:
    """
    获取指定交易日的全行情数据（DataFrame）

    参数：
        trade_date : "YYYYMMDD" / "YYYY-MM-DD"
       
        refresh_mode       : "full" / "snapshot" / "none"

    统一语义（按照你刚才说的）：

    1) refresh_mode in ("none", "readonly") "full":
        - 保险做法：如果 parquet 存在，先删除本地文件
        - 然后进入统一逻辑（读文件；如果文件不存在则拉 ak）

    2) mrefresh_mode  in ("snapshot", "none"):
        - 如果本地文件存在 → 只读 parquet
        - 如果本地文件不存在 → 调 ak.stock_zh_a_spot() 拉一次 & 写 parquet
    """
    path = _get_spot_path(trade_date)
    # 时间戳 15:35:45
    LOG.info(f"[SpotStore] get_spot_daily trade_date={trade_date}, mode={refresh_mode}")

    # ========== 1) full 模式：先删文件，再走统一逻辑 ==========
    if refresh_mode == "full":
        if os.path.exists(path):
            try:
                os.remove(path)
                LOG.info(f"[SpotStore] full mode: 删除旧 spot 文件: {path}")
            except Exception as e:
                LOG.warning(f"[SpotStore] full mode: 删除旧文件失败: {e}")

    df: pd.DataFrame | None = None

    # ========== 2) 统一逻辑：优先读 parquet，缺失再拉 ak ==========
    if os.path.exists(path):
        # 2.1 优先读本地 parquet
        try:
            LOG.info(f"[SpotStore] Load spot parquet: {path}")
            df = pd.read_parquet(path)
        except Exception as e:
            LOG.error(f"[SpotStore] 读取 parquet 失败: {e}")
            df = None

        # 2.2 如果读出来是空的，再拉 ak
        if df is None or df.empty:
            LOG.warning("[SpotStore] 本地 spot 数据为空，尝试拉取 ak.stock_zh_a_spot()")
            try:
                df = ak.stock_zh_a_spot()
            except Exception as e:
                LOG.error(f"[SpotStore] ak.stock_zh_a_spot() 调用失败: {e}")
                df = None

            # 拉到数据就覆盖写回 parquet
            if df is not None and not df.empty:
                try:
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    df.to_parquet(path, index=False)
                    LOG.info(f"[SpotStore] Saved spot parquet (overwrite): {path}, rows={len(df)}")
                except Exception as e:
                    LOG.error(f"[SpotStore] 保存 parquet 失败: {e}")
    else:
        # 2.3 本地没有 parquet → 必须拉一次 ak
        LOG.info(f"[SpotStore] 本地无 parquet，拉取 ak.stock_zh_a_spot() for {trade_date}")
        try:
            df = ak.stock_zh_a_spot()
        except Exception as e:
            LOG.error(f"[SpotStore] ak.stock_zh_a_spot() 调用失败: {e}")
            df = None

        if df is not None and not df.empty:
            try:
                os.makedirs(os.path.dirname(path), exist_ok=True)
                df.to_parquet(path, index=False)
                LOG.info(f"[SpotStore] Saved spot parquet: {path}, rows={len(df)}")
            except Exception as e:
                LOG.error(f"[SpotStore] 保存 parquet 失败: {e}")

    # ========== 3) 最终兜底 ==========
    if df is None or df.empty:
        LOG.warning(f"[SpotStore] 无法获取 A 股全行情数据（trade_date={trade_date}）")
        return pd.DataFrame()

    # ========== 4) 标准化 symbol 字段 ==========
    if "代码" in df.columns:
        try:
            df["symbol"] = df["代码"].apply(normalize_ashare_symbol)
            LOG.info("[SpotStore] 标准化 symbol 字段完成")
        except Exception as e:
            LOG.error(f"[SpotStore] symbol 标准化失败: {e}")

    return df
