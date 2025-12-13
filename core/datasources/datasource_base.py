# core/adapters/datasources/base.py

"""
UnifiedRisk V12
DataSource Base Class（底层统一接口）
"""

from typing import Optional, Any, Dict

from core.utils.logger import get_logger

LOG = get_logger("DS.Base")

# core/utils/datasource_config.py
"""
UnifiedRisk V12 - DataSourceConfig
数据源统一目录配置（cache / history / market）
"""

import os
from dataclasses import dataclass

from core.utils.logger import get_logger
from functools import lru_cache
from core.utils.config_loader import load_paths

LOG = get_logger("DS.Config")

 
@dataclass
class DataSourceConfig:
    """
    V12 数据源配置：
        market: "cn" / "glo" / "us" / ...
        ds_name: 数据源目录名（例：index_series / turnover / margin）
    
    自动生成：
        data_root / market_root / cache_root / history_root
    """

    market: str             # 例如 "cn", "glo"
    ds_name: str = ""       # 数据源名称（子目录）

    def __post_init__(self):

        paths_cfg = load_paths()

        # 从 YAML 获取 data_root，如 data/
        yaml_data_root = paths_cfg.get("data_root", "data")

        # 解析为绝对路径（如 D:/MarketMonitor/data）
        self.data_root = os.path.abspath(yaml_data_root)

        # 预构建市场级路径
        self.market_root = os.path.join(self.data_root, self.market)

        # cache/history 根目录（不含 symbol）
        self.cache_root = os.path.join(self.market_root, "cache", self.ds_name)
        self.history_root = os.path.join(self.market_root, "history", self.ds_name)

        # 创建必要目录
        self.ensure_dirs()



        LOG.info(
            "[DataSourceConfig] market=%s ds_name=%s\n"
            "  data_root=%s\n"
            "  market_root=%s\n"
            "  cache_root=%s\n"
            "  history_root=%s",
            self.market,
            self.ds_name,
            self.data_root,
            self.market_root,
            self.cache_root,
            self.history_root,
        )

    # ------------------------------------------------------------------
    def ensure_dirs(self):
        """
        创建所有必要目录。
        """
        for path in [self.market_root, self.cache_root, self.history_root]:
            try:
                os.makedirs(path, exist_ok=True)
                LOG.info("EnsureDir: %s", path)
            except Exception as e:
                LOG.error("EnsureDirError: path=%s err=%s", path, e)

class BaseDataSource:
    """
    所有 DataSource 的基础类（可选）
    目前主要用于统一日志格式
    """

    def __init__(self, name: str = "DataSource"):
        self.name = name
        LOG.info("Init: %s", self.name)

    # Helper：子类可调用标准日志
    def log_info(self, msg: str, **kwargs):
        kv = " ".join(f"{k}={v}" for k, v in kwargs.items())
        LOG.info("%s: %s %s", self.name, msg, kv)

    def log_warn(self, msg: str, **kwargs):
        kv = " ".join(f"{k}={v}" for k, v in kwargs.items())
        LOG.warning("%s: %s %s", self.name, msg, kv)

    def log_error(self, msg: str, **kwargs):
        kv = " ".join(f"{k}={v}" for k, v in kwargs.items())
        LOG.error("%s: %s %s", self.name, msg, kv)
