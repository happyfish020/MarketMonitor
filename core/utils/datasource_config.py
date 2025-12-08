# core/utils/datasource_config.py
"""
UnifiedRisk V12 - DataSourceConfig
数据源统一目录配置（cache / history / market）
"""

import os
from dataclasses import dataclass

from core.utils.logger import get_logger

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
        root = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..")
        )
        self.data_root = os.path.join(root, "data")
        self.market_root = os.path.join(self.data_root, self.market)

        # cache/{market}/{ds_name}/
        self.cache_root = os.path.join(self.market_root, "cache", self.ds_name)

        # history/{market}/{ds_name}/
        self.history_root = os.path.join(self.market_root, "history", self.ds_name)

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
