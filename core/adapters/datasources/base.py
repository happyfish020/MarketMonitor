# core/adapters/datasources/base.py

"""
UnifiedRisk V12
DataSource Base Class（底层统一接口）
"""

from typing import Optional, Any, Dict

from core.utils.logger import get_logger

LOG = get_logger("DS.Base")


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
