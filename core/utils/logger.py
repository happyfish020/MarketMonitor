# core/utils/logger.py

import logging
import sys
from datetime import datetime

# 统一的日志格式（V12）
LOG_FORMAT = "[%(asctime)s] [%(name)s] (%(levelname)s) %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_logger(module_name: str) -> logging.Logger:
    """
    UnifiedRisk V12 全局日志生成器
    - 时间戳
    - 模块名前缀
    - 等级格式统一
    """
    logger = logging.getLogger(module_name)

    if not logger.handlers:
        logger.setLevel(logging.INFO)

        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(LOG_FORMAT, DATE_FORMAT)
        handler.setFormatter(formatter)

        logger.addHandler(handler)

    return logger
