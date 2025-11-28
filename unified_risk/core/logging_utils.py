import logging
import sys
from datetime import datetime
from pathlib import Path

from unified_risk.global_core.config import LOG_DIR

# 日志格式
LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logger(name: str = "GlobalMultiRisk", level=logging.INFO) -> logging.Logger:
    """
    三个输出目标：
      1) 控制台（StreamHandler）
      2) latest 文件（global_risk_latest.log）
      3) 每日文件（global_risk_YYYY-MM-DD.log）
    """

    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(level)

    # ------------------------------------------------
    # 1) 控制台输出
    # ------------------------------------------------
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
    logger.addHandler(console)

    # ------------------------------------------------
    # 2) latest 文件
    # ------------------------------------------------
    latest_path = LOG_DIR / "global_risk_latest.log"
    latest = logging.FileHandler(latest_path, mode="w", encoding="utf-8")
    latest.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
    logger.addHandler(latest)

    # ------------------------------------------------
    # 3) daily 文件（按日期）
    # ------------------------------------------------
    date_str = datetime.now().strftime("%Y-%m-%d")
    daily_path = LOG_DIR / f"global_risk_{date_str}.log"
    daily = logging.FileHandler(daily_path, mode="a", encoding="utf-8")
    daily.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
    logger.addHandler(daily)

    # 不向 root logger 传播
    logger.propagate = False

    return logger

# =============================
#   Compatibility Layer (v5.2)
# =============================

_default_logger = setup_logger("core")




def log_info(msg: str):
    _default_logger.info(msg)


def log_warn(msg: str):
    _default_logger.warning(msg)


def log_warning(msg: str):
    """
    Old alias used in early v5.2 code.
    """
    _default_logger.warning(msg)


def log_err(msg: str):
    _default_logger.error(msg)


def log_error(msg: str):
    _default_logger.error(msg)