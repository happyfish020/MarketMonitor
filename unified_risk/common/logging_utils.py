from __future__ import annotations

import sys
import logging
from pathlib import Path
from datetime import datetime

from unified_risk.common.config_loader import get_path, SETTINGS


def _create_run_logger(name: str = "UnifiedRisk") -> logging.Logger:
    """
    每次程序运行时都产生一个新的 log 文件。
    文件名格式：unified_risk_YYYY-MM-dd-HHmmss.log
    """

    log_dir: Path = get_path("log_dir", "logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    # === 创建时间戳格式文件 ===
    ts = datetime.now().strftime("%Y-%m-%d-%H%M%S")
    base_name = f"unified_risk_{ts}.log"
    log_file = log_dir / base_name

    level_name = SETTINGS.get("logging", {}).get("level", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # ⚠️ 防止 Handler 重复叠加
    logger.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )

    # File handler（写入文件）
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    fh.setLevel(level)

    # Console handler（终端输出）
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    ch.setLevel(level)

    logger.addHandler(fh)
    logger.addHandler(ch)

    logger.info(f"[Logger] Initialized new log file: {log_file}")

    return logger


# === 全局 logger 实例 ===
LOG = _create_run_logger()


def log_info(msg: str):
    LOG.info(msg)


def log_warning(msg: str):
    LOG.warning(msg)


def log_error(msg: str):
    LOG.error(msg)




def get_logger(name: str, level=logging.INFO) -> logging.Logger:
    """
    UnifiedRisk 统一 Logger 工厂函数
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(level)

        # 控制台输出
        ch = logging.StreamHandler()
        ch.setLevel(level)

        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    return logger


# 兼容旧版本（避免 import 错误）
log_info = lambda msg: get_logger("UnifiedRisk").info(msg)
log_warning = lambda msg: get_logger("UnifiedRisk").warning(msg)
log_error = lambda msg: get_logger("UnifiedRisk").error(msg)
