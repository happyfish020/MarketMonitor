import logging
import sys
from datetime import datetime
from pathlib import Path

from .config_loader import get_path

# 从配置获取日志目录
LOG_DIR: Path = get_path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)


def get_logger(name: str, level=logging.INFO, debug: bool = False) -> logging.Logger:
    """
    统一 logger 工厂：
    - 日志文件：logs/unified_risk_{yyyy-MM-dd-HHmmss}.log
    - 控制台：同步输出
    - 不使用任何硬编码路径
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG if debug else level)
    logger.propagate = False

    if not logger.handlers:
        ts = datetime.now().strftime("%Y-%m-%d-%H%M%S")
        logfile = LOG_DIR / f"unified_risk_{ts}.log"

        fh = logging.FileHandler(logfile, encoding="utf-8")
        sh = logging.StreamHandler(sys.stdout)

        fmt = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
        formatter = logging.Formatter(fmt)

        fh.setFormatter(formatter)
        sh.setFormatter(formatter)

        logger.addHandler(fh)
        logger.addHandler(sh)

    return logger
