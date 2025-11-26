
import logging
import sys
from datetime import datetime
from .paths import get_logs_dir

def setup_logger(level=logging.INFO) -> None:
    logger = logging.getLogger()
    logger.setLevel(level)

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    # Console handler
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(fmt)
        logger.addHandler(ch)

    # File handler
    logs_dir = get_logs_dir()
    date_str = datetime.now().strftime("%Y%m%d")
    log_file = logs_dir / f"{date_str}_UnifiedRisk.log"
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
