import logging
from datetime import datetime
from .config_manager import CONFIG

_LOGGERS = {}

def get_logger(name="UnifiedRisk"):
    if name in _LOGGERS:
        return _LOGGERS[name]

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    log_dir = CONFIG.get_path("log_dir")
    log_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    fh_path = log_dir / f"unified_risk_{ts}.log"

    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(name)s | %(message)s")

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    fh = logging.FileHandler(fh_path, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    _LOGGERS[name] = logger
    return logger
