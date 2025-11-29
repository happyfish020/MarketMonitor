import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_ROOT = Path("logs")
LOG_ROOT.mkdir(parents=True, exist_ok=True)


def _module_log_file(name: str) -> Path:
    if "Engine.AShareDaily" in name:
        return LOG_ROOT / "ashare_daily.log"
    if "Fetcher.AShare" in name:
        return LOG_ROOT / "data_fetcher.log"
    if "Factor.Northbound" in name:
        return LOG_ROOT / "northbound.log"
    if "YF.ETF" in name:
        return LOG_ROOT / "yf_fetcher.log"
    if "Writer.AShareDaily" in name:
        return LOG_ROOT / "ashare_report.log"
    return LOG_ROOT / "unified_risk_module.log"

GLOBAL_LOG_PATH = LOG_ROOT / "unified_risk.log"

_configured = {}


def get_logger(name: str, level: int | None = None) -> logging.Logger:
    if name in _configured:
        return _configured[name]

    logger = logging.getLogger(name)
    logger.setLevel(level or logging.INFO)

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    module_fh = RotatingFileHandler(
        _module_log_file(name),
        maxBytes=3_000_000,
        backupCount=3,
        encoding="utf-8",
    )
    module_fh.setFormatter(fmt)
    logger.addHandler(module_fh)

    global_fh = RotatingFileHandler(
        GLOBAL_LOG_PATH,
        maxBytes=5_000_000,
        backupCount=3,
        encoding="utf-8",
    )
    global_fh.setFormatter(fmt)
    logger.addHandler(global_fh)

    _configured[name] = logger
    return logger
