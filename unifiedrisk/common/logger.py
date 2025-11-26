"""
UnifiedRisk v1.9 - Unified Logger System
---------------------------------------
Features:
- Supports debug=True / False
- Avoids duplicated handlers
- Creates single log file per run
- Console + File dual output
- No cross-run caching (fresh logger each time)
"""

import logging
from datetime import datetime
from pathlib import Path


def get_logger(name: str, debug: bool = False):
    """
    Create a logger with optional debug mode.
    Always recreates handlers to avoid logger reuse complications.
    """

    logger = logging.getLogger(name)

    # Remove old handlers to avoid duplicates
    if logger.handlers:
        for h in list(logger.handlers):
            logger.removeHandler(h)

    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"UnifiedRisk_{timestamp}.log"
    logfile_path = log_dir / filename

    # Formatter
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        "%Y-%m-%d %H:%M:%S"
    )

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)
    console_handler.setLevel(logging.DEBUG if debug else logging.INFO)
    logger.addHandler(console_handler)

    # File handler
    file_handler = logging.FileHandler(logfile_path, encoding="utf-8")
    file_handler.setFormatter(fmt)
    file_handler.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)

    # Logger level
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    logger.propagate = False

    return logger
