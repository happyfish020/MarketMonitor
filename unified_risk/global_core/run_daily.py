
from __future__ import annotations

from .utils.logging_utils import setup_logger
from .api import get_daily_global_risk

logger = setup_logger(__name__)


def main():
    data = get_daily_global_risk(as_dict=False)
    logger.info("GlobalRisk daily snapshot: %s", data)


if __name__ == "__main__":
    main()
