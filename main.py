from datetime import datetime
from unified_risk.common.config_manager import CONFIG
from unified_risk.common.logger import get_logger
from unified_risk.core.ashare.ashare_daily_engine import AShareDailyEngine

LOG = get_logger("UnifiedRisk.Main")

def main():
    # d 是 datetime.date 或 datetime.datetime 都可以
    d = None  # 或者传 date，比如 date.today()
    e = AShareDailyEngine()
    res = e.run(d)
    LOG.info(f"[Main] Finished: score={res['total_risk_score']}")
    
if __name__ == "__main__":
    main()
