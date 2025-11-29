from datetime import datetime
from unified_risk.common.config_manager import CONFIG
from unified_risk.common.logger import get_logger
from unified_risk.engine.ashare_daily_engine import AshareDailyEngine

LOG = get_logger("UnifiedRisk.Main")

def main():
    mode = CONFIG.get("runtime","mode")
    d = CONFIG.get("runtime","default_date")
    if d=="auto":
        d = datetime.now().date()
    else:
        d = datetime.strptime(d,"%Y-%m-%d").date()

    if mode=="ashare_daily":
        e = AshareDailyEngine()
        res = e.run(d)
        LOG.info(f"[Main] Finished: score={res['total_risk_score']}")
    else:
        LOG.error(f"Unknown mode {mode}")

if __name__ == "__main__":
    main()
