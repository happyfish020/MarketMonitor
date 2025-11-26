
import logging
from unifiedrisk.core.ashare.engine import AShareDailyEngine

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')

if __name__ == "__main__":
    eng = AShareDailyEngine()
    res = eng.run()
    print("\n=== UnifiedRisk v2.1 Output ===")
    print(res)
