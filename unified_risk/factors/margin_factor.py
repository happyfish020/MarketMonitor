from datetime import date
from unified_risk.core.cache.day_cache import DayCache
from unified_risk.common.config_manager import CONFIG

class MarginFactor:
    def __init__(self):
        self.cache = DayCache(CONFIG.get_path("cache_dir") / "margin")

    def as_factor_dict(self, d: date):
        return {"margin_score": 0}
