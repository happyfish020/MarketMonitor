
from .data_fetcher import AShareDataFetcher
from .risk_scorer import RiskScorer

class AShareDailyEngine:
    def __init__(self):
        self.fetcher = AShareDataFetcher()
        self.scorer = RiskScorer()

    def run(self):
        raw = self.fetcher.fetch_ashare_daily_raw()
        score = self.scorer.score(raw)
        return {"raw": raw, "score": score}
