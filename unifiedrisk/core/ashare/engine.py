
from .data_fetcher import AShareDataFetcher
#from .risk_scorer import RiskScorer
from unifiedrisk.core.ashare.risk_scorer import score_ashare_risk

class AShareDailyEngine:
    def __init__(self):
        self.fetcher = AShareDataFetcher()
        #self.scorer = RiskScorer()

    def run(self):
        raw = self.fetcher.fetch_ashare_daily_raw()
        #score = self.scorer.score(raw)
        score = score_ashare_risk(raw)
        return {"raw": raw, "score": score}
