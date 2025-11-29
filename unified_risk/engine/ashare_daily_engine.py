from datetime import date, datetime
from unified_risk.common.config_manager import CONFIG
from unified_risk.common.logger import get_logger

from unified_risk.factors.northbound_factor import NorthboundFactor
from unified_risk.factors.turnover_factor import TurnoverFactor
from unified_risk.factors.margin_factor import MarginFactor
from unified_risk.factors.global_factor import GlobalFactor
from unified_risk.scorer.risk_scorer_daily import DailyRiskScorer
from unified_risk.writer.daily_report_writer import DailyReportWriter

LOG = get_logger("UnifiedRisk.Engine.AshareDaily")

class AshareDailyEngine:
    def __init__(self):
        self.nb = NorthboundFactor()
        self.to = TurnoverFactor()
        self.mg = MarginFactor()
        self.gl = GlobalFactor()
        self.scorer = DailyRiskScorer()
        self.writer = DailyReportWriter(CONFIG.get_path("report_dir"))

    def run(self, d: date):
        f = {}
        f.update(self.nb.as_factor_dict(d))
        f.update(self.to.as_factor_dict(d))
        f.update(self.mg.as_factor_dict(d))
        f.update(self.gl.as_factor_dict(d))

        result = self.scorer.score(d, f)
        self.writer.write(result)
        return result
