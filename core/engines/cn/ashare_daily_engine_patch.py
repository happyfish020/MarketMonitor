from core.utils.logger import log
from core.report.cn.ashare_report_cn import save_daily_report

class AshareDailyEngine:
    def run(self, snapshot, summary):
        market = "cn"
        trade_date = snapshot.get("trade_date", "unknown")
        report_text = f"Ashare Daily Report\nTrade Date: {trade_date}\nSummary: {summary}"
        path = save_daily_report(market, trade_date, report_text)
        log(f"[Engine] Report saved: {path}")
        return {"report": path}