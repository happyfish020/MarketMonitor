from pathlib import Path
from ..common.logger import get_logger
from ..common.time_utils import fmt_date_compact, now_bj
from ..common.cache_manager import CacheManager
from ..core.ashare.engine import AShareDailyEngine
from ..core.global_market.engine import GlobalDailyRiskEngine
from ..reports.report_writer import DailyReportWriter


LOG = get_logger("UnifiedRisk.CLI")


def main(argv=None):
    """
    UnifiedRisk CLI 主入口（v6.2f）

    逻辑：
    1. 自动获取今天北京时间对应的 YYYYMMDD
    2. 分别运行 A 股引擎 + Global 引擎
    3. 把结果交给 DailyReportWriter 生成报告
    """
    date_str = fmt_date_compact(now_bj())

    LOG.info(f"Running AShareDailyEngine for {date_str}")
    cache = CacheManager()

    ashare = AShareDailyEngine(cache).run(date_str)
    LOG.info(f"Running GlobalDailyRiskEngine for {date_str}")
    global_ = GlobalDailyRiskEngine(cache).run(date_str)

    # 无 base_dir 参数版本
    writer = DailyReportWriter()
    fpath = writer.write_daily_report(date_str, ashare, global_)

    LOG.info(f"报告生成完成：{fpath}")
