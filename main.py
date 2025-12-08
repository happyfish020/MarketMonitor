# main.py - UnifiedRisk V12 启动入口

import argparse
from datetime import datetime

from core.utils.logger import get_logger
from core.engines.cn.ashare_daily_engine import run_cn_ashare_daily
from core.reporters.cn.ashare_daily_reporter import save_daily_report

LOG = get_logger("Main")


def parse_args():
    parser = argparse.ArgumentParser(description="UnifiedRisk V12 Runner")

    parser.add_argument(
        "--market",
        type=str,
        default="cn",
        help="市场: cn / us / glo 等"
    )

    parser.add_argument(
        "--mode",
        type=str,
        default="ashare_daily",
        help="模式: ashare_daily"
    )

    # V12 新增刷新模式
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="刷新全量数据（所有 symbol）"
    )

    parser.add_argument(
        "--ss-refresh",
        action="store_true",
        help="刷新 snapshot 所需最小数据集"
    )

    return parser.parse_args()


def get_refresh_mode(args):
    if args.full_refresh:
        return "full"
    if args.ss_refresh:
        return "snapshot"
    return "readonly"


def main():
    args = parse_args()
    refresh_mode = get_refresh_mode(args)

    LOG.info("启动 UnifiedRisk V12 | market=%s mode=%s refresh=%s",
             args.market, args.mode, refresh_mode)

    if args.market == "cn" and args.mode == "ashare_daily":
        result = run_cn_ashare_daily(refresh_mode=refresh_mode)

        meta = result["meta"]
        trade_date = meta.get("trade_date")
        report_text = result["report_text"]

        # 保存报告
        report_path = save_daily_report("cn", trade_date, report_text)
        LOG.info("A股日度报告已保存: %s", report_path)
            # === 在控制台打印完整报告 ===
        print("\n" + "=" * 60)
        print(f"A股日度风险报告 {trade_date}（来自 {report_path} ）")
        print("=" * 60 + "\n")
        print(report_text)
        
        print("=" * 60 + "\n")
    else:
        LOG.error("不支持的参数: market=%s mode=%s", args.market, args.mode)


if __name__ == "__main__":
    main()
