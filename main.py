# main.py - UnifiedRisk V12 启动入口

import argparse

from core.utils.logger import setup_logging, get_logger
from core.engines.cn.ashare_daily_engine import AShareDailyEngine


def parse_args():
    parser = argparse.ArgumentParser(description="UnifiedRisk V12 Runner")

    parser.add_argument("--market", type=str, default="cn")
    parser.add_argument("--mode", type=str, default="ashare_daily")
    parser.add_argument("--full-refresh", action="store_true")
    parser.add_argument("--ss-refresh", action="store_true")

    return parser.parse_args()


def get_refresh_mode(args):
    if args.full_refresh:
        return "full"
    if args.ss_refresh:
        return "snapshot"
    return "readonly"


import baostock as bs
import pandas as pd
import pytz
from datetime import datetime, timedelta, time
A_SHARE_OPEN = time(9, 30)   # 开盘时间
A_SHARE_CLOSE = time(15, 0)  # 收盘时间


def get_intraday_status_and_last_trade_date() -> tuple[bool, str]:
    """
    判断当前是否为交易日盘中时间，并返回最近一个交易日

    Returns:
        (is_intraday: bool, last_trade_date: str)
            - is_intraday: True 表示当前是交易日且在 9:30~15:00 之间
            - last_trade_date: 最近的交易日（'YYYY-MM-DD'）
    """
    tz = pytz.timezone('Asia/Shanghai')
    now = datetime.now(tz)

    # 1. 确定参考日期（盘前8点前算前一天）
    if now.hour < 8:
        reference_date = (now - timedelta(days=1)).date()
    else:
        reference_date = now.date()

    # 2. 登录 baostock
    lg = bs.login()
    if lg.error_code != '0':
        print(f"baostock login failed: {lg.error_msg}")
        # 降级处理：仅判断周末 + 时间段（不考虑节假日）
        is_weekend = reference_date.weekday() >= 5
        in_session = A_SHARE_OPEN <= now.time() < A_SHARE_CLOSE
        fallback_last_date = reference_date - timedelta(days=(reference_date.weekday() + 2) % 7 if is_weekend else 0)
        return (not is_weekend and in_session, fallback_last_date.strftime('%Y-%m-%d'))

    try:
        # 3. 查询最近60天的交易日历（足够覆盖节假日）
        start_date = (reference_date - timedelta(days=60)).strftime('%Y-%m-%d')
        end_date = reference_date.strftime('%Y-%m-%d')

        rs = bs.query_trade_dates(start_date=start_date, end_date=end_date)
        if rs.error_code != '0':
            raise Exception(f"query_trade_dates failed: {rs.error_msg}")

        data_list = []
        while rs.next():
            data_list.append(rs.get_row_data())

        trade_df = pd.DataFrame(data_list, columns=rs.fields)
        trade_df['calendar_date'] = pd.to_datetime(trade_df['calendar_date'])
        trade_df['is_trading_day'] = trade_df['is_trading_day'].astype(int)

        # 提取所有交易日并降序排列
        trading_days = trade_df[trade_df['is_trading_day'] == 1]['calendar_date'].dt.date.values

        if len(trading_days) == 0:
            raise Exception("No trading days found in the past 60 days")

        # 4. 找到最近的交易日（从 reference_date 往前找）
        last_trade_date_obj = max(d for d in trading_days if d <= reference_date)
        last_trade_date_str = last_trade_date_obj.strftime('%Y-%m-%d')

        # 5. 判断是否为盘中时间
        # 只有当今天就是交易日，且当前时间在开盘后收盘前，才算盘中
        is_today_trading_day = last_trade_date_obj == now.date()
        in_trading_hours = A_SHARE_OPEN <= now.time() < A_SHARE_CLOSE
        is_intraday = is_today_trading_day and in_trading_hours

        return is_intraday, last_trade_date_str

    except Exception as e:
        print(f"Error in get_intraday_status_and_last_trade_date: {e}")
        # 降级：仅判断周末和时间段
        is_weekend = reference_date.weekday() >= 5
        in_session = A_SHARE_OPEN <= now.time() < A_SHARE_CLOSE
        fallback_last = reference_date - timedelta(days=(reference_date.weekday() - 4) % 7)
        return (not is_weekend and in_session, fallback_last.strftime('%Y-%m-%d'))

    finally:
        bs.logout()


def main():
    args = parse_args()
    refresh_mode = get_refresh_mode(args)

    # 初始化日志（入口层唯一职责之一）
    setup_logging(market=args.market, mode=args.mode)
    LOG = get_logger("Main")

    LOG.info(
        "启动 UnifiedRisk V12 | market=%s mode=%s refresh=%s",
        args.market,
        args.mode,
        refresh_mode,
    )

    if args.market == "cn" and args.mode == "ashare_daily":
        # ✅ V12：只调用，不接收，不解析
        is_intraday, trade_date = get_intraday_status_and_last_trade_date()
        assert trade_date, "定位最后交易日失败！"
        daily_engine = AShareDailyEngine(refresh_mode=refresh_mode)
        daily_engine.run( )

    else:
        LOG.error("不支持的参数: market=%s mode=%s", args.market, args.mode)


    
if __name__ == "__main__":
    main()
