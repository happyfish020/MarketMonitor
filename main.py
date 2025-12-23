# main.py - UnifiedRisk V12 启动入口

import argparse

from core.utils.logger import setup_logging, get_logger
from core.engines.cn.ashare_daily_engine import run_cn_ashare_daily


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
from datetime import datetime, timedelta
import pytz

def get_last_trading_day():
    # 获取北京时间
    tz = pytz.timezone('Asia/Shanghai')
    now = datetime.now(tz)
    
    # 如果在北京时间8:00前，取前一天作为参考日期
    if now.hour < 8:
        reference_date = (now - timedelta(days=1)).date()
    else:
        reference_date = now.date()
    
    print(f"当前北京时间: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"参考日期（调整后）: {reference_date}")
    
    # 处理周末：周六、周日取周五
    weekday = reference_date.weekday()  # 0=周一, 5=周六, 6=周日
    if weekday == 5:  # 周六
        reference_date -= timedelta(days=1)
    elif weekday == 6:  # 周日
        reference_date -= timedelta(days=2)
    
    print(f"周末调整后参考日期: {reference_date}")
    
    # 登陆baostock（匿名登陆，无需注册）
    lg = bs.login()
    if lg.error_code != '0':
        print('login fail! error_msg:', lg.error_msg)
        return None
    
    # 查询最近足够多的交易日历（从参考日期往前推60天，确保覆盖所有节假日）
    start_date = (reference_date - timedelta(days=60)).strftime('%Y-%m-%d')
    end_date = reference_date.strftime('%Y-%m-%d')
    
    rs = bs.query_trade_dates(start_date=start_date, end_date=end_date)
    if rs.error_code != '0':
        print('query_trade_dates fail! error_msg:', rs.error_msg)
        bs.logout()
        return None
    
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    
    trade_df = pd.DataFrame(data_list, columns=rs.fields)
    bs.logout()
    
    # 过滤交易日
    trade_df['calendar_date'] = pd.to_datetime(trade_df['calendar_date'])
    trading_days = trade_df[trade_df['is_trading_day'] == '1'].sort_values('calendar_date', ascending=False)
    
    # 从参考日期开始往前找第一个交易日
    candidate = reference_date
    while candidate >= trading_days['calendar_date'].min().date():
        if candidate in trading_days['calendar_date'].dt.date.values:
            return candidate.strftime('%Y-%m-%d')
        candidate -= timedelta(days=1)
    
    print("未找到交易日（异常情况）")
    return None



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
        trade_date = get_last_trading_day()
        assert trade_date, "定位最后交易日失败！"
        run_cn_ashare_daily(trade_date = trade_date, refresh_mode=refresh_mode)

    else:
        LOG.error("不支持的参数: market=%s mode=%s", args.market, args.mode)


if __name__ == "__main__":
    main()
