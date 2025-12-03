from datetime import date, datetime, timedelta
from typing import Set

# 简化：只排除周末，节假日后续从外部配置注入
HOLIDAYS: Set[date] = set()


def is_trading_day(d: date) -> bool:
    """判断是否为交易日：当前版本 = 工作日且不在假日列表。"""
    if d.weekday() >= 5:
        return False
    if d in HOLIDAYS:
        return False
    return True


def get_last_trade_date(ref: datetime) -> date:
    """
    返回 ref 之前最近的一个“交易日”（含当天）。
    （这个函数现在主要留给其他用途，日级入口改用 get_trade_date_daily）
    """
    d = ref.date()
    while not is_trading_day(d):
        d = d - timedelta(days=1)
    return d


def get_trade_date_daily(bj_now: datetime) -> date:
    """
    日级 trade_date 规则（方案 B）：

    - 北京时间 < 15:00 → 使用上一个交易日 (T-1)
    - 北京时间 ≥ 15:00 → 如果今天是交易日，用今天；否则用上一个交易日

    始终保证 trade_date 不会是未来日期。
    """
    today = bj_now.date()
    hour = bj_now.hour

    # 找到 <= today 的最近一个交易日
    last_trade = today
    while not is_trading_day(last_trade):
        last_trade = last_trade - timedelta(days=1)

    if hour < 15:
        # 盘前/盘中：使用 T-1
        prev_trade = last_trade - timedelta(days=1)
        while not is_trading_day(prev_trade):
            prev_trade = prev_trade - timedelta(days=1)
        return prev_trade

    # 15:00 之后：如果今天本身是交易日，就用今天；否则用最近一个交易日
    if is_trading_day(today):
        return today
    return last_trade
