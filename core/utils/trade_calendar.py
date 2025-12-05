from datetime import date, datetime, timedelta
from typing import Set

HOLIDAYS: Set[date] = set()


def is_trading_day(d: date) -> bool:
    if d.weekday() >= 5:
        return False
    if d in HOLIDAYS:
        return False
    return True


def get_last_trade_date(ref: datetime) -> date:
    """
    返回 ref 之前最近的一个“交易日”（含当天）。
    """
    d = ref.date()
    while not is_trading_day(d):
        d = d - timedelta(days=1)
    return d


def _prev_trading_day(d: date) -> date:
    """严格意义上的 T-1：返回 d 之前的最近一个交易日（不含当天）。"""
    cur = d - timedelta(days=1)
    while not is_trading_day(cur):
        cur = cur - timedelta(days=1)
    return cur


def get_trade_date_daily(bj_now: datetime) -> date:
    """
    日级 trade_date 规则（修正版）：

    - 如果 today 是交易日：
        * bj_now.hour < 15:00 → 使用上一交易日 (T-1)
        * bj_now.hour >= 15:00 → 使用今天 (T)
    - 如果 today 不是交易日（周末/节假日）：
        * 始终使用最近一个过去的交易日（例如周六/周日 → 周五）
    """
    today = bj_now.date()
    hour = bj_now.hour

    if is_trading_day(today):
        # 交易日：区分盘前/盘中 vs 收盘后
        if hour < 15:
            # 盘前/盘中：看 T-1
            return _prev_trading_day(today)
        else:
            # 收盘后：今天就可以作为 trade_date
            return today
    else:
        # 非交易日：一律用最近一个过去的交易日
        return get_last_trade_date(bj_now)
