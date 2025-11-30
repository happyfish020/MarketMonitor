from __future__ import annotations
from datetime import datetime, timedelta
import pytz


BJ_TZ = pytz.timezone("Asia/Shanghai")


def now_bj() -> datetime:
    """
    获取北京时间 datetime（tz-aware）。
    """
    return datetime.now(BJ_TZ)


def get_ashare_trade_date(bj_time: datetime | None = None) -> datetime.date:
    """
    统一获取 A 股交易日逻辑：

    - 盘前（0:00 - 09:30）
        → 用当天日期
    - 盘中（09:30 - 15:00）
        → 用当天日期
    - 盘后（15:00 - 23:59）
        → 用当天日期（因为日级数据已完整）
    - 美股盘中影响（无特殊处理）
    """

    if bj_time is None:
        bj_time = now_bj()

    hour = bj_time.hour
    minute = bj_time.minute

    # A 股交易区间
    if hour < 9 or (hour == 9 and minute < 30):
        # 盘前
        return bj_time.date()

    if 9 <= hour < 15:
        # 盘中
        return bj_time.date()

    # 盘后
    return bj_time.date()
