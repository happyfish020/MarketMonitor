from datetime import datetime, timedelta, timezone

BJ_TZ = timezone(timedelta(hours=8))


def now_bj() -> datetime:
    return datetime.now(BJ_TZ)


def is_intraday_trading_time(bj_now: datetime) -> bool:
    hour = bj_now.hour
    minute = bj_now.minute
    hm = hour * 100 + minute
    return (930 <= hm <= 1130) or (1300 <= hm <= 1500)
