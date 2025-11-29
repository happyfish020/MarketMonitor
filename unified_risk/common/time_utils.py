from datetime import datetime, timezone, timedelta

BJ_TZ = timezone(timedelta(hours=8))

def now_bj() -> datetime:
    """返回当前北京时间。""" 
    return datetime.now(BJ_TZ)

def fmt_date(d: datetime) -> str:
    return d.strftime("%Y-%m-%d")

def fmt_date_compact(d: datetime) -> str:
    return d.strftime("%Y%m%d")
