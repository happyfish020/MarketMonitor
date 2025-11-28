from datetime import datetime
from zoneinfo import ZoneInfo

BJ_TZ = ZoneInfo("Asia/Shanghai")
NY_TZ = ZoneInfo("America/New_York")

def now_bj() -> datetime:
    return datetime.now(BJ_TZ)

def now_ny() -> datetime:
    return datetime.now(NY_TZ)
