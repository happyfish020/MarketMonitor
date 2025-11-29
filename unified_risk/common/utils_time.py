from datetime import datetime, timedelta, timezone

BJ_TZ = timezone(timedelta(hours=8))


def now_bj() -> datetime:
    return datetime.now(tz=BJ_TZ)
