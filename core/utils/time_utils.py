from datetime import datetime, timedelta, timezone

BJ_TZ = timezone(timedelta(hours=8))


def now_bj() -> datetime:
    return datetime.now(BJ_TZ)

def is_trade_date_for_now (trade_date:str) -> bool:
    now_bj = datetime.now(BJ_TZ)
    format_string = "%Y-%m-%d"
    datetime_trade_date = datetime.strptime(trade_date, format_string)
    return now_bj.date() ==  datetime_trade_date
    

def is_intraday_trading_time(bj_now: datetime) -> bool:
    hour = bj_now.hour
    minute = bj_now.minute
    hm = hour * 100 + minute
    return (930 <= hm <= 1130) or (1300 <= hm <= 1500)



