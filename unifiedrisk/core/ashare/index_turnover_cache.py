# index_turnover_cache.py
import os, json
from datetime import datetime, time
from pytz import timezone
BJ_TZ = timezone("Asia/Shanghai")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CACHE_DIR = os.path.join(BASE_DIR, "data", "cache", "index_turnover")
os.makedirs(CACHE_DIR, exist_ok=True)

def is_trading_time(now_bj: datetime) -> bool:
    t = now_bj.time()
    return ((time(9,30)<=t<=time(11,30)) or (time(13,0)<=t<=time(15,0)))

def cache_path(date_str): return os.path.join(CACHE_DIR, f"{date_str}.json")

def write_turnover_cache(date_str, data):
    with open(cache_path(date_str),"w",encoding="utf-8") as f:
        json.dump(data,f,ensure_ascii=False,indent=2)

def load_turnover_cache(date_str):
    p=cache_path(date_str)
    if os.path.exists(p):
        with open(p,"r",encoding="utf-8") as f: return json.load(f)
    return None

def load_latest_cache():
    try:
        files=sorted(os.listdir(CACHE_DIR), reverse=True)
        for f in files:
            if f.endswith(".json"):
                with open(os.path.join(CACHE_DIR,f),"r",encoding="utf-8") as fh:
                    return json.load(fh)
    except: pass
    return None
