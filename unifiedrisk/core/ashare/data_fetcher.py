# UnifiedRisk v4.0 data_fetcher.py
import requests

def fetch_chart(symbol: str):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=1d&interval=1d"
    try:
        r = requests.get(url, timeout=5)
        r.raise_for_status()
        js = r.json()
        res = js["chart"]["result"][0]
        closes = res["indicators"]["quote"][0].get("close", [])
        if len(closes) < 2:
            return {"symbol": symbol, "error": "insufficient data"}
        last, prev = closes[-1], closes[-2]
        if last and prev:
            pct = (last - prev) / prev * 100
            return {"symbol": symbol, "last": last, "prev": prev, "change_pct": round(pct, 3)}
    except Exception as e:
        return {"symbol": symbol, "error": str(e)}
    return {"symbol": symbol, "error": "unknown"}

def fetch_us_afterhours(symbol: str):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=1d&interval=5m&includePrePost=True"
    try:
        r = requests.get(url, timeout=5); r.raise_for_status()
        res = r.json()["chart"]["result"][0]
        closes = res["indicators"]["quote"][0].get("close", [])
        if len(closes) < 2:
            return None
        last, prev = closes[-1], closes[-2]
        pct = (last - prev) / prev * 100
        return {"symbol": symbol, "after_close": last, "regular_close": prev, "change_pct": round(pct, 3)}
    except Exception as e:
        return {"symbol": symbol, "error": str(e)}

def fetch_index_futures():
    futs = {}
    for sym in ["IF=F", "IH=F", "IC=F"]:
        q = fetch_chart(sym)
        futs[sym] = q.get("change_pct", 0)
    return futs

def fetch_a50_future():
    q = fetch_chart("CN1=F")
    return q.get("change_pct", 0)

def fetch_us_afterhours(symbol):
    url=f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=1d&interval=5m&includePrePost=True"
    try:
        r=requests.get(url,timeout=5); r.raise_for_status()
        js=r.json()['chart']['result'][0]
        closes=js['indicators']['quote'][0].get('close',[])
        if len(closes)<2: return None
        last, prev = closes[-1], closes[-2]
        if last and prev:
            pct=(last-prev)/prev*100
            return {'symbol':symbol,'after_close':last,'regular_close':prev,'change_pct':pct}
    except Exception as e:
        return {'symbol':symbol,'error':str(e)}
