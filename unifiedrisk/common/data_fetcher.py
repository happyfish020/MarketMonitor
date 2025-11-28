# UnifiedRisk v4.3.8 data_fetcher.py
import requests


def fetch_chart(symbol: str):
    """
    标准化获取 Yahoo Finance v8 chart 数据
    返回结构：
    {
        "last": float,
        "prev": float,
        "change_pct": float
    }
    """

    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        "?range=1d&interval=1d"
    )

    try:
        r = requests.get(url, timeout=5)
        r.raise_for_status()
        js = r.json()

        result = js["chart"]["result"][0]
        quotes = result["indicators"]["quote"][0]
        closes = quotes.get("close", [])

        if len(closes) < 2:
            return {"symbol": symbol, "error": "insufficient data"}

        last = closes[-1]
        prev = closes[-2]

        if last and prev:
            change_pct = (last - prev) / prev * 100
            return {
                "symbol": symbol,
                "last": last,
                "prev": prev,
                "change_pct": round(change_pct, 3),
            }

    except Exception as e:
        return {"symbol": symbol, "error": str(e)}

    return {"symbol": symbol, "error": "unknown"}
 
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