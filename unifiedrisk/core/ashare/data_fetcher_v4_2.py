
import requests
from .em_parser import parse_em_jsonp

def fetch_em(report, start_date):
    url = "https://datacenter-web.eastmoney.com/web/api/data/v1/get"
    params = {
        "reportName": report,
        "columns": "ALL",
        "filter": f"(TRADE_DATE>='{start_date}')",
        "pageSize": "500", "pageNumber": "1",
    }
    r = requests.get(url, params=params, timeout=10)
    data = parse_em_jsonp(r.text)
    return data.get("result", {}).get("data", [])
