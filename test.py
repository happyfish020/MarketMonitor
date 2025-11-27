
import requests
from typing import Dict, Any, Optional
import time

def _fetch_eastmoney_data( secid: str, fields: str) -> Optional[Dict]:
    """东方财富通用数据获取函数，带进程内缓存"""
    

    url = f"https://push2.eastmoney.com/api/qt/stock/get?secid={secid}&fields={fields}&_={int(time.time() * 1000)}"
    try:
        response = requests.get(url, timeout=5)
        
        result = response.json()
        if result.get('rc') != 0 or not result.get('data'):
            print( "API Error/No Data")
            return None
        data = result.get('data')

        #print(result)
        #{'rc': 0, 'rt': 4, 'svr': 183636151, 'lt': 1, 'full': 1, 'dlmkts': '', 'data': {'f43': 386418, 'f47': 0, 'f60': 386418}}
        # 写入缓存
        
        return data
    except Exception:
        print("error")
        return None

        return result.get('data')
    except Exception:
        print("error")
        
        return None
	 
_fetch_eastmoney_data("1.000001", "f43,f47,f60")

