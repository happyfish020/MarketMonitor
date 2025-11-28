
"""
示例：如何在 DataFetcher 中接入 AkCache

注意：这是示例文件，不会被自动调用。
你需要在自己的 unifiedrisk/core/ashare/data_fetcher.py 里做类似修改：

    from unifiedrisk.common.ak_cache import AkCache
    BASE_DIR = Path(__file__).resolve().parents[3]
    ak_cache = AkCache(base_dir=BASE_DIR)

    class DataFetcher:
        def get_spot(self):
            records = ak_cache.stock_zh_a_spot_cached()
            return pd.DataFrame(records)

"""

from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from unifiedrisk.common.ak_cache import AkCache

BASE_DIR = Path(__file__).resolve().parents[3]
ak_cache = AkCache(base_dir=str(BASE_DIR))


def get_spot_df() -> pd.DataFrame:
    """
    示例方法：返回带缓存的全市场快照 DataFrame
    """
    records: List[Dict[str, Any]] = ak_cache.stock_zh_a_spot_cached()
    if not records:
        return pd.DataFrame()
    return pd.DataFrame(records)
