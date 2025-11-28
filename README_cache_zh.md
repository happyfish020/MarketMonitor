
# UnifiedRisk v4.3.8 Cache 扩展包

本压缩包提供了一个**通用的缓存系统**，用于：

- 缓存 akshare 调用结果（例如 `stock_zh_a_spot()`）
- 缓存计算因子后的中间结果（可自行扩展）

## 目录结构

- `unifiedrisk/common/cache_manager.py`
- `unifiedrisk/common/ak_cache.py`
- `unifiedrisk/core/ashare/spot_cache_example.py`

## 一、CacheManager 用法

```python
from unifiedrisk.common.cache_manager import CacheManager
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
cache = CacheManager(base_dir=str(BASE_DIR))

data = cache.get("my_key")
if data is None:
    data = {"x": 1}
    cache.set("my_key", data)
```

缓存按**北京日期**自动分目录：

```text
cache/
  20251127/
    my_key.json
    spot_all.json
```

## 二、AkCache 用法（替换 akshare 大调用）

```python
from unifiedrisk.common.ak_cache import AkCache
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
ak_cache = AkCache(base_dir=str(BASE_DIR))

# 原来：
# df = ak.stock_zh_a_spot()

# 现在：
import pandas as pd
records = ak_cache.stock_zh_a_spot_cached()
df = pd.DataFrame(records)
```

你可以在 `DataFetcher` 中把所有 `ak.xxx` 替换为 `ak_cache.xxx_cached()`，实现**全局日级缓存**。

## 三、下一步建议

1. 先在 `data_fetcher.py` 里只替换一个最重的：
   - `stock_zh_a_spot()` → `AkCache.stock_zh_a_spot_cached()`
2. 确认运行正常、cache/2025XXXX/spot_all.json 已生成
3. 再逐步把：
   - 指数行情
   - 行业数据
   - 宏观数据（如果使用 akshare）
   接入缓存。

这样，你的 UnifiedRisk v4.3.8 在同一天内多次运行时，将**极大降低 akshare 调用次数**，减少超时与限流问题。
