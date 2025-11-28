
"""
FactorCache - 用于缓存“因子结果”的简单封装。

用途：
- 避免同一天内多次重复计算相同因子集（例如核心因子 / 风险因子 / T+1 因子）

示例（在 RiskScorer / Engine 中使用）：

    from pathlib import Path
    from unifiedrisk.common.factor_cache import FactorCache

    BASE_DIR = Path(__file__).resolve().parents[2]
    fcache = FactorCache(base_dir=str(BASE_DIR))

    core = fcache.get("factor_core")
    if core is None:
        core = self._compute_core_factors()
        fcache.set("factor_core", core)

    # 后续其它模块可直接读取：
    core2 = fcache.get("factor_core")
"""

from typing import Any, Optional
import os
from .cache_manager import CacheManager


class FactorCache:
    def __init__(self, base_dir: str):
        self.base_dir = os.path.abspath(base_dir)
        self.cache = CacheManager(self.base_dir, subdir="cache_factors")

    def get(self, key: str) -> Optional[Any]:
        return self.cache.get(key)

    def set(self, key: str, value: Any) -> None:
        self.cache.set(key, value)
