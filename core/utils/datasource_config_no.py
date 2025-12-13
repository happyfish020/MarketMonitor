# core/utils/datasource_config.py
# ============================================================
# UnifiedRisk V12.3
# 统一数据源路径配置（cache / history / snapshot）
# 全局唯一真源：所有 DataSource 必须依赖此类生成路径
# ============================================================

import os
from functools import lru_cache
from typing import Dict

from core.adapters.cache.symbol_cache import normalize_symbol


# ============================================================
# 读取 config/paths.yaml
# ============================================================

@lru_cache()
def load_paths() -> Dict[str, str]:
    """
    统一加载 paths.yaml
    YAML 示例：
        data_root: "data/"
        cache_dir: "data/cache/"
    """
    import yaml

    # 项目根目录：MarketMonitor/
    root_dir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..")
    )

    cfg_path = os.path.join(root_dir, "config", "paths.yaml")

    if not os.path.exists(cfg_path):
        raise FileNotFoundError(f"[DataSourceConfig] paths.yaml 不存在: {cfg_path}")

    with open(cfg_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    return data


# ============================================================
# DataSourceConfig —— 全系统统一路径入口
# ============================================================

class DataSourceConfig:
    """
    UnifiedRisk V12 路径规范：
    ------------------------------------
    data_root/
       {market}/
          cache/{ds_name}/
          history/{ds_name}/

    ✔ 所有路径从 paths.yaml 获取
    ✔ 路径必须是绝对路径
    ✔ 所有符号必须 normalize
    ✔ cache 与 history 必须分层存放
    """

    def __init__(self, market: str, ds_name: str):
        self.market = market
        self.ds_name = ds_name

        paths_cfg = load_paths()

        # 从 YAML 获取 data_root，如 data/
        yaml_data_root = paths_cfg.get("data_root", "data")

        # 解析为绝对路径（如 D:/MarketMonitor/data）
        self.data_root = os.path.abspath(yaml_data_root)

        # 预构建市场级路径
        self.market_root = os.path.join(self.data_root, self.market)

        # cache/history 根目录（不含 symbol）
        self.cache_root = os.path.join(self.market_root, "cache", self.ds_name)
        self.history_root = os.path.join(self.market_root, "history", self.ds_name)

        # 创建必要目录
        self.ensure_dirs()

    # ------------------------------------------------------------
    # 目录构建
    # ------------------------------------------------------------

    def cache_dir(self) -> str:
        """
        data/{market}/cache/{ds_name}/
        """
        return self.cache_root

    def history_dir(self) -> str:
        """
        data/{market}/history/{ds_name}/
        """
        return self.history_root

    # ------------------------------------------------------------
    # 文件路径构建（cache）
    # ------------------------------------------------------------

    def cache_file(self, symbol: str, date_str: str, ext: str = "json") -> str:
        """
        生成 cache 文件：
            data/{market}/cache/{ds_name}/{symbol}/{date_str}.json
        """
        symbol = normalize_symbol(symbol)

        return os.path.join(
            self.cache_root,
            symbol,
            f"{date_str}.{ext}"
        )

    # ------------------------------------------------------------
    # 文件路径构建（history）
    # ------------------------------------------------------------

    def history_file(self, symbol: str, ext: str = "json") -> str:
        """
        生成 history 文件：
            data/{market}/history/{ds_name}/{symbol}.json
        """
        symbol = normalize_symbol(symbol)

        return os.path.join(
            self.history_root,
            f"{symbol}.{ext}"
        )

    # ------------------------------------------------------------
    # ensure_dir 单一路径
    # ------------------------------------------------------------

    @staticmethod
    def ensure_dir(path: str):
        """
        创建目录（如果不存在）。
        """
        os.makedirs(path, exist_ok=True)

    # ------------------------------------------------------------
    # ensure_dirs —— 创建整个 DS 目录树（V12 专用）
    # ------------------------------------------------------------

    def ensure_dirs(self):
        """
        统一目录创建：
            data_root/
                {market}/
                    cache/{ds_name}/
                    history/{ds_name}/
        """
        # 根 data/
        self.ensure_dir(self.data_root)

        # 市场目录 data/{market}/
        self.ensure_dir(self.market_root)

        # cache/
        self.ensure_dir(self.cache_root)

        # history/
        self.ensure_dir(self.history_root)

        return self.cache_root, self.history_root


# ============================================================
# END OF FILE
# ============================================================
