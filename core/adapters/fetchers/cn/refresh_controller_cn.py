# core/adapters/fetchers/cn/refresh_controller_cn.py

"""
UnifiedRisk V12 - CN Refresh Controller
三种刷新模式：
  1) readonly  —— 仅读缓存
  2) snapshot  —— 刷新 snapshot 所需符号
  3) full      —— 刷新 symbols.yaml 中全部符号
"""

from core.utils.config_loader import load_symbols
from core.utils.logger import get_logger

LOG = get_logger("RefreshControllerCN")


class RefreshControllerCN:

    def __init__(self, refresh_mode: str = "readonly"):
        """
        refresh_mode ∈ {"readonly", "snapshot", "full"}
        """
        self.refresh_mode = refresh_mode
        self.symbols = load_symbols()

        LOG.info("RefreshControllerCN 初始化: mode=%s", refresh_mode)

    # ------------------------------------------------------------------
    # 基础属性
    # ------------------------------------------------------------------
    @property
    def refresh_flag(self) -> bool:
        """是否允许 refresh=True"""
        return self.refresh_mode in ("snapshot", "full")

    @property
    def is_snapshot(self) -> bool:
        return self.refresh_mode == "snapshot"

    @property
    def is_full(self) -> bool:
        return self.refresh_mode == "full"

    @property
    def is_readonly(self) -> bool:
        return self.refresh_mode in ("readonly", "none")

    # ------------------------------------------------------------------
    # Snapshot 需要刷新哪些符号（minimal set）
    # ------------------------------------------------------------------
    def list_snapshot_symbols(self):
        """
        Snapshot 模式刷新最小集合：
        - A股核心指数
        - 核心ETF
        - GlobalLead 全部
        """
        result = []

        # 1) index
        for sym in self.symbols.get("cn_index", {}).values():
            result.append(sym)

        # 2) core ETF
        for sym in self.symbols.get("cn_etf", {}).get("core", []):
            result.append(sym)

        # 3) global lead 全部
        glead = self.symbols.get("global_lead", {})
        for group, items in glead.items():
            for sym in items:
                result.append(sym)

        LOG.info("Snapshot 刷新符号数=%s", len(result))
        return result

    # ------------------------------------------------------------------
    # FULL 刷新所有符号（递归遍历 YAML）
    # ------------------------------------------------------------------
    def list_full_symbols(self):
        results = []

        def add(node):
            if isinstance(node, str):
                results.append(node)
            elif isinstance(node, list):
                for it in node:
                    add(it)
            elif isinstance(node, dict):
                for it in node.values():
                    add(it)

        add(self.symbols)
        LOG.info("FULL 刷新符号总数=%s", len(results))
        return results

    # ------------------------------------------------------------------
    # fetcher 使用 should_refresh(symbol)
    # ------------------------------------------------------------------
    def should_refresh(self, symbol: str) -> bool:
        """统一判断一个 symbol 是否应该刷新"""

        # READONLY —— 永远不刷
        if self.is_readonly:
            return False

        # SNAPSHOT —— 只刷 snapshot 集合
        if self.is_snapshot:
            return symbol in self.list_snapshot_symbols()

        # FULL —— 全部刷新
        if self.is_full:
            return True

        return False
