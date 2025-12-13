# core/adapters/datasources/providers/provider_router.py
# UnifiedRisk V12 - Provider Router
# ---------------------------------------------------------------
# 负责管理所有 Provider，并按 provider_label 返回对应实例
# ---------------------------------------------------------------

from __future__ import annotations
from typing import Dict
from core.utils.logger import get_logger

# 各 Provider 实现（目前先只注册 YFProvider，其它稍后补上）
from core.adapters.providers.provider_yf import YFProvider
from core.adapters.providers.provider_bs import BSProvider
from core.adapters.providers.provider_em import EMProvider
LOG = get_logger("Provider.Router")


class ProviderRouter:
    """
    V12 Provider 中央路由器。
    用法：
        provider = router.get_provider("yf")
        df = provider.fetch(symbol, method=...)
    """

    def __init__(self):
        LOG.info("[ProviderRouter] Initializing router...")

        # ---------------------------------------------------------------
        # Provider 注册表：
        # key = provider_label (来自 symbols.yaml)
        # value = Provider 实例
        # ---------------------------------------------------------------
        self.registry: Dict[str, object] = {}

        # 默认启用 YFProvider
        self.registry["yf"] = YFProvider()
        
        self.registry["bs"] = BSProvider()

        self.registry["em"] = EMProvider()
        # 未来可启用：
        # self.registry["bs"] = BSProvider()
        # self.registry["push2"] = ECProvider()
        # self.registry["ak"] = AKProvider()

        LOG.info(f"[ProviderRouter] Providers registered: {list(self.registry.keys())}")

    # ---------------------------------------------------------------
    # 获取 Provider 实例
    # ---------------------------------------------------------------
    def get_provider(self, provider_label: str):
        """
        根据 provider_label 返回 Provider 实例
        """
        if provider_label not in self.registry:
            LOG.error(
                f"[ProviderRouter] Provider '{provider_label}' is not registered. "
                f"Available: {list(self.registry.keys())}"
            )
            raise ValueError(f"Unknown provider: {provider_label}")

        return self.registry[provider_label]
