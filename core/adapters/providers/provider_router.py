# core/adapters/providers/provider_router.py
# UnifiedRisk V12 - Provider Router
# ---------------------------------------------------------------
# 负责管理所有 Provider，并按 provider_label 返回对应实例
# ---------------------------------------------------------------

from __future__ import annotations

from typing import Dict

from core.utils.logger import get_logger

from core.adapters.providers.provider_yf import YFProvider
from core.adapters.providers.provider_bs import BSProvider
from core.adapters.providers.provider_em import EMProvider
from core.adapters.providers.db_provider_oracle import DBMarketProvider

LOG = get_logger("Provider.Router")


class ProviderRouter:
    """V12 Provider 中央路由器。

    用法：
        provider = router.get_provider("yf")
        df = provider.fetch(symbol, window=..., method=...)

    Providers:
        - yf: yfinance (fallback)
        - em: eastmoney (if enabled)
        - db: local DB-first with yf fallback
    """

    def __init__(self):
        LOG.info("[ProviderRouter] Initializing router...")

        self.registry: Dict[str, object] = {}

        # Primary market data providers
        self.registry["yf"] = YFProvider()
        self.registry["em"] = EMProvider()
        # self.registry["bs"] = BSProvider()

        # DB-first provider backed by Oracle local DB (falls back to yf)
        self.registry["db"] = DBMarketProvider()

        LOG.info(f"[ProviderRouter] Providers registered: {list(self.registry.keys())}")

    def get_provider(self, provider_label: str):
        """根据 provider_label 返回 Provider 实例"""
        provider_label = (provider_label or "").strip().lower()
        if provider_label not in self.registry:
            LOG.error(
                f"[ProviderRouter] Provider '{provider_label}' is not registered. "
                f"Available: {list(self.registry.keys())}"
            )
            raise ValueError(f"Unknown provider: {provider_label}")

        return self.registry[provider_label]
