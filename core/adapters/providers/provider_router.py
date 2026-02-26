# core/adapters/providers/provider_router.py
# UnifiedRisk V12 - Provider Router
# ---------------------------------------------------------------
# 璐熻矗绠＄悊鎵€鏈?Provider锛屽苟鎸?provider_label 杩斿洖瀵瑰簲瀹炰緥
# ---------------------------------------------------------------

from __future__ import annotations

from typing import Dict

from core.utils.logger import get_logger

from core.adapters.providers.provider_bs import BSProvider
from core.adapters.providers.provider_em import EMProvider
from core.adapters.providers.db_provider_mysql_market import DBMarketProvider

LOG = get_logger("Provider.Router")


class ProviderRouter:
    """V12 Provider 涓ぎ璺敱鍣ㄣ€?

    鐢ㄦ硶锛?
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
        # yfinance is optional in offline/air-gapped environments.
        try:
            from core.adapters.providers.provider_yf import YFProvider  # local import to avoid hard startup dependency
            self.registry["yf"] = YFProvider()
        except Exception as e:
            LOG.warning("[ProviderRouter] 'yf' provider unavailable: %s", e)

        self.registry["em"] = EMProvider()
        # self.registry["bs"] = BSProvider()

        # DB-first provider backed by MySQL local DB (falls back to yf)
        self.registry["db"] = DBMarketProvider()

        LOG.info(f"[ProviderRouter] Providers registered: {list(self.registry.keys())}")

    def get_provider(self, provider_label: str):
        """鏍规嵁 provider_label 杩斿洖 Provider 瀹炰緥"""
        provider_label = (provider_label or "").strip().lower()
        if provider_label not in self.registry:
            LOG.error(
                f"[ProviderRouter] Provider '{provider_label}' is not registered. "
                f"Available: {list(self.registry.keys())}"
            )
            raise ValueError(f"Unknown provider: {provider_label}")

        return self.registry[provider_label]

