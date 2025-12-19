from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, List, Tuple, Any

import yaml
import logging

LOG = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class WatchlistCoverage:
    source: str
    groups: Dict[str, List[str]]  # category -> sectors


class WatchlistCoverageLoader:
    """
    读取 config/watchlist_sectors.yaml（Coverage 定义）
    - 只负责读取与校验（不做评估）
    """

    def __init__(self, *, project_root: str | None = None):
        self._project_root = project_root or os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")
        )

    def load(self) -> WatchlistCoverage:
        path = os.path.join(self._project_root, "config", "watchlist_sectors.yaml")
        if not os.path.exists(path):
            LOG.error("[WatchlistCoverageLoader] missing coverage file: %s", path)
            # Coverage 缺失属于配置错误：这里允许抛出（这是 Phase-2 配置必需项）
            raise FileNotFoundError(f"Missing coverage config: {path}")

        try:
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
        except Exception:
            LOG.exception("[WatchlistCoverageLoader] failed to read yaml: %s", path)
            raise

        groups = data.get("watchlist_sectors", {})
        if not isinstance(groups, dict):
            raise ValueError("watchlist_sectors must be a dict")

        cleaned: Dict[str, List[str]] = {}
        for cat, items in groups.items():
            if not isinstance(cat, str) or not cat.strip():
                continue
            if not isinstance(items, list):
                continue
            seen = set()
            out: List[str] = []
            for x in items:
                if not isinstance(x, str):
                    continue
                s = x.strip()
                if not s or s in seen:
                    continue
                seen.add(s)
                out.append(s)
            if out:
                cleaned[cat] = out

        if not cleaned:
            raise ValueError("watchlist_sectors is empty after cleaning")

        LOG.info("[WatchlistCoverageLoader] loaded coverage groups=%d from %s", len(cleaned), path)
        return WatchlistCoverage(source=path, groups=cleaned)
