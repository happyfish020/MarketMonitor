from __future__ import annotations

from pathlib import Path
import yaml
from functools import lru_cache


def _find_project_root() -> Path:
    p = Path(__file__).resolve()
    for parent in p.parents:
        if (parent / "config" / "settings.yaml").exists():
            return parent
    return p.parents[2]  # fallback


ROOT_DIR = _find_project_root()
CONFIG_DIR = ROOT_DIR / "config"


@lru_cache(maxsize=1)
def load_settings() -> dict:
    cfg_file = CONFIG_DIR / "settings.yaml"
    if not cfg_file.exists():
        raise FileNotFoundError(f"settings.yaml not found at {cfg_file}")
    with cfg_file.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


SETTINGS = load_settings()
PATHS = SETTINGS.get("paths", {})
CACHE_CFG = SETTINGS.get("cache", {})
RUNTIME = SETTINGS.get("runtime", {})
FACTORS = SETTINGS.get("factors", {})
WEIGHTS = SETTINGS.get("weights", {})


def get_path(name: str, default: str | None = None) -> Path:
    rel = PATHS.get(name, default)
    if rel is None:
        raise KeyError(f"paths.{name} not defined in settings.yaml")
    return (ROOT_DIR / rel).resolve()
