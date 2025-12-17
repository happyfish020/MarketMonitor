import os
import yaml
from functools import lru_cache
from typing import Any, Dict

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
CONFIG_DIR = os.path.join(ROOT_DIR, "config")


def _load_yaml(name: str) -> Dict[str, Any]:
    path = os.path.join(CONFIG_DIR, name)
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


@lru_cache()
def load_paths() -> Dict[str, Any]:
    return _load_yaml("paths.yaml")


@lru_cache()
def load_symbols() -> Dict[str, Any]:
    return _load_yaml("symbols.yaml")


@lru_cache()
def load_weights() -> Dict[str, Any]:
    return _load_yaml("weights.yaml")


@lru_cache()
def load_config() -> Dict[str, Any]:
    """Load root config/config.yaml.

    Append-only addition for V12 Phase-2 runtime integration.
    """
    return _load_yaml("config.yaml")

def logs_path(*p):
    return os.path.join(ROOT_DIR, "logs", *p)

def reports_path(*p):
    return os.path.join(ROOT_DIR, "reports", *p)