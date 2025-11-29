import yaml
from pathlib import Path
from functools import lru_cache

# 正确推断项目根目录：unified_risk 的父目录
# unified_risk/common/config_loader.py → parents[2] = MarketMonitor
AUTO_ROOT = Path(__file__).resolve().parents[2]


@lru_cache(maxsize=1)
def _load_config() -> dict:
    """
    只加载一次 settings.yaml，存入缓存。
    """
    config_dir = AUTO_ROOT / "config"
    config_path = config_dir / "settings.yaml"

    if not config_path.exists():
        raise FileNotFoundError(f"UnifiedRisk config file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


@lru_cache(maxsize=1)
def get_project_root() -> Path:
    """
    返回项目根目录 Path。
    """
    cfg = _load_config()
    pr = cfg.get("project_root")
    if isinstance(pr, str) and pr.strip():
        return Path(pr).resolve()
    return AUTO_ROOT


def _expand_project_root(raw: str) -> str:
    pr = str(get_project_root())
    return raw.replace("{project_root}", pr)


def get_path(key: str) -> Path:
    cfg = _load_config()
    raw = cfg["paths"][key]  # 必定存在
    return Path(_expand_project_root(raw)).resolve()


def get_etf_wide() -> list:
    return _load_config().get("etf", {}).get("wide", [])


def get_etf_sector() -> list:
    return _load_config().get("etf", {}).get("sector", [])


def get_risk_weights() -> dict:
    return _load_config().get("risk_weights", {})
