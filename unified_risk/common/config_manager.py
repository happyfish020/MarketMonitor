from pathlib import Path
import yaml

class ConfigManager:
    def __init__(self):
        self.project_root = Path(__file__).resolve().parents[2]
        self.config_path = self.project_root / "config" / "config.yaml"
        self._config = yaml.safe_load(self.config_path.read_text(encoding="utf-8"))

    def get(self, *keys, default=None):
        cur = self._config
        for k in keys:
            if not isinstance(cur, dict):
                return default
            cur = cur.get(k)
            if cur is None:
                return default
        return cur

    def get_path(self, key: str):
        rel = self.get("paths", key)
        return (self.project_root / rel).resolve()

CONFIG = ConfigManager()
