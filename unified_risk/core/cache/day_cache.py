import json
from pathlib import Path
from datetime import date

class DayCache:
    def __init__(self, base_dir: Path):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _file(self, d: date):
        return self.base_dir / f"{d.strftime('%Y%m%d')}.json"

    def load(self, d: date):
        f = self._file(d)
        if not f.exists():
            return {}
        try:
            return json.loads(f.read_text(encoding="utf-8"))
        except:
            return {}

    def save(self, d: date, data):
        f = self._file(d)
        f.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    def get(self, d: date, key: str):
        return self.load(d).get(key)

    def set(self, d: date, key: str, value):
        data = self.load(d)
        data[key] = value
        self.save(d, data)
