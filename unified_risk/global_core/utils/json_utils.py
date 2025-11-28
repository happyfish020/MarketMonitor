
from __future__ import annotations

import json
from typing import Any

def json_default(o: Any):
    try:
        import numpy as np  # type: ignore
    except Exception:
        return str(o)
    if "numpy" in str(type(o)):
        try:
            return o.item()
        except Exception:
            return str(o)
    return str(o)


def to_json(data: Any, ensure_ascii: bool = False) -> str:
    return json.dumps(data, ensure_ascii=ensure_ascii, default=json_default)
