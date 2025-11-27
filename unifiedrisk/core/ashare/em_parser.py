
import json
import re
from typing import Any, Dict


def parse_em_jsonp(text: str) -> Dict[str, Any]:
    """Robust EastMoney JSON / JSONP parser.

    - If text is plain JSON, parse directly.
    - If text is JSONP (callback(...)), strip wrapper then parse.
    - On failure, return {"result": {"data": []}} as a safe default.
    """
    text = (text or "").strip()

    if text.startswith("{") and text.endswith("}"):
        try:
            return json.loads(text)
        except Exception:
            pass

    try:
        json_str = re.sub(r"^[^(]*\((.*)\)[^)]*$", r"\1", text)
        return json.loads(json_str)
    except Exception:
        return {"result": {"data": []}}
