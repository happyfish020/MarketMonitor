# -*- coding: utf-8 -*-
"""UnifiedRisk V12 FULL
RuleSpec Loader (frozen)

职责：
- 加载 YAML / JSON 规则文件为 dict
- 永不抛异常：失败返回 (None, error)

注意：
- 本模块不做任何业务解释，不做执行
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional, Tuple

import yaml

from core.utils.logger import get_logger

LOG = get_logger("RuleLoader")


def load_rule_file(path: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Load a YAML/JSON rule file.

    Returns:
        (spec, error)
        - spec: dict when success, else None
        - error: str when failure, else None
    """
    if not path or not isinstance(path, str):
        return None, "invalid_path"

    if not os.path.exists(path):
        return None, f"not_found:{path}"

    ext = os.path.splitext(path)[1].lower()
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = f.read()
    except Exception as e:
        LOG.error("[RuleLoader] read error: %s", e)
        return None, f"read_error:{e}"

    try:
        if ext in (".yaml", ".yml"):
            data = yaml.safe_load(raw)
        elif ext == ".json":
            data = json.loads(raw)
        else:
            return None, f"unsupported_ext:{ext}"

        if not isinstance(data, dict):
            return None, "invalid_spec_type"

        # Minimal sanity checks (non-fatal)
        rid = data.get("rule_id")
        if rid is None:
            LOG.warning("[RuleLoader] missing rule_id: %s", path)

        return data, None

    except Exception as e:
        LOG.error("[RuleLoader] parse error: %s", e)
        return None, f"parse_error:{e}"
