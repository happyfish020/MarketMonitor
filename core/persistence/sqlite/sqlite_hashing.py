# -*- coding: utf-8 -*-
from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Optional


def _canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def sha256_hex(obj: Any) -> str:
    data = _canonical_json(obj).encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def report_hash(trade_date: str, report_kind: str, content_text: str, meta_json: Optional[str]) -> str:
    return sha256_hex({
        "trade_date": trade_date,
        "report_kind": report_kind,
        "content_text": content_text,
        "meta_json": meta_json,
    })


def des_hash(trade_date: str, report_kind: str, engine_version: str, des_payload_json: str) -> str:
    return sha256_hex({
        "trade_date": trade_date,
        "report_kind": report_kind,
        "engine_version": engine_version,
        "des_payload_json": des_payload_json,
    })
