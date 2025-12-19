# core/utils/ds_refresh.py
# ============================================
# UnifiedRisk V12 - DataSource Refresh Helper
# ============================================

import os
from typing import Optional


def normalize_refresh_mode(mode) -> str:
    """
    统一把 refresh 参数转成字符串:
    - "full" / "snapshot" / "none" 保持不变（忽略大小写）
    - True  -> "snapshot"
    - False -> "none"
    - 其它非法值一律视为 "none"
    """
    if isinstance(mode, str):
        m = mode.strip().lower()
        if m in ("full", "snapshot", "none"):
            return m
        return "none"
    if mode is True:
        return "snapshot"
    return "none"


def apply_refresh_cleanup(
    refresh_mode,
    cache_path: Optional[str] = None,
    history_path: Optional[str] = None,
    spot_path: Optional[str] = None,
) -> str:
    """
    执行 Step 1 的统一清理逻辑，并返回规范化后的 refresh_mode 字符串。
    所有路径参数如果为 None，则忽略。
    """
    mode = normalize_refresh_mode(refresh_mode)

    def _rm(path: Optional[str]):
        if path and os.path.exists(path):
            try:
                os.remove(path)
            except Exception:
                # 保守起见，删除失败也不抛异常，由上层 log
                pass

    if mode == "full":
        _rm(cache_path)
        _rm(history_path)
        _rm(spot_path)
    elif mode == "snapshot":
        _rm(cache_path)
    else:
        # "none" 不做任何清理
        pass

    return mode
