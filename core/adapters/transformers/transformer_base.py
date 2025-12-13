# core/adapters/transformers/base.py
# UnifiedRisk V12.1 - Transformer Base Class
# 所有 Transformer 必须继承本类

from __future__ import annotations

from typing import Dict, Any

from core.utils.logger import get_logger

LOG = get_logger("TransformerBase")


class TransformerBase:
    """
    UnifiedRisk V12.1
    -----------------
    Transformer 的职责：
    - 输入 snapshot dict（只读）
    - 提取数据 / 清洗 / 补特征 / 变换格式
    - 输出新的 block dict（写入 snapshot 由 fetcher 完成）

    Transformer 禁止：
    - 拉取外部数据
    - 写 cache/history
    - 写文件
    """

    def __init__(self, name: str = "Transformer"):
        self.name = name
        self.logger = LOG

    # ---------------------------------------------------------
    def transform(self, snapshot: Dict[str, Any], refresh_mode: str = "none") -> Dict[str, Any]:
        """
        所有子类必须实现：
        输入 snapshot（dict）
        输出 block（dict）
        """
        raise NotImplementedError(
            f"{self.__class__.__name__}.transform() 必须由子类实现"
        )

    # ---------------------------------------------------------
    def log(self, msg: str):
        """方便统一日志输出"""
        self.logger.info(f"[{self.name}] {msg}")
