# -*- coding: utf-8 -*-
"""
UnifiedRisk V12
SnapshotBuilderBase

职责：
- 定义 snapshot builder 的统一接口
- 作为所有市场 snapshot builder 的抽象父类
"""

from abc import ABC, abstractmethod
from typing import Dict, Any


class SnapshotBuilderBase(ABC):
    """
    SnapshotBuilderBase（V12）

    说明：
    - 抽象基类
    - 永不被直接实例化
    - 不包含任何市场 / 业务逻辑
    """

    @abstractmethod
    def build(self, snapshot: Dict[str, Any]) -> Dict[str, Any]:
        """
        构建 / 修复 snapshot 结构

        参数：
            snapshot: fetcher 输出的原始 snapshot dict

        返回：
            结构修复后的 snapshot dict
        """
        raise NotImplementedError
