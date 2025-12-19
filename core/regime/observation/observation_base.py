from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional

RiskLevel = Literal["LOW", "NEUTRAL", "HIGH"]
ObservationProfile = Literal["sector", "stock", "index"]


@dataclass(frozen=True, slots=True)
class ObservationMeta:
    kind: str
    profile: ObservationProfile
    phase: str
    asof: str
    inputs: List[str]
    coverage_source: Optional[str] = None
    note: str = "解释性观察，不构成预测、推荐或行动指令。"


class ObservationBase(ABC):
    """
    Phase-2 ObservationBase（冻结：最小协议）
    - 只读 Phase-2 outputs（slots / factor results）
    - 产出可审计 dict：meta/evidence/observation
    - 不访问 DS/snapshot
    - 不产生 score/weight
    - 缺失输入：unknown/NA，不抛异常（仅日志）
    """

    @property
    @abstractmethod
    def meta(self) -> ObservationMeta:
        raise NotImplementedError

    @abstractmethod
    def build(self, *, inputs: Dict[str, Any], asof: str) -> Dict[str, Any]:
        """
        返回 Observation dict（必须含 meta/evidence/observation）
        """
        raise NotImplementedError

    def validate(self, *, observation: Dict[str, Any]) -> None:
        """
        冻结：默认仅做轻校验，不 raise。
        如需 dev_mode 严格校验，可在调用方外置控制。
        """
        # minimal structural checks
        if not isinstance(observation, dict):
            return
        for k in ("meta", "evidence", "observation"):
            if k not in observation:
                return
