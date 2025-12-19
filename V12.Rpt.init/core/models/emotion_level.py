from enum import Enum


class EmotionLevel(str, Enum):
    PANIC = "PANIC"
    RISK_OFF = "RISK_OFF"
    NEUTRAL = "NEUTRAL"
    RISK_ON = "RISK_ON"
    EUPHORIA = "EUPHORIA"
