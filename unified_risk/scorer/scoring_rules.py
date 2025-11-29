def classify_risk_level(score: float) -> str:
    if score >= 3:
        return "High Risk"
    if score >= 1:
        return "Moderate Risk"
    if score <= -3:
        return "Critical Risk"
    if score <= -1:
        return "Low Risk"
    return "Neutral"

def score_to_advise(score: float) -> str:
    if score >= 3:
        return "Reduce positions."
    if score >= 1:
        return "Slightly bullish."
    if score <= -3:
        return "Be defensive."
    if score <= -1:
        return "Caution."
    return "Wait and observe."
