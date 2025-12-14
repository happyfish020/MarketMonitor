# Phase-2 Changelog  
(UnifiedRisk V12 – RegimeStack Validator)

## Scope
This changelog records **all intentional changes** made during Phase-2,
from the first real-market run to the final HEALTHY validation.

Rule:
- Only *behavioral / structural* changes are allowed here.
- No parameter tuning or signal redefinition is allowed without a new Phase.

---

## v2.0.0 – Phase-2 Final (HEALTHY)

**Status:** HEALTHY  
**Date:** 2025-12-12  
**Data:** Real market data (≈400 stocks)  
**Window:** 2025-09-13 → 2025-12-12  
**new_low_window:** 50

### ✅ Result
- Phase-2 regime validation PASSED
- Gate behavior is stable
- No zigzag detected
- System is eligible to enter Phase-3

---

## v2.0.0-rc1 – First Real Run (FAIL)

**Status:** FAIL  
**Trigger:** zigzag_detected = true

### Observed Problem
Repeated patterns observed in `daily_gate.csv`:

```text
PlanB → Normal → PlanB

Phase-2 Final Statement (Frozen) 

Market Breadth Damage Factor
Index–Sector Correlation Regime Factor
Market Participation / Breadth Expansion Factor

