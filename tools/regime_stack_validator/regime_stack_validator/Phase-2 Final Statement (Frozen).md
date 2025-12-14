
---

# 文档二  
## `PHASE2_RULES_REFERENCE.md`

> **用途**：  
> - Phase-3 / Phase-4 的“制度速查表”  
> - 防止未来引入新模块时：
>   - 偷偷改口径
>   - 混淆 State / Gate / Health 的职责
> - 类似“宪法附录”

---

```markdown
# Phase-2 Rules Reference  
(UnifiedRisk V12 – RegimeStack)

## 1. Phase-2 Mission (Reminder)

Phase-2 answers one question only:

> Can the system detect **mid-term structural risk**
> and enforce **stable, disciplined risk behavior**?

It does NOT:
- predict prices
- optimize returns
- perform intraday trading

---

## 2. Layer Responsibility Matrix

| Layer | Can Read | Can Write | Forbidden |
|------|---------|-----------|----------|
| DataLoader | Raw data | Standardized DataFrame | Any scoring / logic |
| Metrics | Prices | Numeric metrics | Thresholds / states |
| State Mapper | Metrics | Discrete states | Gate logic |
| Gate Engine | States | H4_gate | Reading prices |
| Health Check | Gate history | Health verdict | Fixing behavior |

---

## 3. Core Metrics (Frozen)

### 3.1 Breadth Metrics
- adv_ratio
- median_return
- new_low_ratio
- new_low_persistence

### 3.2 new_low_window
- Default: **50**
- Meaning: mid-term structural damage
- Must NOT scale with backtest window length

---

## 4. State Definitions (Frozen Semantics)

### Breadth Damage State
- Healthy
- Early
- Confirmed
- Breakdown

### Participation State
- BroadUp
- Narrow
- HiddenWeakness
- Neutral

---

## 5. Gate Levels (H4_gate)

| Gate | Meaning |
|----|--------|
| Normal | Normal risk exposure allowed |
| Caution | Risk slowing, no aggressive expansion |
| PlanB | Defensive posture |
| Freeze | Stop offensive actions |

---

## 6. Gate Transition Rules (Phase-2 Frozen)

### 6.1 Downgrade Rules
- Breakdown → Freeze
- Confirmed → PlanB
- HiddenWeakness → PlanB
- Early / Narrow → Caution

### 6.2 Recovery Discipline (CRITICAL)

```text
PlanB → Normal ❌
PlanB → Caution → Normal ✅
This rule is mandatory to prevent zigzag behavior.

7. Health Check Red Lines
Automatic FAIL if any occurs:

zigzag_detected == true

Breadth damage (Confirmed / Breakdown) with gate == Normal

WARNING (but not FAIL):

HiddenWeakness capture ratio < 80%

8. Event-Anchored Backtest Rule (Recommended)

Given an event anchor date:

start_date ≤ anchor_date − max(50, 40)

end_date ≥ anchor_date + 20

Purpose:

avoid hindsight bias

preserve metric validity

9. Phase-2 Freeze Declaration

The following are frozen until Phase-3:

new_low_window = 50

Breadth Damage logic

Participation logic

Gate recovery discipline

Health red lines

Any modification requires:

new Phase

explicit rationale

independent Health validation

