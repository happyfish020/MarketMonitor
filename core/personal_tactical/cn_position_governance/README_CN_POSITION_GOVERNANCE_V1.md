# CN_POSITION_GOVERNANCE_V1 (Personal Tactical Subsystem)

## 1) Architecture Diagram

```mermaid
flowchart LR
  subgraph CLI[tools/]
    EOD[run_cn_pg_eod.py --trade-date]
    T1[run_cn_pg_t1.py --trade-date]
  end

  subgraph PG[core/personal_tactical/cn_position_governance/]
    ENG[engine.py\nCnPositionGovernanceEngine]
    SM[state_machine.py\nRiskStateMachine]
    ORA[oracle_facts.py\nOracleFacts]
    ST[sqlite_store.py\nSqliteStore]
    REP[reporting.py]
    CFG[config.py]
    SQL[resources/sqlite_schema.sql]
  end

  subgraph ORACLE[Oracle XE]
    TBL[(SECOPR.CN_STOCK_DAILY_PRICE)]
  end

  subgraph SQLITE[SQLite]
    DB[(data/cn_position_governance.db)]
    C1[cn_pg_position_config]
    S1[cn_pg_position_state]
    L1[cn_pg_event_log]
  end

  EOD --> ENG
  T1 --> ENG
  ENG --> ORA --> TBL
  ENG --> SM
  ENG --> ST --> DB
  ST --> C1
  ST --> S1
  ST --> L1
  ST -. init .-> SQL
  ENG --> REP
  ENG --> CFG
```

## 2) Function Contract Table

| Module | Function/Class | Inputs | Outputs | Side Effects | Failure Policy |
|---|---|---|---|---|---|
| tools/run_cn_pg_eod.py | main() | --trade-date | stdout summary + run_id | writes SQLite state + events | hard-fail (no silent fail) |
| tools/run_cn_pg_t1.py | main() | --trade-date | stdout signals + run_id | writes T1_SIGNAL events | hard-fail |
| engine.py | CnPositionGovernanceEngine.run_eod | trade_date, run_id | list[EvalResult] | upsert state, insert events | raise on Oracle/SQLite errors |
| engine.py | CnPositionGovernanceEngine.run_t1 | trade_date, run_id | list[{symbol,signal,reason}] | insert T1_SIGNAL events | raise on SQLite errors |
| oracle_facts.py | OracleFacts.load_bars | symbols, end_trade_date, lookback_days | dict[symbol->bars] | Oracle read | raise on DB errors |
| state_machine.py | RiskStateMachine.compute_metrics | bars | RiskMetrics/None | none | None if insufficient bars |
| state_machine.py | RiskStateMachine.classify | RiskMetrics | LOW/NORMAL/HIGH | none | always returns level |
| sqlite_store.py | SqliteStore.init_schema | - | - | create tables/index if not exist | raise |
| sqlite_store.py | SqliteStore.upsert_position_state | state row | - | UPSERT by PK | raise |
| sqlite_store.py | SqliteStore.insert_event_dedup | event row | - | INSERT OR IGNORE (dedup by unique idx) | raise |

## 3) SQLite Schema

Path: `core/personal_tactical/cn_position_governance/resources/sqlite_schema.sql`

- `cn_pg_position_config` (static config)
- `cn_pg_position_state` (daily state, PK(trade_date, symbol))
- `cn_pg_event_log` (event stream, unique(trade_date,symbol,event_type,run_id))

## 4) EOD Flow (run_cn_pg_eod.py)

1. Init SQLite schema (idempotent).
2. Read config rows: (symbol, theme, max_lots, theme_cap_pct, enable_add, enable_trim).
3. OracleFacts: fetch daily bars up to 35 trading bars ending at trade_date (DATE bind).
4. Compute metrics per symbol:
   - MA20
   - 10D return
   - 10D drawdown (rolling peak-to-trough within last 10)
   - volume shrink ratio (avg vol last5 / prev5)
   - 3D close below MA20
5. Risk classification: LOW / NORMAL / HIGH.
6. Apply frozen rules:
   - Add permission: risk!=HIGH, dd_10d<=-8%, lots<max_lots, theme exposure < cap, enable_add=1
   - Trim required: dd_10d<-15% OR 3D below MA20 OR theme risk HIGH
7. Persist:
   - UPSERT state row (PK(trade_date,symbol))
   - insert event log rows: EOD_ADD_ALLOWED/EOD_ADD_BLOCKED (+EOD_TRIM_REQUIRED when triggered)
8. Print summary.

## 5) T+1 Flow (run_cn_pg_t1.py)

1. Read state rows for trade_date from SQLite.
2. Derive T+1 signal per symbol:
   - TRIM_REQUIRED if trim_required=1
   - else ADD_ALLOWED if add_permission=1
   - else HOLD_ONLY
3. Write event log row per symbol: event_type = T1_SIGNAL, reason = "<SIGNAL>::<details>".
4. Print signal list.

## 6) Idempotency Strategy

- State table uses UPSERT on PK(trade_date,symbol): rerun overwrites the same row and updates run_id/created_at.
- Event log uses unique(trade_date,symbol,event_type,run_id) + INSERT OR IGNORE:
  - Prevents duplicates for the same run_id.
  - Allows multiple runs for the same trade_date with distinct run_id for audit replay.
- Schema creation is safe to call every run (CREATE IF NOT EXISTS).
- No silent fail: all Oracle/SQLite exceptions propagate.

## Notes / Frozen Constraints Coverage

- Oracle DSN fixed: `oracle+oracledb://secopr:secopr@localhost:1521/xe`
- Oracle source fixed: `SECOPR.CN_STOCK_DAILY_PRICE`
- DATE bind used via SQLAlchemy `bindparam(..., type_=Date())`
- CLI only allows `--trade-date`
- No bootstrap.py / db.py / persistence.py added
