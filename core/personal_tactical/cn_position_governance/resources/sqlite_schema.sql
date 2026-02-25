-- CN_POSITION_GOVERNANCE_V1 (SQLite)
-- File: data/cn_position_governance.db

PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS cn_pg_position_config (
    symbol        TEXT PRIMARY KEY,
    theme         TEXT NOT NULL,
    max_lots      INTEGER NOT NULL,
    theme_cap_pct REAL NOT NULL,
    enable_add    INTEGER NOT NULL,
    enable_trim   INTEGER NOT NULL,
    created_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cn_pg_position_state (
    trade_date      TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    lots_held       INTEGER NOT NULL,
    avg_cost        REAL NOT NULL,
    exposure_pct    REAL NOT NULL,
    risk_level      TEXT NOT NULL,
    add_permission  INTEGER NOT NULL,
    trim_required   INTEGER NOT NULL,
    run_id          TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    PRIMARY KEY (trade_date, symbol)
);

CREATE TABLE IF NOT EXISTS cn_pg_event_log (
    trade_date  TEXT NOT NULL,
    symbol      TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    reason      TEXT NOT NULL,
    run_id      TEXT NOT NULL,
    created_at  TEXT NOT NULL
);

-- Prevent duplicate event inserts for the same run_id
CREATE UNIQUE INDEX IF NOT EXISTS ux_cn_pg_event_dedup
ON cn_pg_event_log(trade_date, symbol, event_type, run_id);

CREATE INDEX IF NOT EXISTS ix_cn_pg_state_symbol_date
ON cn_pg_position_state(symbol, trade_date);

CREATE INDEX IF NOT EXISTS ix_cn_pg_event_symbol_date
ON cn_pg_event_log(symbol, trade_date);
