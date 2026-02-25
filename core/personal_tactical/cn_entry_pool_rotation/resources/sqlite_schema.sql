-- CN_ENTRY_POOL_ROTATION_V1 SQLite schema (idempotent)

BEGIN;

CREATE TABLE IF NOT EXISTS cn_epr_entry_pool (
  symbol TEXT PRIMARY KEY,
  theme TEXT,
  name TEXT,
  oracle_symbol TEXT NOT NULL,
  entry_mode TEXT NOT NULL,
  max_lots_2026 INTEGER DEFAULT 0,
  is_active INTEGER DEFAULT 1,
  updated_at TEXT
);

CREATE TABLE IF NOT EXISTS cn_epr_state_snap (
  trade_date TEXT NOT NULL,
  symbol TEXT NOT NULL,
  state TEXT NOT NULL,
  breakout_level REAL,
  confirm_ok_streak INTEGER DEFAULT 0,
  fail_streak INTEGER DEFAULT 0,
  cooldown_days_left INTEGER DEFAULT 0,
  asof TEXT NOT NULL,
  updated_at TEXT,
  PRIMARY KEY (trade_date, symbol)
);

CREATE TABLE IF NOT EXISTS cn_epr_state_event (
  trade_date TEXT NOT NULL,
  symbol TEXT NOT NULL,
  event_kind TEXT NOT NULL,
  from_state TEXT NOT NULL,
  to_state TEXT NOT NULL,
  reason_code TEXT NOT NULL,
  reason_text TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE (trade_date, symbol, event_kind, from_state, to_state, reason_code)
);

CREATE TABLE IF NOT EXISTS cn_epr_position_snap (
  trade_date TEXT NOT NULL,
  symbol TEXT NOT NULL,
  position_lots INTEGER NOT NULL,
  avg_cost REAL,
  asof TEXT NOT NULL,
  updated_at TEXT,
  PRIMARY KEY (trade_date, symbol)
);

CREATE TABLE IF NOT EXISTS cn_epr_execution (
  trade_date TEXT NOT NULL,
  symbol TEXT NOT NULL,
  action TEXT NOT NULL,        -- BUY / SELL / HOLD / NONE
  lots INTEGER NOT NULL,
  limit_price REAL,
  note TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (trade_date, symbol, action)
);

COMMIT;
