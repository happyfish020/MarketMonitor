CN Entry Pool Rotation (SQLite + Oracle facts)

Run:
  python tools/run_cn_epr_eod.py --trade-date YYYY-MM-DD
  python tools/run_cn_epr_t1.py  --trade-date YYYY-MM-DD

Notes:
- Oracle source: SECOPR.CN_STOCK_DAILY_PRICE (CLOSE/VOLUME/HIGH/TRADE_DATE)
- SQLite store: data/cn_entry_pool_rotation.db (created automatically)
- Entry pool config: config/cn_entry_pool_rotation.yaml
