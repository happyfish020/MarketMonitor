import yaml
import os
from src.data_loader import load_market_data
from src.metrics import compute_metrics
from src.state_mapper import map_states
from src.gate_engine import compute_gate
from src.health_check import run_health_check

def main():
    with open("config.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    os.makedirs(cfg["output"]["dir"], exist_ok=True)

    df_prices = load_market_data(
        csv_path=cfg["data"]["price_csv"],
        start_date=cfg["backtest"]["start_date"],
        days=cfg["backtest"]["days"]
    )

    df_metrics = compute_metrics(df_prices, cfg["metrics"]["new_low_window"])
    df_states = map_states(df_metrics)
    df_gate = compute_gate(df_states)

    gate_csv = os.path.join(cfg["output"]["dir"], "daily_gate.csv")
    df_gate.to_csv(gate_csv, index=False)

    health = run_health_check(df_gate)

    with open(os.path.join(cfg["output"]["dir"], "health_report.json"), "w", encoding="utf-8") as f:
        import json
        json.dump(health, f, indent=2, ensure_ascii=False)

    print("✅ Phase-2 backtest finished")
    print("Health status:", health["summary"]["status"])

if __name__ == "__main__":
    main()
