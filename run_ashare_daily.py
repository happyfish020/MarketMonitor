from pathlib import Path
import unified_risk.config.path_config
from unified_risk.core.ashare.report_writer import run_and_write

def main():
    out_dir = Path("reports")   # ←必须转成 Path
    p = run_and_write(out_dir)
    print("Report written to:", p)

if __name__ == "__main__":
    main()
