from pathlib import Path

def project_root() -> Path:
    # Assuming this file resides in unifiedrisk/common/utils.py
    return Path(__file__).resolve().parents[2]
