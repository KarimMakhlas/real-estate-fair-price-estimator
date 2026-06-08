"""
export_for_deploy.py
─────────────────────
Exports the computed usage parquet data to a plain JSON file that can be
committed to git and read by the Render deployment (no Spark/Airflow needed).

Run this locally after the pipeline has produced results:
    docker compose exec airflow-webserver bash -c \
        "cd /opt/airflow && python scripts/export_for_deploy.py"

Output: api/data.json
"""

import json
import os
from pathlib import Path

import pandas as pd

ROOT = Path(os.getenv("DATALAKE_ROOT", "data"))
USAGE_PATH = ROOT / "usage" / "real_estate" / "fair_price_estimates"
OUT_PATH = Path(__file__).resolve().parents[1] / "api" / "data.json"


def run():
    if not USAGE_PATH.exists() or not list(USAGE_PATH.rglob("*.parquet")):
        print("No parquet data found. Run the pipeline first.")
        return

    df = pd.read_parquet(str(USAGE_PATH))

    # Convert all values to JSON-serialisable types
    records = []
    for _, row in df.iterrows():
        rec = {}
        for k, v in row.items():
            if hasattr(v, "item"):       # numpy scalar
                rec[k] = v.item()
            elif hasattr(v, "isoformat"): # date/datetime
                rec[k] = str(v)[:10]
            elif v != v:                  # NaN check
                rec[k] = None
            else:
                rec[k] = v
        records.append(rec)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(records, f, indent=2)

    print(f"Exported {len(records)} records → {OUT_PATH}")


if __name__ == "__main__":
    run()
