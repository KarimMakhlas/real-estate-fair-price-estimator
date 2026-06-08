"""
extract_rents.py
────────────────
Ingestion job – Carte des loyers (rent indicators by commune)

Downloads the rent indicators CSV from data.gouv.fr and stores it in the
raw Data Lake layer, partitioned by ingestion date.

Output path:
    data/raw/real_estate/rents/ingestion_date=YYYY-MM-DD/rents_YYYY.csv

Usage (standalone):
    python jobs/ingestion/extract_rents.py

Environment variables:
    DATALAKE_ROOT   – absolute path to the data lake root (default: "data")
"""

import os
import logging
from datetime import date
from pathlib import Path

import requests
import pandas as pd
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"


def _load_config():
    with open(CONFIG_DIR / "sources.yml") as f:
        sources = yaml.safe_load(f)
    with open(CONFIG_DIR / "paths.yml") as f:
        paths = yaml.safe_load(f)
    return sources, paths


def run(ingestion_date: str | None = None) -> None:
    sources, paths = _load_config()
    root = os.getenv("DATALAKE_ROOT", paths.get("data_lake_root", "data"))
    ingestion_date = ingestion_date or str(date.today())

    rents_cfg = sources["rents"]
    url = rents_cfg["url"]
    columns_to_keep = rents_cfg["columns_to_keep"]
    source_year = rents_cfg["source_year"]

    out_dir = Path(root) / "raw" / "real_estate" / "rents" / f"ingestion_date={ingestion_date}"
    out_dir.mkdir(parents=True, exist_ok=True)

    log.info("Downloading rent indicators from %s", url)
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
    except Exception as exc:
        log.error("Failed to download rent indicators: %s", exc)
        raise

    # The file may use semicolons or commas; try both
    from io import StringIO
    raw_text = response.content.decode("utf-8", errors="replace")
    sep = ";" if raw_text.count(";") > raw_text.count(",") else ","
    df = pd.read_csv(StringIO(raw_text), sep=sep, dtype=str, low_memory=False)

    log.info("Downloaded %d rows, columns: %s", len(df), list(df.columns))

    # Keep only the columns we need (flexible — match case-insensitively)
    col_map = {c.lower().strip(): c for c in df.columns}
    keep = []
    for wanted in columns_to_keep:
        matched = col_map.get(wanted.lower().strip())
        if matched:
            keep.append(matched)
        else:
            log.warning("Column '%s' not found in source, skipping", wanted)

    df = df[keep].copy()
    df.columns = [c.lower().strip() for c in df.columns]

    dest = out_dir / f"rents_{source_year}.csv"
    df.to_csv(dest, index=False)
    log.info("Saved %s (%d rows, %.1f KB)", dest, len(df), dest.stat().st_size / 1e3)
    log.info("Rent ingestion complete → %s", out_dir)


if __name__ == "__main__":
    run()
