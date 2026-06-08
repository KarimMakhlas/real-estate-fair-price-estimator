"""
extract_ecb_rates.py
────────────────────
Ingestion job – ECB Data Portal (ESTR interest rate)

Fetches the latest ESTR (Euro Short-Term Rate) values from the ECB Data Portal
REST API and stores them as a JSON file in the raw Data Lake layer.

Output path:
    data/raw/real_estate/ecb_rates/ingestion_date=YYYY-MM-DD/estr.json

API documentation:
    https://data.ecb.europa.eu/help/api/data

Usage (standalone):
    python jobs/ingestion/extract_ecb_rates.py

Environment variables:
    DATALAKE_ROOT   – absolute path to the data lake root (default: "data")
"""

import json
import logging
import os
from datetime import date
from pathlib import Path

import requests
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"

# Fallback API endpoint if the one in sources.yml is unavailable
_FALLBACK_URL = (
    "https://data-api.ecb.europa.eu/service/data/EON/B.ESTR.PERC.1M.2.EUR.000.NITE.A"
)


def _load_config():
    with open(CONFIG_DIR / "sources.yml") as f:
        sources = yaml.safe_load(f)
    with open(CONFIG_DIR / "paths.yml") as f:
        paths = yaml.safe_load(f)
    return sources, paths


def _parse_ecb_json(data: dict, rate_type: str, source: str) -> list[dict]:
    """
    Parse the ECB SDMX-JSON response into a list of records.
    Structure: data → dataSets[0] → series → {0: {observations: {period_idx: [value, ...]}}}
    """
    records = []
    try:
        dataset = data["data"]["dataSets"][0]
        structure = data["data"]["structure"]
        time_periods = [
            dim["id"]
            for dim in structure["dimensions"]["observation"]
            if dim["id"] == "TIME_PERIOD"
        ]

        # Time dimension values
        obs_dims = structure["dimensions"]["observation"]
        time_dim = next((d for d in obs_dims if d["id"] == "TIME_PERIOD"), None)
        if not time_dim:
            log.warning("TIME_PERIOD dimension not found in ECB response")
            return records

        time_values = [v["id"] for v in time_dim["values"]]

        for series_key, series_data in dataset["series"].items():
            observations = series_data.get("observations", {})
            for idx_str, obs_vals in observations.items():
                idx = int(idx_str)
                if idx < len(time_values) and obs_vals[0] is not None:
                    records.append(
                        {
                            "date": time_values[idx],
                            "rate_value": float(obs_vals[0]),
                            "rate_type": rate_type,
                            "source": source,
                        }
                    )
    except (KeyError, IndexError, TypeError) as exc:
        log.error("Failed to parse ECB response: %s", exc)

    return records


def run(ingestion_date: str | None = None) -> None:
    sources, paths = _load_config()
    root = os.getenv("DATALAKE_ROOT", paths.get("data_lake_root", "data"))
    ingestion_date = ingestion_date or str(date.today())

    ecb_cfg = sources["ecb_rates"]
    api_url = ecb_cfg.get("api_url", _FALLBACK_URL)
    params = ecb_cfg.get("params", {"format": "jsondata", "lastNObservations": 30})
    rate_type = ecb_cfg.get("rate_type", "ESTR")
    source = ecb_cfg.get("source", "ECB Data Portal")

    out_dir = (
        Path(root) / "raw" / "real_estate" / "ecb_rates" / f"ingestion_date={ingestion_date}"
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    fallback_rate = ecb_cfg.get("fallback_rate", 3.9)
    records = []

    log.info("Fetching ECB rates from %s", api_url)
    try:
        response = requests.get(api_url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        records = _parse_ecb_json(data, rate_type, source)
    except Exception as exc:
        log.warning("ECB API unavailable (%s) — using fallback rate %.2f%%", exc, fallback_rate)

    if not records:
        log.warning("No rate records from API — writing fallback rate %.2f%%", fallback_rate)
        records = [{
            "date": str(date.today()),
            "rate_value": fallback_rate,
            "rate_type": rate_type,
            "source": f"{source} (fallback)",
        }]
    else:
        log.info("Parsed %d rate observations", len(records))

    dest = out_dir / "estr.json"
    with open(dest, "w") as fh:
        json.dump(records, fh, indent=2)

    log.info("Saved %s (%d records)", dest, len(records))
    log.info("ECB rates ingestion complete → %s", out_dir)


if __name__ == "__main__":
    run()
