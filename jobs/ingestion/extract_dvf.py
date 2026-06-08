"""
extract_dvf.py
──────────────
Ingestion job – DVF (Demandes de Valeurs Foncières)

Downloads historical property transaction CSV files from data.gouv.fr and
stores them in the raw Data Lake layer, partitioned by ingestion date.

Output path:
    data/raw/real_estate/dvf/ingestion_date=YYYY-MM-DD/dvf_YYYY.csv

Usage (standalone):
    python jobs/ingestion/extract_dvf.py

Environment variables:
    DATALAKE_ROOT   – absolute path to the data lake root (default: "data")
"""

import os
import logging
from datetime import date
from pathlib import Path

import requests
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"

def _load_config():
    with open(CONFIG_DIR / "sources.yml") as f:
        sources = yaml.safe_load(f)
    with open(CONFIG_DIR / "paths.yml") as f:
        paths = yaml.safe_load(f)
    return sources, paths


# ── Helpers ───────────────────────────────────────────────────────────────────

def _raw_dvf_path(root: str, ingestion_date: str) -> Path:
    return Path(root) / "raw" / "real_estate" / "dvf" / f"ingestion_date={ingestion_date}"


def _build_dvf_url(base_url: str, year: int, department: str) -> str:
    """
    DVF URL pattern on data.gouv.fr:
    https://files.data.gouv.fr/geo-dvf/latest/csv/{year}/departements/{dept}.csv
    """
    return f"{base_url}/{year}/departements/{department}.csv"


def _download_file(url: str, dest: Path) -> bool:
    """Download a file with streaming. Returns True on success."""
    log.info("Downloading %s → %s", url, dest)
    try:
        response = requests.get(url, stream=True, timeout=120)
        response.raise_for_status()
        dest.parent.mkdir(parents=True, exist_ok=True)
        with open(dest, "wb") as fh:
            for chunk in response.iter_content(chunk_size=1024 * 256):
                fh.write(chunk)
        log.info("Saved %s (%.1f MB)", dest.name, dest.stat().st_size / 1e6)
        return True
    except requests.HTTPError as exc:
        log.warning("HTTP error for %s: %s", url, exc)
        return False
    except Exception as exc:
        log.error("Failed to download %s: %s", url, exc)
        return False


# ── Main ──────────────────────────────────────────────────────────────────────

def run(ingestion_date: str | None = None) -> None:
    sources, paths = _load_config()
    root = os.getenv("DATALAKE_ROOT", paths.get("data_lake_root", "data"))
    ingestion_date = ingestion_date or str(date.today())

    dvf_cfg = sources["dvf"]
    base_url = dvf_cfg["base_url"]
    years = dvf_cfg["years"]
    departments = dvf_cfg["departments"]
    columns_to_keep = dvf_cfg["columns_to_keep"]

    out_dir = _raw_dvf_path(root, ingestion_date)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Write column whitelist so downstream jobs can reference it
    col_file = out_dir / "_columns_to_keep.txt"
    col_file.write_text("\n".join(columns_to_keep))

    for year in years:
        # Merge all departments for this year into a single CSV to keep
        # the Data Lake structure simple for the MVP.
        import pandas as pd

        year_frames = []
        for dept in departments:
            url = _build_dvf_url(base_url, year, dept)
            tmp_path = out_dir / f"_tmp_{year}_{dept}.csv"
            ok = _download_file(url, tmp_path)
            if not ok:
                continue
            try:
                df = pd.read_csv(
                    tmp_path,
                    sep="|",
                    dtype=str,
                    low_memory=False,
                    usecols=lambda c: c in columns_to_keep,
                )
                year_frames.append(df)
                tmp_path.unlink(missing_ok=True)
            except Exception as exc:
                log.error("Could not parse %s: %s", tmp_path, exc)
                tmp_path.unlink(missing_ok=True)

        if year_frames:
            combined = pd.concat(year_frames, ignore_index=True)
            dest = out_dir / f"dvf_{year}.csv"
            combined.to_csv(dest, index=False)
            log.info("Wrote %s (%d rows)", dest, len(combined))
        else:
            log.warning("No data collected for year %d", year)

    log.info("DVF ingestion complete → %s", out_dir)


if __name__ == "__main__":
    run()
