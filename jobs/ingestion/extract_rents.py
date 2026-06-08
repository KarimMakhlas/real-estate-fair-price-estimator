"""
extract_rents.py
────────────────
Ingestion job – Carte des loyers 2025 (rent indicators by commune)

Downloads the apartment and house rent indicator CSVs from data.gouv.fr,
merges them on commune code, and stores the result in the raw Data Lake layer.

Output path:
    data/raw/real_estate/rents/ingestion_date=YYYY-MM-DD/rents_2025.csv

Usage (standalone):
    python jobs/ingestion/extract_rents.py

Environment variables:
    DATALAKE_ROOT   – absolute path to the data lake root (default: "data")
"""

import os
import logging
from datetime import date
from io import StringIO
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


def _download_csv(url: str) -> pd.DataFrame:
    log.info("Downloading %s", url)
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    raw = response.content.decode("utf-8", errors="replace")
    sep = ";" if raw.count(";") > raw.count(",") else ","
    df = pd.read_csv(StringIO(raw), sep=sep, dtype=str, low_memory=False)
    df.columns = [c.lower().strip() for c in df.columns]
    log.info("  → %d rows, columns: %s", len(df), list(df.columns))
    return df


def _find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Return the first column name from candidates that exists in df."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def run(ingestion_date: str | None = None) -> None:
    sources, paths = _load_config()
    root = os.getenv("DATALAKE_ROOT", paths.get("data_lake_root", "data"))
    ingestion_date = ingestion_date or str(date.today())

    rents_cfg = sources["rents"]
    source_year = rents_cfg["source_year"]

    out_dir = Path(root) / "raw" / "real_estate" / "rents" / f"ingestion_date={ingestion_date}"
    out_dir.mkdir(parents=True, exist_ok=True)

    # ── Download apartment file ───────────────────────────────────────────────
    df_apt = _download_csv(rents_cfg["url_apartment"])

    # ── Download house file ───────────────────────────────────────────────────
    df_house = _download_csv(rents_cfg["url_house"])

    # ── Detect commune code & name columns (flexible) ─────────────────────────
    code_candidates = ["cod_communes", "code_commune", "codecommune", "cod_comm", "codgeo"]
    name_candidates = ["lib_communes", "nom_commune", "libelle_commune", "lib_comm", "libgeo"]
    rent_candidates = ["loypredm2", "loyer_m2", "loyer_pred_m2", "loy_pred_m2", "loyerm2", "pred_m2"]

    apt_code = _find_col(df_apt, code_candidates)
    apt_name = _find_col(df_apt, name_candidates)
    apt_rent = _find_col(df_apt, rent_candidates)

    house_code = _find_col(df_house, code_candidates)
    house_rent = _find_col(df_house, rent_candidates)

    if not apt_code or not apt_rent:
        log.error("Apartment CSV columns not recognised. Got: %s", list(df_apt.columns))
        raise ValueError(f"Cannot find commune code or rent column in apartment CSV. Columns: {list(df_apt.columns)}")

    if not house_code or not house_rent:
        log.error("House CSV columns not recognised. Got: %s", list(df_house.columns))
        raise ValueError(f"Cannot find commune code or rent column in house CSV. Columns: {list(df_house.columns)}")

    # ── Build unified dataframe ───────────────────────────────────────────────
    apt_cols = {apt_code: "code_commune", apt_rent: "loyer_m2_appartement"}
    if apt_name:
        apt_cols[apt_name] = "nom_commune"
    df_apt = df_apt.rename(columns=apt_cols)[list(apt_cols.values())]

    house_cols = {house_code: "code_commune", house_rent: "loyer_m2_maison"}
    df_house = df_house.rename(columns=house_cols)[list(house_cols.values())]

    merged = pd.merge(df_apt, df_house, on="code_commune", how="outer")
    merged["source_year"] = source_year

    # ── Normalise commune code to 5 chars ─────────────────────────────────────
    merged["code_commune"] = merged["code_commune"].astype(str).str.zfill(5)

    dest = out_dir / f"rents_{source_year}.csv"
    merged.to_csv(dest, index=False)
    log.info("Saved %s (%d rows, %.1f KB)", dest, len(merged), dest.stat().st_size / 1e3)
    log.info("Rent ingestion complete → %s", out_dir)


if __name__ == "__main__":
    run()
