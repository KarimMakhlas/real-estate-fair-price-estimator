"""
generate_sample_data.py
────────────────────────
Generates realistic synthetic data for all three sources so the full pipeline
can be tested instantly without downloading real files.

Creates:
  data/raw/real_estate/dvf/ingestion_date=TODAY/dvf_2024.csv
  data/raw/real_estate/rents/ingestion_date=TODAY/rents_2025.csv
  data/raw/real_estate/ecb_rates/ingestion_date=TODAY/estr.json

Usage:
  python scripts/generate_sample_data.py

Environment variables:
  DATALAKE_ROOT  – data lake root (default: "data")
"""

import json
import os
import random
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

SEED = 42
random.seed(SEED)

ROOT = Path(os.getenv("DATALAKE_ROOT", "data"))
TODAY = str(date.today())

# ── Commune catalogue ─────────────────────────────────────────────────────────
# (code, name, dept, median_apt_m2, median_house_m2, rent_apt_m2, rent_house_m2)
COMMUNES = [
    ("75056", "Paris",           "75", 10500, 12000, 32.0, 28.0),
    ("92012", "Boulogne-Billancourt", "92", 8200, 9500, 25.0, 22.0),
    ("92049", "Nanterre",        "92", 5800, 6500, 18.0, 16.0),
    ("93008", "Aubervilliers",   "93", 4200, 4800, 14.5, 13.0),
    ("93066", "Saint-Denis",     "93", 3900, 4500, 13.5, 12.0),
    ("94028", "Créteil",         "94", 4500, 5200, 15.0, 14.0),
    ("94041", "Ivry-sur-Seine",  "94", 5100, 5900, 16.5, 15.0),
    ("44109", "Nantes",          "44", 4300, 3800, 15.8, 12.5),
    ("69123", "Lyon",            "69", 5200, 5800, 18.0, 16.0),
    ("13055", "Marseille",       "13", 3100, 3600, 12.0, 11.0),
]

PROPERTY_TYPES = ["Appartement", "Maison"]
ROOMS = [1, 2, 3, 4, 5]
ROOM_WEIGHTS = [0.15, 0.25, 0.30, 0.20, 0.10]

TRANSACTIONS_PER_SEGMENT = 80   # per commune × property_type × rooms bucket


def _rand_date(start="2022-01-01", end="2024-12-31") -> str:
    s = date.fromisoformat(start)
    e = date.fromisoformat(end)
    delta = (e - s).days
    return str(s + timedelta(days=random.randint(0, delta)))


def _price(median_m2: float, surface: float, noise: float = 0.20) -> float:
    factor = 1 + random.uniform(-noise, noise)
    return round(median_m2 * factor * surface, 0)


# ── DVF ───────────────────────────────────────────────────────────────────────

def generate_dvf() -> pd.DataFrame:
    rows = []
    for code, name, dept, med_apt, med_house, *_ in COMMUNES:
        for prop_raw, med_m2 in [("Appartement", med_apt), ("Maison", med_house)]:
            for rooms in ROOMS:
                # Surface varies by room count and type
                base_surface = 20 + rooms * (18 if prop_raw == "Appartement" else 25)
                for _ in range(TRANSACTIONS_PER_SEGMENT):
                    surface = round(base_surface * random.uniform(0.7, 1.4), 1)
                    price = _price(med_m2, surface)
                    rows.append({
                        "date_mutation": _rand_date(),
                        "valeur_fonciere": price,
                        "type_local": prop_raw,
                        "surface_reelle_bati": surface,
                        "nombre_pieces_principales": rooms,
                        "code_commune": code,
                        "nom_commune": name,
                        "code_departement": dept,
                    })
    return pd.DataFrame(rows)


# ── Rents ─────────────────────────────────────────────────────────────────────

def generate_rents() -> pd.DataFrame:
    rows = []
    for code, name, *_, rent_apt, rent_house in COMMUNES:
        rows.append({
            "code_commune": code,
            "nom_commune": name,
            "loyer_m2_appartement": round(rent_apt * random.uniform(0.95, 1.05), 2),
            "loyer_m2_maison": round(rent_house * random.uniform(0.95, 1.05), 2),
        })
    return pd.DataFrame(rows)


# ── ECB rates ─────────────────────────────────────────────────────────────────

def generate_ecb_rates() -> list[dict]:
    records = []
    base_rate = 3.9
    start = date(2024, 1, 1)
    for i in range(30):
        d = start + timedelta(days=i * 10)
        records.append({
            "date": str(d),
            "rate_value": round(base_rate + random.uniform(-0.1, 0.1), 4),
            "rate_type": "ESTR",
            "source": "ECB Data Portal (synthetic)",
        })
    return records


# ── Write to Data Lake ────────────────────────────────────────────────────────

def run():
    print(f"Generating sample data → {ROOT}  (ingestion_date={TODAY})")

    # DVF
    dvf_dir = ROOT / "raw" / "real_estate" / "dvf" / f"ingestion_date={TODAY}"
    dvf_dir.mkdir(parents=True, exist_ok=True)
    dvf_df = generate_dvf()
    dvf_path = dvf_dir / "dvf_2024.csv"
    dvf_df.to_csv(dvf_path, index=False)
    print(f"  DVF:   {len(dvf_df):,} rows → {dvf_path}")

    # Rents
    rents_dir = ROOT / "raw" / "real_estate" / "rents" / f"ingestion_date={TODAY}"
    rents_dir.mkdir(parents=True, exist_ok=True)
    rents_df = generate_rents()
    rents_path = rents_dir / "rents_2025.csv"
    rents_df.to_csv(rents_path, index=False)
    print(f"  Rents: {len(rents_df):,} rows → {rents_path}")

    # ECB rates
    ecb_dir = ROOT / "raw" / "real_estate" / "ecb_rates" / f"ingestion_date={TODAY}"
    ecb_dir.mkdir(parents=True, exist_ok=True)
    ecb_records = generate_ecb_rates()
    ecb_path = ecb_dir / "estr.json"
    with open(ecb_path, "w") as f:
        json.dump(ecb_records, f, indent=2)
    print(f"  ECB:   {len(ecb_records)} records → {ecb_path}")

    print("\nDone. Now run the Spark formatting + combination jobs, or trigger the Airflow DAG.")


if __name__ == "__main__":
    run()
