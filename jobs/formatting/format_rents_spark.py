"""
format_rents_spark.py
──────────────────────
Spark formatting job – Rent indicators by commune

Reads raw rent CSV files, normalises column names and types, and writes
clean Parquet files partitioned by source_year.

Input:  data/raw/real_estate/rents/
Output: data/formatted/real_estate/rents/source_year=YYYY/

Usage:
    python jobs/formatting/format_rents_spark.py

Environment variables:
    DATALAKE_ROOT    – data lake root  (default: "data")
    SPARK_MASTER_URL – Spark master    (default: "local[*]")
"""

import os
import logging
from pathlib import Path

import yaml
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, IntegerType

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"


def _load_config():
    with open(CONFIG_DIR / "paths.yml") as f:
        paths = yaml.safe_load(f)
    with open(CONFIG_DIR / "sources.yml") as f:
        sources = yaml.safe_load(f)
    return paths, sources


def _get_spark(master: str) -> SparkSession:
    return (
        SparkSession.builder.master(master)
        .appName("format_rents")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def run() -> None:
    paths_cfg, sources_cfg = _load_config()
    root = os.getenv("DATALAKE_ROOT", paths_cfg.get("data_lake_root", "data"))
    master = os.getenv("SPARK_MASTER_URL", "local[*]")
    source_year = sources_cfg["rents"]["source_year"]

    raw_path = str(Path(root) / "raw" / "real_estate" / "rents")
    out_path = str(Path(root) / "formatted" / "real_estate" / "rents")

    spark = _get_spark(master)
    log.info("Reading raw rent files from %s", raw_path)

    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .csv(f"{raw_path}/**/*.csv")
    )

    if df.rdd.isEmpty():
        log.warning("No rent raw data found — nothing to format.")
        spark.stop()
        return

    log.info("Loaded %d raw rows, columns: %s", df.count(), df.columns)

    # ── Normalise column names (lowercase + strip) ────────────────────────────
    df = df.toDF(*[c.lower().strip() for c in df.columns])

    # ── Flexible column mapping ───────────────────────────────────────────────
    # The source may use slightly different names; map to our standard schema.
    possible_maps = {
        "commune_code": ["code_commune", "codecommune", "commune_code"],
        "commune_name": ["nom_commune", "libelle_commune", "commune_name"],
        "rent_m2_apartment": ["loyer_m2_appartement", "loyer_appartement_m2", "rent_m2_apartment"],
        "rent_m2_house": ["loyer_m2_maison", "loyer_maison_m2", "rent_m2_house"],
    }

    existing_cols = set(df.columns)
    for target, candidates in possible_maps.items():
        for cand in candidates:
            if cand in existing_cols and cand != target:
                df = df.withColumnRenamed(cand, target)
                break

    # ── Cast types ────────────────────────────────────────────────────────────
    df = (
        df.withColumn("commune_code", F.lpad(F.col("commune_code").cast("string"), 5, "0"))
        .withColumn(
            "rent_m2_apartment",
            F.regexp_replace(F.col("rent_m2_apartment").cast("string"), ",", ".").cast(DoubleType()),
        )
        .withColumn(
            "rent_m2_house",
            F.regexp_replace(F.col("rent_m2_house").cast("string"), ",", ".").cast(DoubleType()),
        )
        .withColumn("source_year", F.lit(source_year).cast(IntegerType()))
    )

    # ── Data quality filters ──────────────────────────────────────────────────
    df = df.filter(F.col("commune_code").isNotNull()).filter(
        F.col("rent_m2_apartment").isNotNull() | F.col("rent_m2_house").isNotNull()
    )

    # ── Select final schema ───────────────────────────────────────────────────
    available = set(df.columns)
    select_cols = [c for c in ["commune_code", "commune_name", "rent_m2_apartment", "rent_m2_house", "source_year"] if c in available]
    df = df.select(*select_cols)

    log.info("Writing %d formatted rent rows to %s", df.count(), out_path)
    df.write.mode("overwrite").partitionBy("source_year").parquet(out_path)
    log.info("Rent formatting complete → %s", out_path)
    spark.stop()


if __name__ == "__main__":
    run()
