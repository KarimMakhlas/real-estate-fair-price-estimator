"""
format_dvf_spark.py
───────────────────
Spark formatting job – DVF property transactions

Reads raw DVF CSVs from the raw layer, cleans and normalises them,
computes price_per_m2 and rooms_bucket, then writes clean Parquet
files to the formatted layer partitioned by year.

Input:  data/raw/real_estate/dvf/
Output: data/formatted/real_estate/sales/year=YYYY/

Usage (standalone / spark-submit):
    python jobs/formatting/format_dvf_spark.py

Environment variables:
    DATALAKE_ROOT   – data lake root  (default: "data")
    SPARK_MASTER_URL – Spark master   (default: "local[*]")
"""

import os
import logging
from pathlib import Path

import yaml
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"


def _load_paths():
    with open(CONFIG_DIR / "paths.yml") as f:
        return yaml.safe_load(f)


def _get_spark(master: str) -> SparkSession:
    return (
        SparkSession.builder.master(master)
        .appName("format_dvf")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def _rooms_bucket(col):
    """Categorise room count into a human-readable bucket."""
    return (
        F.when(col == 1, "1 room")
        .when(col == 2, "2 rooms")
        .when(col == 3, "3 rooms")
        .when(col == 4, "4 rooms")
        .when(col >= 5, "5+ rooms")
        .otherwise("unknown")
    )


def run() -> None:
    paths = _load_paths()
    root = os.getenv("DATALAKE_ROOT", paths.get("data_lake_root", "data"))
    master = os.getenv("SPARK_MASTER_URL", "local[*]")

    raw_path = str(Path(root) / "raw" / "real_estate" / "dvf")
    out_path = str(Path(root) / "formatted" / "real_estate" / "sales")

    spark = _get_spark(master)
    log.info("Reading raw DVF files from %s", raw_path)

    # Read all CSVs under the raw path (wildcard picks up ingestion-date partitions)
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .csv(f"{raw_path}/**/*.csv")
    )

    if df.rdd.isEmpty():
        log.warning("No DVF raw data found — nothing to format.")
        spark.stop()
        return

    log.info("Loaded %d raw rows", df.count())

    # ── Rename raw French column names ────────────────────────────────────────
    rename_map = {
        "date_mutation": "sale_date_raw",
        "valeur_fonciere": "sale_price_raw",
        "type_local": "property_type_raw",
        "surface_reelle_bati": "surface_m2_raw",
        "nombre_pieces_principales": "rooms_raw",
        "code_commune": "commune_code",
        "nom_commune": "commune_name",
        "code_departement": "department_code",
    }
    for old, new in rename_map.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)

    # ── Cast to correct types ─────────────────────────────────────────────────
    df = (
        df.withColumn("sale_date", F.to_date(F.col("sale_date_raw"), "yyyy-MM-dd"))
        .withColumn("sale_price", F.regexp_replace("sale_price_raw", ",", ".").cast(DoubleType()))
        .withColumn("surface_m2", F.regexp_replace("surface_m2_raw", ",", ".").cast(DoubleType()))
        .withColumn("rooms", F.col("rooms_raw").cast(IntegerType()))
        .withColumn("commune_code", F.lpad(F.col("commune_code").cast(StringType()), 5, "0"))
        .withColumn("department_code", F.col("department_code").cast(StringType()))
    )

    # ── Data quality filters ──────────────────────────────────────────────────
    df = (
        df.filter(F.col("sale_price").isNotNull() & (F.col("sale_price") > 0))
        .filter(F.col("surface_m2").isNotNull() & (F.col("surface_m2") > 0))
        .filter(F.col("sale_date").isNotNull())
        .filter(
            F.lower(F.col("property_type_raw")).isin("appartement", "maison")
        )
    )

    # ── Normalise property type ───────────────────────────────────────────────
    df = df.withColumn(
        "property_type",
        F.when(F.lower(F.col("property_type_raw")) == "appartement", "Apartment")
        .when(F.lower(F.col("property_type_raw")) == "maison", "House")
        .otherwise("Other"),
    )

    # ── Derived columns ───────────────────────────────────────────────────────
    df = (
        df.withColumn("year", F.year("sale_date"))
        .withColumn("price_per_m2", F.round(F.col("sale_price") / F.col("surface_m2"), 2))
        .withColumn("rooms_bucket", _rooms_bucket(F.col("rooms")))
        .withColumn("sale_id", F.sha2(F.concat_ws("|", "sale_date", "commune_code", "sale_price", "surface_m2"), 256))
    )

    # ── Select final schema ───────────────────────────────────────────────────
    df = df.select(
        "sale_id",
        "sale_date",
        "year",
        "commune_code",
        "commune_name",
        "department_code",
        "property_type",
        "surface_m2",
        "rooms",
        "rooms_bucket",
        "sale_price",
        "price_per_m2",
    )

    log.info("Writing %d formatted sales rows to %s", df.count(), out_path)

    df.write.mode("overwrite").partitionBy("year").parquet(out_path)
    log.info("DVF formatting complete → %s", out_path)
    spark.stop()


if __name__ == "__main__":
    run()
