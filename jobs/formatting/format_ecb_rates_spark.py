"""
format_ecb_rates_spark.py
──────────────────────────
Spark formatting job – ECB interest rate data

Reads the raw ESTR JSON files, parses and cleans them, and writes clean
Parquet files partitioned by date.

Input:  data/raw/real_estate/ecb_rates/
Output: data/formatted/real_estate/rates/date=YYYY-MM-DD/

Usage:
    python jobs/formatting/format_ecb_rates_spark.py

Environment variables:
    DATALAKE_ROOT    – data lake root  (default: "data")
    SPARK_MASTER_URL – Spark master    (default: "local[*]")
"""

import os
import logging
from datetime import date
from pathlib import Path

import yaml
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, DateType, StringType, StructField, StructType

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"

_RAW_SCHEMA = StructType(
    [
        StructField("date", StringType(), True),
        StructField("rate_value", DoubleType(), True),
        StructField("rate_type", StringType(), True),
        StructField("source", StringType(), True),
    ]
)


def _load_paths():
    with open(CONFIG_DIR / "paths.yml") as f:
        return yaml.safe_load(f)


def _get_spark(master: str) -> SparkSession:
    return (
        SparkSession.builder.master(master)
        .appName("format_ecb_rates")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def run(ingestion_date: str | None = None) -> None:
    paths_cfg = _load_paths()
    root = os.getenv("DATALAKE_ROOT", paths_cfg.get("data_lake_root", "data"))
    master = os.getenv("SPARK_MASTER_URL", "local[*]")
    ingestion_date = ingestion_date or str(date.today())

    raw_path = str(Path(root) / "raw" / "real_estate" / "ecb_rates")
    out_path = str(Path(root) / "formatted" / "real_estate" / "rates")

    spark = _get_spark(master)
    log.info("Reading raw ECB rate files from %s", raw_path)

    df = (
        spark.read.option("multiLine", "true")
        .schema(_RAW_SCHEMA)
        .json(f"{raw_path}/**/*.json")
    )

    if df.rdd.isEmpty():
        log.warning("No ECB rate raw data found — nothing to format.")
        spark.stop()
        return

    log.info("Loaded %d raw rate records", df.count())

    df = (
        df.withColumn("rate_date", F.to_date(F.col("date"), "yyyy-MM-dd"))
        .withColumn("rate_value", F.col("rate_value").cast(DoubleType()))
        .withColumn("ingestion_date", F.lit(ingestion_date).cast(DateType()))
        .filter(F.col("rate_date").isNotNull())
        .filter(F.col("rate_value").isNotNull())
        .drop("date")
    )

    # Keep only the latest value per rate_type (for the usage join)
    window = (
        __import__("pyspark.sql.window", fromlist=["Window"])
        .Window.partitionBy("rate_type")
        .orderBy(F.col("rate_date").desc())
    )
    df = df.withColumn("_rank", F.row_number().over(window)).filter(F.col("_rank") == 1).drop("_rank")

    df = df.select("rate_date", "rate_value", "rate_type", "ingestion_date")

    log.info("Writing %d formatted rate rows to %s", df.count(), out_path)
    df.write.mode("overwrite").partitionBy("rate_date").parquet(out_path)
    log.info("ECB rates formatting complete → %s", out_path)
    spark.stop()


if __name__ == "__main__":
    run()
