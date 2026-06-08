"""
combine_market_data_spark.py
────────────────────────────
Spark combination job – Join sales, rents, and rates

Reads the three formatted datasets, groups sales by market segment
(commune_code + property_type + rooms_bucket), computes market statistics,
joins rent indicators and the latest ECB rate, then writes the combined
dataset to the usage layer.

Input:
    data/formatted/real_estate/sales/
    data/formatted/real_estate/rents/
    data/formatted/real_estate/rates/

Output:
    data/usage/real_estate/fair_price_estimates/computation_date=YYYY-MM-DD/

Usage:
    python jobs/combination/combine_market_data_spark.py

Environment variables:
    DATALAKE_ROOT    – data lake root  (default: "data")
    SPARK_MASTER_URL – Spark master    (default: "local[*]")
"""

import os
import logging
import math
from datetime import date
from pathlib import Path

import yaml
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"


def _load_paths():
    with open(CONFIG_DIR / "paths.yml") as f:
        return yaml.safe_load(f)


def _get_spark(master: str) -> SparkSession:
    return (
        SparkSession.builder.master(master)
        .appName("combine_market_data")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def run(computation_date: str | None = None) -> None:
    paths_cfg = _load_paths()
    root = os.getenv("DATALAKE_ROOT", paths_cfg.get("data_lake_root", "data"))
    master = os.getenv("SPARK_MASTER_URL", "local[*]")
    computation_date = computation_date or str(date.today())

    sales_path = str(Path(root) / "formatted" / "real_estate" / "sales")
    rents_path = str(Path(root) / "formatted" / "real_estate" / "rents")
    rates_path = str(Path(root) / "formatted" / "real_estate" / "rates")
    out_path = str(Path(root) / "usage" / "real_estate" / "fair_price_estimates")

    spark = _get_spark(master)

    # ── Read formatted datasets ───────────────────────────────────────────────
    log.info("Reading formatted sales from %s", sales_path)
    sales = spark.read.parquet(sales_path)

    log.info("Reading formatted rents from %s", rents_path)
    rents = spark.read.parquet(rents_path)

    log.info("Reading formatted rates from %s", rates_path)
    rates = spark.read.parquet(rates_path)

    # ── Latest ECB rate (single scalar) ──────────────────────────────────────
    latest_rate_row = rates.orderBy(F.col("rate_date").desc()).first()
    latest_rate_value = float(latest_rate_row["rate_value"]) if latest_rate_row else None
    log.info("Latest ECB rate: %s", latest_rate_value)

    # ── Aggregate sales by market segment ─────────────────────────────────────
    segment_keys = ["commune_code", "commune_name", "department_code", "property_type", "rooms_bucket"]

    agg = (
        sales.groupBy(*segment_keys)
        .agg(
            F.count("sale_id").alias("transaction_count"),
            F.round(F.percentile_approx("price_per_m2", 0.5), 2).alias("median_price_m2"),
            F.round(F.avg("price_per_m2"), 2).alias("avg_price_m2"),
            F.round(F.percentile_approx("price_per_m2", 0.25), 2).alias("q25_price_m2"),
            F.round(F.percentile_approx("price_per_m2", 0.75), 2).alias("q75_price_m2"),
        )
    )

    # ── Join rent indicators ───────────────────────────────────────────────────
    # Use the most recent rent data available (one row per commune after dedup)
    window_rent = Window.partitionBy("commune_code").orderBy(F.col("source_year").desc())
    rents_dedup = (
        rents.withColumn("_rank", F.row_number().over(window_rent))
        .filter(F.col("_rank") == 1)
        .drop("_rank", "source_year")
    )

    # Select the appropriate rent column based on property_type
    rents_apt = rents_dedup.select(
        F.col("commune_code"),
        F.col("rent_m2_apartment").alias("rent_m2_apt"),
        F.col("rent_m2_house").alias("rent_m2_house"),
    )

    combined = agg.join(rents_apt, on="commune_code", how="left")

    combined = combined.withColumn(
        "rent_m2",
        F.when(F.col("property_type") == "Apartment", F.col("rent_m2_apt"))
        .when(F.col("property_type") == "House", F.col("rent_m2_house"))
        .otherwise(F.col("rent_m2_apt")),
    ).drop("rent_m2_apt", "rent_m2_house")

    # ── Add ECB rate ───────────────────────────────────────────────────────────
    combined = combined.withColumn("latest_rate_value", F.lit(latest_rate_value))

    # ── Market liquidity score (0–1 based on transaction count) ───────────────
    # log(count) / log(1000) capped at 1.0
    combined = combined.withColumn(
        "market_liquidity_score",
        F.round(
            F.least(
                F.lit(1.0),
                F.log(F.col("transaction_count").cast("double") + 1) / math.log(1000),
            ),
            4,
        ),
    )

    # ── Computation date ───────────────────────────────────────────────────────
    combined = combined.withColumn("computation_date", F.lit(computation_date).cast("date"))

    log.info("Combined dataset: %d market segments", combined.count())
    log.info("Writing to %s", out_path)

    combined.write.mode("overwrite").partitionBy("computation_date").parquet(out_path)
    log.info("Market data combination complete → %s", out_path)
    spark.stop()


if __name__ == "__main__":
    run()
