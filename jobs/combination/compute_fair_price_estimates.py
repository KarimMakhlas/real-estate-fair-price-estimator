"""
compute_fair_price_estimates.py
────────────────────────────────
Spark job – Compute fair price indicators and confidence scores

Reads the combined market dataset, applies the statistical fair-price logic
defined in the spec, and enriches the output with:
  - estimated_gross_yield
  - fair_price_confidence (LOW / MEDIUM / HIGH)

This job reads from and overwrites the same usage layer path, adding the
enriched columns in-place.

Input/Output:
    data/usage/real_estate/fair_price_estimates/computation_date=YYYY-MM-DD/

Usage:
    python jobs/combination/compute_fair_price_estimates.py

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
from pyspark.sql.types import StringType, DoubleType

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"


def _load_paths():
    with open(CONFIG_DIR / "paths.yml") as f:
        return yaml.safe_load(f)


def _get_spark(master: str) -> SparkSession:
    return (
        SparkSession.builder.master(master)
        .appName("compute_fair_price_estimates")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def run(computation_date: str | None = None) -> None:
    paths_cfg = _load_paths()
    root = os.getenv("DATALAKE_ROOT", paths_cfg.get("data_lake_root", "data"))
    master = os.getenv("SPARK_MASTER_URL", "local[*]")
    computation_date = computation_date or str(date.today())

    usage_path = str(Path(root) / "usage" / "real_estate" / "fair_price_estimates")

    spark = _get_spark(master)

    log.info("Reading combined market data from %s", usage_path)
    df = spark.read.parquet(usage_path)

    # ── Gross rental yield ────────────────────────────────────────────────────
    # estimated_gross_yield = (rent_m2 * 12) / median_price_m2
    df = df.withColumn(
        "estimated_gross_yield",
        F.when(
            F.col("median_price_m2").isNotNull() & (F.col("median_price_m2") > 0)
            & F.col("rent_m2").isNotNull(),
            F.round((F.col("rent_m2") * 12) / F.col("median_price_m2"), 4),
        ).otherwise(F.lit(None).cast(DoubleType())),
    )

    # ── Confidence based on transaction count ─────────────────────────────────
    df = df.withColumn(
        "fair_price_confidence",
        F.when(F.col("transaction_count") >= 100, "HIGH")
        .when(F.col("transaction_count") >= 30, "MEDIUM")
        .otherwise("LOW")
        .cast(StringType()),
    )

    # ── Select and reorder final usage schema ─────────────────────────────────
    final_cols = [
        "commune_code",
        "commune_name",
        "department_code",
        "property_type",
        "rooms_bucket",
        "transaction_count",
        "median_price_m2",
        "avg_price_m2",
        "q25_price_m2",
        "q75_price_m2",
        "rent_m2",
        "estimated_gross_yield",
        "latest_rate_value",
        "market_liquidity_score",
        "fair_price_confidence",
        "computation_date",
    ]
    # Only select columns that actually exist (defensive for partial pipelines)
    available = set(df.columns)
    df = df.select(*[c for c in final_cols if c in available])

    count = df.count()
    log.info("Fair price estimates ready: %d market segments", count)

    # Write to a temp path first (avoids read/write conflict on same directory)
    temp_path = usage_path + "_tmp"
    df.write.mode("overwrite").partitionBy("computation_date").parquet(temp_path)
    spark.stop()

    # Replace the original with the enriched version
    import shutil
    if Path(usage_path).exists():
        shutil.rmtree(usage_path)
    shutil.move(temp_path, usage_path)
    log.info("Fair price computation complete → %s", usage_path)


# ─────────────────────────────────────────────────────────────────────────────
# Standalone helper: classify a single property against the pre-computed refs
# ─────────────────────────────────────────────────────────────────────────────

def classify_property(
    commune_code: str,
    property_type: str,
    rooms_bucket: str,
    surface_m2: float,
    asked_price: float,
    root: str = "data",
    computation_date: str | None = None,
) -> dict:
    """
    Return a fair-price classification for a single property.

    Example usage:
        result = classify_property("44109", "Apartment", "3 rooms", 55, 250000)
        # {"label": "FAIRLY_PRICED", "confidence": "HIGH", ...}
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.master("local[*]").appName("classify_property").getOrCreate()
    computation_date = computation_date or str(date.today())
    usage_path = str(Path(root) / "usage" / "real_estate" / "fair_price_estimates")

    try:
        df = spark.read.parquet(usage_path)
        row = (
            df.filter(
                (F.col("commune_code") == commune_code)
                & (F.col("property_type") == property_type)
                & (F.col("rooms_bucket") == rooms_bucket)
            )
            .orderBy(F.col("computation_date").desc())
            .first()
        )
    finally:
        spark.stop()

    if not row:
        return {"error": f"No market reference found for {commune_code}/{property_type}/{rooms_bucket}"}

    q25 = row["q25_price_m2"]
    q75 = row["q75_price_m2"]
    median = row["median_price_m2"]

    low_fair = q25 * surface_m2
    high_fair = q75 * surface_m2
    estimated_fair = median * surface_m2

    if asked_price < low_fair:
        label = "UNDERPRICED"
    elif asked_price > high_fair:
        label = "OVERPRICED"
    else:
        label = "FAIRLY_PRICED"

    return {
        "commune_code": commune_code,
        "property_type": property_type,
        "rooms_bucket": rooms_bucket,
        "surface_m2": surface_m2,
        "asked_price": asked_price,
        "estimated_fair_price": round(estimated_fair, 0),
        "low_fair_price": round(low_fair, 0),
        "high_fair_price": round(high_fair, 0),
        "label": label,
        "confidence": row["fair_price_confidence"],
        "transaction_count": row["transaction_count"],
        "median_price_m2": median,
        "estimated_gross_yield": row.get("estimated_gross_yield"),
        "latest_rate_value": row.get("latest_rate_value"),
    }


if __name__ == "__main__":
    run()
