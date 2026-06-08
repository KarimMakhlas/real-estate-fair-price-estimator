#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# run_pipeline_local.sh
# Runs the full pipeline locally (no Airflow, no Docker needed for the jobs).
# Requires: Python 3.11+, pyspark, pandas, pyarrow, elasticsearch, pyyaml
#
# Usage (from the project root):
#   bash scripts/run_pipeline_local.sh
#
# Options:
#   --sample   Generate synthetic data instead of downloading real sources
# ─────────────────────────────────────────────────────────────────────────────

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

export DATALAKE_ROOT="$PROJECT_ROOT/data"
export SPARK_MASTER_URL="local[*]"
export ELASTICSEARCH_HOST="${ELASTICSEARCH_HOST:-localhost}"
export PYTHONPATH="$PROJECT_ROOT"

cd "$PROJECT_ROOT"

USE_SAMPLE=false
for arg in "$@"; do
  [[ "$arg" == "--sample" ]] && USE_SAMPLE=true
done

echo "════════════════════════════════════════════"
echo " Real Estate Fair Price — Local Pipeline Run"
echo "════════════════════════════════════════════"
echo "DATALAKE_ROOT : $DATALAKE_ROOT"
echo "ELASTICSEARCH : $ELASTICSEARCH_HOST:9200"
echo ""

# ── Step 0: Ingestion (or sample data) ───────────────────────────────────────
if $USE_SAMPLE; then
  echo "[0/6] Generating synthetic sample data…"
  python scripts/generate_sample_data.py
else
  echo "[1/6] Ingesting DVF…"
  python jobs/ingestion/extract_dvf.py

  echo "[2/6] Ingesting rent indicators…"
  python jobs/ingestion/extract_rents.py

  echo "[3/6] Ingesting ECB rates…"
  python jobs/ingestion/extract_ecb_rates.py
fi

# ── Step 1-3: Spark formatting ────────────────────────────────────────────────
echo "[4/6] Formatting DVF with Spark…"
python jobs/formatting/format_dvf_spark.py

echo "[4/6] Formatting rents with Spark…"
python jobs/formatting/format_rents_spark.py

echo "[4/6] Formatting ECB rates with Spark…"
python jobs/formatting/format_ecb_rates_spark.py

# ── Step 4: Combination ───────────────────────────────────────────────────────
echo "[5/6] Combining market data…"
python jobs/combination/combine_market_data_spark.py

echo "[5/6] Computing fair price estimates…"
python jobs/combination/compute_fair_price_estimates.py

# ── Step 5: Index ─────────────────────────────────────────────────────────────
echo "[6/6] Indexing to Elasticsearch…"
python jobs/indexing/index_to_elasticsearch.py

echo ""
echo "✓ Pipeline complete."
echo "  → Browse results in Kibana: http://localhost:5601"
echo "  → Query the API:            http://localhost:8000/docs"
