"""
real_estate_fair_price_dag.py
──────────────────────────────
Airflow DAG – Real Estate Fair Price Estimator

Orchestrates the full data pipeline:

  extract_dvf ──────────────┐
  extract_rents ────────────┤──► format_* ──► combine ──► compute ──► index
  extract_ecb_rates ────────┘

Schedule: daily at 06:00 UTC  (ECB data refreshes on business days)
Catchup:  False               (run from today onwards)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Default args ──────────────────────────────────────────────────────────────

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="real_estate_fair_price_dag",
    description="Real Estate Fair Price Estimator – end-to-end Data Lake pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 6 * * *",
    catchup=False,
    tags=["real_estate", "data_lake", "spark", "elasticsearch"],
) as dag:

    # ── Import job runners lazily so the DAG parses even if deps aren't ready ──

    def _run_extract_dvf(**ctx):
        from jobs.ingestion.extract_dvf import run
        run(ingestion_date=ctx["ds"])

    def _run_extract_rents(**ctx):
        from jobs.ingestion.extract_rents import run
        run(ingestion_date=ctx["ds"])

    def _run_extract_ecb_rates(**ctx):
        from jobs.ingestion.extract_ecb_rates import run
        run(ingestion_date=ctx["ds"])

    def _run_format_dvf(**ctx):
        from jobs.formatting.format_dvf_spark import run
        run()

    def _run_format_rents(**ctx):
        from jobs.formatting.format_rents_spark import run
        run()

    def _run_format_ecb_rates(**ctx):
        from jobs.formatting.format_ecb_rates_spark import run
        run(ingestion_date=ctx["ds"])

    def _run_combine(**ctx):
        from jobs.combination.combine_market_data_spark import run
        run(computation_date=ctx["ds"])

    def _run_compute(**ctx):
        from jobs.combination.compute_fair_price_estimates import run
        run(computation_date=ctx["ds"])

    def _run_index(**ctx):
        from jobs.indexing.index_to_elasticsearch import run
        run(computation_date=ctx["ds"])

    # ── Tasks ─────────────────────────────────────────────────────────────────

    extract_dvf = PythonOperator(
        task_id="extract_dvf",
        python_callable=_run_extract_dvf,
    )

    extract_rents = PythonOperator(
        task_id="extract_rents",
        python_callable=_run_extract_rents,
    )

    extract_ecb_rates = PythonOperator(
        task_id="extract_ecb_rates",
        python_callable=_run_extract_ecb_rates,
    )

    format_dvf = PythonOperator(
        task_id="format_dvf_with_spark",
        python_callable=_run_format_dvf,
    )

    format_rents = PythonOperator(
        task_id="format_rents_with_spark",
        python_callable=_run_format_rents,
    )

    format_ecb_rates = PythonOperator(
        task_id="format_ecb_rates_with_spark",
        python_callable=_run_format_ecb_rates,
    )

    combine = PythonOperator(
        task_id="combine_market_data_with_spark",
        python_callable=_run_combine,
    )

    compute = PythonOperator(
        task_id="compute_fair_price_estimates",
        python_callable=_run_compute,
    )

    index = PythonOperator(
        task_id="index_to_elasticsearch",
        python_callable=_run_index,
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    #
    #   extract_dvf        ──► format_dvf        ──┐
    #   extract_rents      ──► format_rents      ──┼──► combine ──► compute ──► index
    #   extract_ecb_rates  ──► format_ecb_rates  ──┘

    extract_dvf >> format_dvf
    extract_rents >> format_rents
    extract_ecb_rates >> format_ecb_rates

    [format_dvf, format_rents, format_ecb_rates] >> combine >> compute >> index
