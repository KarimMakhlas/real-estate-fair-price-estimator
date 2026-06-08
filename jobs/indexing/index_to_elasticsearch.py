"""
index_to_elasticsearch.py
──────────────────────────
Indexing job – Elasticsearch

Reads the final usage Parquet dataset and bulk-indexes it into Elasticsearch.

Creates the index with the correct mappings if it doesn't already exist,
then indexes all records in configurable batches.

Input:  data/usage/real_estate/fair_price_estimates/
Output: Elasticsearch index – real_estate_fair_price_estimates

Usage:
    python jobs/indexing/index_to_elasticsearch.py

Environment variables:
    DATALAKE_ROOT       – data lake root          (default: "data")
    ELASTICSEARCH_HOST  – ES host                  (default: "localhost")
    ELASTICSEARCH_PORT  – ES port                  (default: 9200)
"""

import json
import logging
import os
from datetime import date
from pathlib import Path

import pandas as pd
import yaml
from elasticsearch import Elasticsearch, helpers

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"


def _load_config():
    with open(CONFIG_DIR / "paths.yml") as f:
        paths = yaml.safe_load(f)
    with open(CONFIG_DIR / "elasticsearch.yml") as f:
        es_cfg = yaml.safe_load(f)
    return paths, es_cfg


def _get_es_client(host: str, port: int) -> Elasticsearch:
    return Elasticsearch(
        f"http://{host}:{port}",
        request_timeout=30,
        max_retries=3,
        retry_on_timeout=True,
    )


def _create_index_if_missing(es: Elasticsearch, index: str, mappings: dict) -> None:
    if not es.indices.exists(index=index):
        log.info("Creating Elasticsearch index: %s", index)
        es.indices.create(
            index=index,
            body={
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0,
                },
                "mappings": mappings,
            },
        )
        log.info("Index '%s' created.", index)
    else:
        log.info("Index '%s' already exists.", index)


def _iter_actions(df: pd.DataFrame, index: str):
    """Generate bulk action dicts from a DataFrame."""
    for _, row in df.iterrows():
        doc = row.dropna().to_dict()
        # Convert date objects to ISO strings
        for k, v in doc.items():
            if isinstance(v, (pd.Timestamp, date)):
                doc[k] = str(v)[:10]
            elif hasattr(v, "item"):
                # numpy scalars → Python native
                doc[k] = v.item()
        yield {
            "_index": index,
            "_source": doc,
        }


def run(computation_date: str | None = None) -> None:
    paths_cfg, es_cfg = _load_config()
    root = os.getenv("DATALAKE_ROOT", paths_cfg.get("data_lake_root", "data"))
    es_host = os.getenv("ELASTICSEARCH_HOST", es_cfg.get("host", "localhost"))
    # Resolve env-var placeholder in YAML if present
    if es_host.startswith("${"):
        es_host = "localhost"
    es_port = int(os.getenv("ELASTICSEARCH_PORT", es_cfg.get("port", 9200)))
    index_name = es_cfg.get("index", "real_estate_fair_price_estimates")
    batch_size = es_cfg.get("batch_size", 1000)
    mappings = es_cfg.get("mappings", {})

    usage_path = Path(root) / "usage" / "real_estate" / "fair_price_estimates"

    # ── Read Parquet files (pure pandas for the indexing step — no Spark needed)
    parquet_files = list(usage_path.rglob("*.parquet"))
    if not parquet_files:
        log.error("No Parquet files found under %s — run the combination jobs first.", usage_path)
        return

    log.info("Reading %d Parquet file(s) from %s", len(parquet_files), usage_path)
    df = pd.concat([pd.read_parquet(p) for p in parquet_files], ignore_index=True)
    log.info("Loaded %d records to index", len(df))

    # ── Connect to Elasticsearch ───────────────────────────────────────────────
    log.info("Connecting to Elasticsearch at %s:%d", es_host, es_port)
    es = _get_es_client(es_host, es_port)

    try:
        info = es.info()
        log.info("Connected: Elasticsearch %s", info["version"]["number"])
    except Exception as exc:
        log.error("Cannot connect to Elasticsearch: %s", exc)
        raise

    _create_index_if_missing(es, index_name, mappings)

    # ── Bulk index ─────────────────────────────────────────────────────────────
    log.info("Bulk indexing into '%s' (batch_size=%d)…", index_name, batch_size)
    success, errors = helpers.bulk(
        es,
        _iter_actions(df, index_name),
        chunk_size=batch_size,
        raise_on_error=False,
    )
    log.info("Indexed %d documents; %d errors", success, len(errors) if errors else 0)
    if errors:
        log.warning("First error sample: %s", errors[0])

    # Refresh so Kibana can query immediately
    es.indices.refresh(index=index_name)
    log.info("Elasticsearch indexing complete → index '%s'", index_name)


if __name__ == "__main__":
    run()
