# 🏠 Real Estate Fair Price Estimator

A Big Data pipeline that tells you whether a French property is **underpriced, fairly priced, or overpriced** — built on a proper Data Lake architecture.

🌐 **Live demo:** [ismyhouseexpensive.netlify.app](https://ismyhouseexpensive.netlify.app)  
📦 **GitHub:** [github.com/KarimMakhlas/real-estate-fair-price-estimator](https://github.com/KarimMakhlas/real-estate-fair-price-estimator)

---

## What does it do?

You give it a property (city, type, surface, price) and it compares it against thousands of historical transactions to answer:

> "Is this apartment in Nantes at 250,000 € a good deal?"

```
Estimated fair price : 220,000 € – 258,500 €
Asked price          : 250,000 €
Verdict              : ✅ FAIRLY PRICED
Confidence           : HIGH  (1,250 transactions)
Rental yield         : 4.4%
```

---

## Tech stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow |
| Transformation | Apache Spark (PySpark) |
| Storage | Local filesystem (Parquet) |
| Search | Elasticsearch |
| Dashboard | Kibana + custom FastAPI UI |

---

## Before you start

You need:
- **Docker Desktop** installed and running — [download here](https://www.docker.com/products/docker-desktop/)
- About **4 GB of free RAM** for all the containers
- That's it. Everything else runs inside Docker.

---

## Getting started

### Step 1 — Clone and enter the project

```bash
cd real-estate-fair-price-estimator
cp .env.example .env
```

### Step 2 — Build and start everything

The first time this will take 5–10 minutes because Docker needs to build the custom image (it installs Java and PySpark inside).

```bash
docker compose up airflow-init
```

Wait for it to print `User "admin" created` and exit. Then:

```bash
docker compose up -d
```

### Step 3 — Check everything is running

```bash
docker compose ps
```

You should see all services as `Up` or `healthy`. Give it a minute if some are still starting.

### Step 4 — Generate sample data and run the pipeline

We use synthetic data so you don't have to wait for large real downloads:

```bash
docker compose exec airflow-webserver bash -c "cd /opt/airflow && python scripts/generate_sample_data.py"
```

Then run the full pipeline (Spark formatting → combination → Elasticsearch indexing):

```bash
docker compose exec airflow-webserver bash -c "
  export JAVA_HOME=/usr/lib/jvm/default-java &&
  cd /opt/airflow &&
  python jobs/formatting/format_dvf_spark.py &&
  python jobs/formatting/format_rents_spark.py &&
  python jobs/formatting/format_ecb_rates_spark.py &&
  python jobs/combination/combine_market_data_spark.py &&
  python jobs/combination/compute_fair_price_estimates.py &&
  python jobs/indexing/index_to_elasticsearch.py
"
```

This takes 2–3 minutes. You'll see Spark logs, which is normal.

### Step 5 — Open the UI

Go to **http://localhost:8000** — this is the main interface.

Select a city, property type, number of rooms, and enter a surface and price. Hit **Estimate Fair Price** and you'll get the verdict instantly.

---

## All the URLs

| Service | URL | Login |
|---|---|---|
| **Fair Price UI** | http://localhost:8000 | — |
| **API docs** | http://localhost:8000/docs | — |
| **Airflow** | http://localhost:8080 | admin / admin |
| **Kibana** | http://localhost:5601 | — |
| **Elasticsearch** | http://localhost:9200 | — |

---

## Running with real data (optional)

The pipeline can also download real French property data instead of sample data. In the Airflow UI (http://localhost:8080), find the DAG called `real_estate_fair_price_dag`, enable it, and click the play button. It will download DVF data for Île-de-France automatically.

> ⚠️ Real DVF files are large (several hundred MB). The sample data is fine for testing and demo purposes.

---

## Running the tests

```bash
docker compose exec airflow-webserver bash -c "cd /opt/airflow && python -m pytest tests/ -v"
```

---

## Project structure

```
real-estate-fair-price-estimator/
│
├── Dockerfile                    ← Custom image with Java + PySpark
├── docker-compose.yml            ← All services
├── requirements.txt
│
├── dags/
│   └── real_estate_fair_price_dag.py   ← Airflow DAG
│
├── jobs/
│   ├── ingestion/
│   │   ├── extract_dvf.py              ← Download DVF transactions
│   │   ├── extract_rents.py            ← Download rent indicators
│   │   └── extract_ecb_rates.py        ← Download ECB interest rates
│   │
│   ├── formatting/
│   │   ├── format_dvf_spark.py         ← Clean + normalise with Spark
│   │   ├── format_rents_spark.py
│   │   └── format_ecb_rates_spark.py
│   │
│   ├── combination/
│   │   ├── combine_market_data_spark.py     ← Join all sources
│   │   └── compute_fair_price_estimates.py  ← Compute fair price logic
│   │
│   └── indexing/
│       └── index_to_elasticsearch.py   ← Push to Elasticsearch
│
├── api/
│   ├── main.py                   ← FastAPI app
│   └── templates/
│       └── index.html            ← The UI
│
├── scripts/
│   └── generate_sample_data.py   ← Creates synthetic test data
│
├── config/
│   ├── paths.yml                 ← Data Lake paths
│   ├── sources.yml               ← Source URLs and settings
│   └── elasticsearch.yml         ← ES index config
│
├── data/                         ← Data Lake (created automatically)
│   ├── raw/
│   ├── formatted/
│   └── usage/
│
└── tests/
    ├── test_price_calculation.py
    └── test_data_quality.py
```

---

## How the fair price logic works

For each market segment (city + property type + number of rooms):

1. Collect all historical sale prices per m²
2. Compute: median, Q25 (bottom 25%), Q75 (top 25%)
3. Fair price range = `[Q25 × surface, Q75 × surface]`

Then:
- `asked_price < Q25 × surface` → **UNDERPRICED**
- `Q25 × surface ≤ asked_price ≤ Q75 × surface` → **FAIRLY PRICED**
- `asked_price > Q75 × surface` → **OVERPRICED**

Confidence is based on transaction count: `< 30` → LOW, `30–99` → MEDIUM, `≥ 100` → HIGH.

---

## Stopping everything

```bash
docker compose down
```

To also delete all stored data:

```bash
docker compose down -v
```
