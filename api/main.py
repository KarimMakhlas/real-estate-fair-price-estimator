"""
api/main.py
────────────
FastAPI – Real Estate Fair Price Estimator

Endpoints:
  GET  /health                          – liveness check
  GET  /communes                        – list all communes with data
  GET  /market/{commune_code}           – full market profile for a commune
  POST /estimate                        – classify a property as UNDERPRICED /
                                          FAIRLY_PRICED / OVERPRICED

Usage:
  uvicorn api.main:app --reload --port 8000

Docs:
  http://localhost:8000/docs   (Swagger UI)
  http://localhost:8000/redoc
"""

import os
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

# ── Config ────────────────────────────────────────────────────────────────────

DATALAKE_ROOT = Path(os.getenv("DATALAKE_ROOT", "data"))
USAGE_PATH = DATALAKE_ROOT / "usage" / "real_estate" / "fair_price_estimates"
# Pre-exported JSON fallback (used on Render where parquet pipeline doesn't run)
DATA_JSON = Path(__file__).parent / "data.json"

TEMPLATES_DIR = Path(__file__).parent / "templates"

app = FastAPI(
    title="Real Estate Fair Price Estimator",
    description="Estimates whether a French property is underpriced, fairly priced, or overpriced.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def index():
    html = (TEMPLATES_DIR / "index.html").read_text()
    return HTMLResponse(content=html)

# ── In-memory cache ───────────────────────────────────────────────────────────

_cache: Optional[pd.DataFrame] = None


def _load_estimates() -> pd.DataFrame:
    global _cache
    if _cache is not None:
        return _cache

    # Prefer parquet (local pipeline), fall back to exported JSON (Render deploy)
    if USAGE_PATH.exists() and list(USAGE_PATH.rglob("*.parquet")):
        _cache = pd.read_parquet(str(USAGE_PATH))
    elif DATA_JSON.exists():
        _cache = pd.read_json(str(DATA_JSON))
        _cache["commune_code"] = _cache["commune_code"].astype(str)
    else:
        raise HTTPException(
            status_code=503,
            detail="No usage data found. Run the pipeline first.",
        )
    return _cache


def _refresh_cache():
    """Force reload from disk (call after pipeline re-runs)."""
    global _cache
    _cache = None


# ── Request / Response models ─────────────────────────────────────────────────

class EstimateRequest(BaseModel):
    commune_code: str = Field(..., example="44109", description="INSEE commune code (5 digits)")
    property_type: str = Field(..., example="Apartment", description="'Apartment' or 'House'")
    rooms_bucket: str = Field(..., example="3 rooms", description="'1 room', '2 rooms', '3 rooms', '4 rooms', '5+ rooms'")
    surface_m2: float = Field(..., gt=0, example=55.0, description="Built surface in square metres")
    asked_price: float = Field(..., gt=0, example=250000.0, description="Listed price in euros")

    class Config:
        json_schema_extra = {
            "example": {
                "commune_code": "44109",
                "property_type": "Apartment",
                "rooms_bucket": "3 rooms",
                "surface_m2": 55,
                "asked_price": 250000,
            }
        }


class EstimateResponse(BaseModel):
    commune_code: str
    commune_name: Optional[str]
    property_type: str
    rooms_bucket: str
    surface_m2: float
    asked_price: float
    estimated_fair_price: float
    low_fair_price: float
    high_fair_price: float
    label: str
    confidence: str
    transaction_count: int
    median_price_m2: float
    estimated_gross_yield: Optional[float]
    latest_rate_value: Optional[float]
    computation_date: Optional[str]


class MarketProfile(BaseModel):
    commune_code: str
    commune_name: Optional[str]
    department_code: Optional[str]
    segments: list[dict]


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health", tags=["System"])
def health():
    return {"status": "ok", "date": str(date.today())}


@app.post("/refresh-cache", tags=["System"])
def refresh_cache():
    """Reload the usage dataset from disk (useful after a pipeline run)."""
    _refresh_cache()
    return {"status": "cache cleared"}


@app.get("/communes", tags=["Market"])
def list_communes():
    """Return all communes that have market data."""
    df = _load_estimates()
    result = (
        df[["commune_code", "commune_name", "department_code"]]
        .drop_duplicates()
        .sort_values("commune_name")
        .to_dict(orient="records")
    )
    return {"count": len(result), "communes": result}


@app.get("/market/{commune_code}", response_model=MarketProfile, tags=["Market"])
def get_market(commune_code: str):
    """Return all market segments for a given commune."""
    df = _load_estimates()
    subset = df[df["commune_code"] == commune_code]
    if subset.empty:
        raise HTTPException(status_code=404, detail=f"No data for commune {commune_code}")

    segments = []
    for _, row in subset.iterrows():
        seg = row.dropna().to_dict()
        for k, v in seg.items():
            if hasattr(v, "item"):
                seg[k] = v.item()
            if hasattr(v, "isoformat"):
                seg[k] = str(v)[:10]
        segments.append(seg)

    first = subset.iloc[0]
    return MarketProfile(
        commune_code=commune_code,
        commune_name=str(first.get("commune_name", "")),
        department_code=str(first.get("department_code", "")),
        segments=segments,
    )


@app.post("/estimate", response_model=EstimateResponse, tags=["Estimation"])
def estimate(req: EstimateRequest):
    """
    Classify a property as UNDERPRICED, FAIRLY_PRICED, or OVERPRICED.

    Logic:
    - low_fair_price  = q25_price_m2 × surface_m2
    - high_fair_price = q75_price_m2 × surface_m2
    - UNDERPRICED  if asked_price < low_fair_price
    - FAIRLY_PRICED if low_fair_price ≤ asked_price ≤ high_fair_price
    - OVERPRICED   if asked_price > high_fair_price
    """
    df = _load_estimates()

    match = df[
        (df["commune_code"] == req.commune_code)
        & (df["property_type"] == req.property_type)
        & (df["rooms_bucket"] == req.rooms_bucket)
    ]

    if match.empty:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No market reference found for commune={req.commune_code}, "
                f"type={req.property_type}, rooms={req.rooms_bucket}. "
                "Try GET /market/{commune_code} to see available segments."
            ),
        )

    # Use the most recent computation
    row = match.sort_values("computation_date", ascending=False).iloc[0]

    q25 = float(row["q25_price_m2"])
    q75 = float(row["q75_price_m2"])
    median = float(row["median_price_m2"])

    low_fair = round(q25 * req.surface_m2, 0)
    high_fair = round(q75 * req.surface_m2, 0)
    estimated_fair = round(median * req.surface_m2, 0)

    if req.asked_price < low_fair:
        label = "UNDERPRICED"
    elif req.asked_price > high_fair:
        label = "OVERPRICED"
    else:
        label = "FAIRLY_PRICED"

    return EstimateResponse(
        commune_code=req.commune_code,
        commune_name=str(row.get("commune_name", "")),
        property_type=req.property_type,
        rooms_bucket=req.rooms_bucket,
        surface_m2=req.surface_m2,
        asked_price=req.asked_price,
        estimated_fair_price=estimated_fair,
        low_fair_price=low_fair,
        high_fair_price=high_fair,
        label=label,
        confidence=str(row.get("fair_price_confidence", "LOW")),
        transaction_count=int(row.get("transaction_count", 0)),
        median_price_m2=median,
        estimated_gross_yield=float(row["estimated_gross_yield"]) if pd.notna(row.get("estimated_gross_yield")) else None,
        latest_rate_value=float(row["latest_rate_value"]) if pd.notna(row.get("latest_rate_value")) else None,
        computation_date=str(row.get("computation_date", ""))[:10] if row.get("computation_date") is not None else None,
    )
