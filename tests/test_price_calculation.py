"""
test_price_calculation.py
──────────────────────────
Unit tests for fair price classification and rental yield logic.

These tests run without Spark or Elasticsearch — they validate the pure
business-logic formulas defined in the specification.
"""

import math
import pytest


# ── Replicated logic (mirrors the Spark UDFs) ────────────────────────────────

def classify(asked_price: float, q25: float, q75: float) -> str:
    low = q25
    high = q75
    if asked_price < low:
        return "UNDERPRICED"
    if asked_price > high:
        return "OVERPRICED"
    return "FAIRLY_PRICED"


def fair_price_range(q25_m2: float, q75_m2: float, median_m2: float, surface: float):
    return {
        "low": round(q25_m2 * surface, 2),
        "high": round(q75_m2 * surface, 2),
        "estimated": round(median_m2 * surface, 2),
    }


def confidence(transaction_count: int) -> str:
    if transaction_count >= 100:
        return "HIGH"
    if transaction_count >= 30:
        return "MEDIUM"
    return "LOW"


def gross_yield(rent_m2: float, median_price_m2: float) -> float | None:
    if not median_price_m2:
        return None
    return round((rent_m2 * 12) / median_price_m2, 4)


def liquidity_score(count: int) -> float:
    return round(min(1.0, math.log(count + 1) / math.log(1000)), 4)


# ── Classification tests ──────────────────────────────────────────────────────

class TestClassification:
    def test_fairly_priced(self):
        assert classify(250_000, 220_000, 258_500) == "FAIRLY_PRICED"

    def test_underpriced(self):
        assert classify(200_000, 220_000, 258_500) == "UNDERPRICED"

    def test_overpriced(self):
        assert classify(300_000, 220_000, 258_500) == "OVERPRICED"

    def test_exactly_at_low_boundary(self):
        # At the lower boundary → FAIRLY_PRICED (spec: low <= asked <= high)
        assert classify(220_000, 220_000, 258_500) == "FAIRLY_PRICED"

    def test_exactly_at_high_boundary(self):
        assert classify(258_500, 220_000, 258_500) == "FAIRLY_PRICED"

    def test_one_above_high(self):
        assert classify(258_501, 220_000, 258_500) == "OVERPRICED"

    def test_zero_price(self):
        assert classify(0, 220_000, 258_500) == "UNDERPRICED"


# ── Fair price range tests ────────────────────────────────────────────────────

class TestFairPriceRange:
    def test_nantes_apartment(self):
        result = fair_price_range(q25_m2=4000, q75_m2=4700, median_m2=4300, surface=55)
        assert result["low"] == 220_000.0
        assert result["high"] == 258_500.0
        assert result["estimated"] == 236_500.0

    def test_surface_zero_returns_zero(self):
        result = fair_price_range(4000, 4700, 4300, 0)
        assert result["low"] == 0
        assert result["estimated"] == 0


# ── Confidence tests ──────────────────────────────────────────────────────────

class TestConfidence:
    def test_high(self):
        assert confidence(100) == "HIGH"
        assert confidence(1250) == "HIGH"

    def test_medium(self):
        assert confidence(30) == "MEDIUM"
        assert confidence(99) == "MEDIUM"

    def test_low(self):
        assert confidence(0) == "LOW"
        assert confidence(29) == "LOW"

    def test_boundary_30(self):
        assert confidence(30) == "MEDIUM"

    def test_boundary_100(self):
        assert confidence(100) == "HIGH"


# ── Rental yield tests ────────────────────────────────────────────────────────

class TestGrossYield:
    def test_example_from_spec(self):
        # rent_m2=16, median_price_m2=4300 → 4.46%
        result = gross_yield(16, 4300)
        assert abs(result - 0.0447) < 0.001

    def test_none_on_zero_price(self):
        assert gross_yield(16, 0) is None

    def test_none_on_none_price(self):
        assert gross_yield(16, None) is None


# ── Liquidity score tests ─────────────────────────────────────────────────────

class TestLiquidityScore:
    def test_zero_count(self):
        assert liquidity_score(0) == round(math.log(1) / math.log(1000), 4)

    def test_capped_at_one(self):
        assert liquidity_score(1_000_000) == 1.0

    def test_1000_count(self):
        # log(1001)/log(1000) ≈ 1.0
        score = liquidity_score(999)
        assert 0.99 <= score <= 1.0

    def test_high_count(self):
        # 1250 transactions (Nantes example)
        score = liquidity_score(1250)
        assert score == 1.0
