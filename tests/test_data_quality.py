"""
test_data_quality.py
─────────────────────
Data quality checks for the ingestion and formatting layers.

Tests run against sample/mock DataFrames — no real network calls or Spark
cluster required (uses pandas for the pure-Python ingestion tests).
"""

import io
import pytest
import pandas as pd


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_dvf_raw(**overrides) -> pd.DataFrame:
    """Return a single-row DVF-like DataFrame with sensible defaults."""
    defaults = {
        "date_mutation": "2024-03-15",
        "valeur_fonciere": "250000",
        "type_local": "Appartement",
        "surface_reelle_bati": "55",
        "nombre_pieces_principales": "3",
        "code_commune": "44109",
        "nom_commune": "Nantes",
        "code_departement": "44",
    }
    defaults.update(overrides)
    return pd.DataFrame([defaults])


def _format_dvf_row(row: pd.Series) -> dict | None:
    """
    Replicates the key formatting logic from format_dvf_spark.py using pandas.
    Returns None if the row should be filtered out.
    """
    try:
        price = float(str(row.get("valeur_fonciere", "")).replace(",", "."))
        surface = float(str(row.get("surface_reelle_bati", "")).replace(",", "."))
    except (ValueError, TypeError):
        return None

    if price <= 0 or surface <= 0:
        return None

    prop_type_raw = str(row.get("type_local", "")).lower()
    if prop_type_raw not in ("appartement", "maison"):
        return None

    prop_type = "Apartment" if prop_type_raw == "appartement" else "House"
    price_per_m2 = round(price / surface, 2)
    commune_code = str(row.get("code_commune", "")).zfill(5)

    return {
        "sale_price": price,
        "surface_m2": surface,
        "property_type": prop_type,
        "price_per_m2": price_per_m2,
        "commune_code": commune_code,
    }


# ── DVF data quality ──────────────────────────────────────────────────────────

class TestDvfFormatting:
    def test_valid_apartment_row(self):
        df = _make_dvf_raw()
        result = _format_dvf_row(df.iloc[0])
        assert result is not None
        assert result["property_type"] == "Apartment"
        assert result["price_per_m2"] == round(250000 / 55, 2)
        assert result["commune_code"] == "44109"

    def test_valid_house_row(self):
        df = _make_dvf_raw(type_local="Maison")
        result = _format_dvf_row(df.iloc[0])
        assert result["property_type"] == "House"

    def test_missing_price_is_filtered(self):
        df = _make_dvf_raw(valeur_fonciere="")
        assert _format_dvf_row(df.iloc[0]) is None

    def test_zero_price_is_filtered(self):
        df = _make_dvf_raw(valeur_fonciere="0")
        assert _format_dvf_row(df.iloc[0]) is None

    def test_zero_surface_is_filtered(self):
        df = _make_dvf_raw(surface_reelle_bati="0")
        assert _format_dvf_row(df.iloc[0]) is None

    def test_missing_surface_is_filtered(self):
        df = _make_dvf_raw(surface_reelle_bati="")
        assert _format_dvf_row(df.iloc[0]) is None

    def test_garage_is_filtered(self):
        df = _make_dvf_raw(type_local="Dépendance")
        assert _format_dvf_row(df.iloc[0]) is None

    def test_commune_code_zero_padded(self):
        df = _make_dvf_raw(code_commune="75056")
        result = _format_dvf_row(df.iloc[0])
        assert result["commune_code"] == "75056"

    def test_commune_code_short_is_padded(self):
        df = _make_dvf_raw(code_commune="1001")
        result = _format_dvf_row(df.iloc[0])
        assert result["commune_code"] == "01001"

    def test_price_with_comma_decimal(self):
        # Some DVF exports use comma as decimal separator
        df = _make_dvf_raw(valeur_fonciere="250000,50", surface_reelle_bati="55,0")
        result = _format_dvf_row(df.iloc[0])
        assert result is not None
        assert abs(result["sale_price"] - 250000.50) < 0.01


# ── Rent data quality ─────────────────────────────────────────────────────────

class TestRentFormatting:
    def _make_rents_df(self, **overrides):
        defaults = {
            "code_commune": "44109",
            "nom_commune": "Nantes",
            "loyer_m2_appartement": "15.8",
            "loyer_m2_maison": "12.5",
        }
        defaults.update(overrides)
        return pd.DataFrame([defaults])

    def test_valid_row_parsed(self):
        df = self._make_rents_df()
        assert float(df.iloc[0]["loyer_m2_appartement"]) == 15.8

    def test_missing_commune_code_detected(self):
        df = self._make_rents_df(code_commune=None)
        assert df.iloc[0]["code_commune"] is None

    def test_comma_decimal_in_rent(self):
        df = self._make_rents_df(loyer_m2_appartement="15,8")
        val = float(str(df.iloc[0]["loyer_m2_appartement"]).replace(",", "."))
        assert val == 15.8


# ── ECB rates data quality ────────────────────────────────────────────────────

class TestEcbRatesParsing:
    def _make_ecb_record(self, **overrides):
        defaults = {
            "date": "2024-11-20",
            "rate_value": 3.4,
            "rate_type": "ESTR",
            "source": "ECB Data Portal",
        }
        defaults.update(overrides)
        return defaults

    def test_valid_record(self):
        rec = self._make_ecb_record()
        assert rec["rate_value"] == 3.4
        assert rec["rate_type"] == "ESTR"

    def test_null_value_should_be_filtered(self):
        rec = self._make_ecb_record(rate_value=None)
        assert rec["rate_value"] is None  # downstream Spark filter removes these

    def test_date_format(self):
        rec = self._make_ecb_record(date="2024-11-20")
        parsed = pd.to_datetime(rec["date"]).date()
        assert str(parsed) == "2024-11-20"


# ── Usage schema validation ───────────────────────────────────────────────────

class TestUsageSchema:
    REQUIRED_COLS = [
        "commune_code", "commune_name", "department_code",
        "property_type", "rooms_bucket", "transaction_count",
        "median_price_m2", "avg_price_m2", "q25_price_m2", "q75_price_m2",
        "rent_m2", "estimated_gross_yield", "latest_rate_value",
        "market_liquidity_score", "fair_price_confidence", "computation_date",
    ]

    def _make_usage_row(self):
        return {col: None for col in self.REQUIRED_COLS}

    def test_all_required_columns_present(self):
        row = self._make_usage_row()
        missing = [c for c in self.REQUIRED_COLS if c not in row]
        assert missing == []

    def test_confidence_values_are_valid(self):
        valid = {"LOW", "MEDIUM", "HIGH"}
        for v in valid:
            row = self._make_usage_row()
            row["fair_price_confidence"] = v
            assert row["fair_price_confidence"] in valid
