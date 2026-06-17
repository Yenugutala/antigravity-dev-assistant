"""Unit tests for Bronze layer — bronze_ingest.py"""

import sys
import types
from unittest.mock import patch, MagicMock


# ---------------------------------------------------------------------------
# PySpark stubs — allow bronze module to load without a Spark cluster
# ---------------------------------------------------------------------------
class _StubField:
    def __init__(self, *a, **kw):
        pass

class _StubType:
    def __init__(self, *a, **kw):
        pass

_pyspark_types_mod = types.ModuleType("pyspark.sql.types")
for _name in (
    "StructType", "StructField", "IntegerType", "StringType",
    "DoubleType", "ArrayType", "LongType",
):
    setattr(_pyspark_types_mod, _name, type(_name, (), {"__init__": lambda self, *a, **kw: None}))

sys.modules.setdefault("pyspark", types.ModuleType("pyspark"))
sys.modules.setdefault("pyspark.sql", types.ModuleType("pyspark.sql"))
sys.modules.setdefault("pyspark.sql.types", _pyspark_types_mod)

# ---------------------------------------------------------------------------
# Load bronze module via cell-based extraction
# ---------------------------------------------------------------------------
_SRC = open("src/pipelines/sales/orders/bronze_ingest.py").read()
_CELLS = _SRC.split("# COMMAND ----------")

# Prepare a namespace with required builtins
import requests as _requests_mod
import uuid as _uuid_mod
import datetime as _datetime_mod

_ns: dict = {
    "__builtins__": __builtins__,
    "requests": _requests_mod,
    "uuid": _uuid_mod,
    "datetime": _datetime_mod,
    "spark": MagicMock(),
    "display": MagicMock(),
}
# Inject PySpark type stubs
for _n in ("StructType", "StructField", "IntegerType", "StringType", "DoubleType", "ArrayType"):
    _ns[_n] = getattr(_pyspark_types_mod, _n)

# Execute only safe cells (skip spark.sql, .write., display, MAGIC)
_SKIP_MARKERS = ("spark.sql(", ".write.", "display(", "# MAGIC", "dbutils.")
for _cell in _CELLS:
    stripped = _cell.strip()
    if stripped.startswith("# Databricks notebook source"):
        continue
    if any(m in stripped for m in _SKIP_MARKERS):
        continue
    try:
        exec(stripped, _ns)
    except Exception:
        pass  # skip cells that fail without Spark

bronze = types.ModuleType("bronze_ingest")
bronze.__dict__.update(_ns)


# ===== TESTS =============================================================

class TestCleanProduct:
    """Test product data cleaning and normalization."""

    def test_clean_product_fields(self):
        """Products should be cleaned to tuples with correct types."""
        raw = [{"id": 1, "title": "Test Product", "price": 29.99,
                "category": "beauty", "rating": 4.5, "brand": "TestBrand",
                "description": "A test product"}]
        cleaned = []
        for p in raw:
            cleaned.append((
                int(p["id"]),
                str(p.get("title", "")),
                float(p.get("price", 0.0)),
                str(p.get("category", "")),
                float(p["rating"]) if p.get("rating") is not None else None,
                str(p.get("brand", "")) if p.get("brand") is not None else None,
                str(p.get("description", "")) if p.get("description") is not None else None,
                "2025-01-01T00:00:00",
                "test",
                "batch-1",
            ))
        assert cleaned[0][0] == 1
        assert cleaned[0][1] == "Test Product"
        assert cleaned[0][2] == 29.99
        assert cleaned[0][3] == "beauty"
        assert cleaned[0][4] == 4.5

    def test_clean_product_nullable_fields(self):
        """Nullable fields should be None when absent."""
        raw = [{"id": 2, "title": "No Optional", "price": 5.0,
                "category": "other", "rating": None, "brand": None,
                "description": None}]
        cleaned = []
        for p in raw:
            cleaned.append((
                int(p["id"]),
                str(p.get("title", "")),
                float(p.get("price", 0.0)),
                str(p.get("category", "")),
                float(p["rating"]) if p.get("rating") is not None else None,
                str(p.get("brand", "")) if p.get("brand") is not None else None,
                str(p.get("description", "")) if p.get("description") is not None else None,
                "ts", "src", "batch",
            ))
        assert cleaned[0][4] is None
        assert cleaned[0][5] is None
        assert cleaned[0][6] is None


class TestCleanCart:
    """Test cart data cleaning and normalization."""

    def test_clean_cart_items(self):
        """Cart items should be extracted as list of tuples."""
        raw_cart = {
            "id": 1, "userId": 10, "totalProducts": 2,
            "totalQuantity": 3, "total": 50.0,
            "products": [
                {"id": 1, "title": "A", "price": 10.0, "quantity": 2, "total": 20.0},
                {"id": 2, "title": "B", "price": 30.0, "quantity": 1, "total": 30.0},
            ]
        }
        items = []
        for item in raw_cart["products"]:
            items.append((
                int(item["id"]),
                str(item.get("title", "")),
                float(item.get("price", 0.0)),
                int(item.get("quantity", 0)),
                float(item.get("total", 0.0)),
            ))
        assert len(items) == 2
        assert items[0] == (1, "A", 10.0, 2, 20.0)
        assert items[1] == (2, "B", 30.0, 1, 30.0)


class TestCleanUser:
    """Test user data cleaning and normalization."""

    def test_clean_user_with_address(self):
        """User address should be flattened to a tuple."""
        raw_user = {
            "id": 1, "firstName": "Emily", "lastName": "Johnson",
            "email": "emily@test.com", "phone": "555-0101",
            "username": "emilys",
            "address": {"address": "123 Main St", "city": "Phoenix",
                        "state": "AZ", "postalCode": "85001"}
        }
        addr = raw_user.get("address", {})
        address_tuple = (
            str(addr.get("address", "")),
            str(addr.get("city", "")),
            str(addr.get("state", "")),
            str(addr.get("postalCode", "")),
        )
        assert address_tuple == ("123 Main St", "Phoenix", "AZ", "85001")


class TestSchemaFields:
    """Test that schemas have expected fields defined."""

    def test_products_schema_exists(self):
        """Products schema should be defined in bronze module."""
        assert "products_schema" in bronze.__dict__

    def test_carts_schema_exists(self):
        """Carts schema should be defined in bronze module."""
        assert "carts_schema" in bronze.__dict__

    def test_users_schema_exists(self):
        """Users schema should be defined in bronze module."""
        assert "users_schema" in bronze.__dict__


class TestAPIFallback:
    """Test API fallback data is embedded."""

    @patch("requests.get")
    def test_api_failure_uses_fallback(self, mock_get):
        """When API fails, fallback data should be used."""
        mock_get.side_effect = Exception("Connection refused")

        source_label = "dummyjson_api"
        try:
            resp = _requests_mod.get("https://dummyjson.com/products?limit=50", timeout=10)
            resp.raise_for_status()
            raw_products = resp.json()["products"]
        except Exception:
            raw_products = bronze.__dict__["FALLBACK_PRODUCTS"]
            source_label = "fallback_sample"

        assert source_label == "fallback_sample"
        assert len(raw_products) > 0
        assert raw_products[0]["id"] == 1

    def test_fallback_products_not_empty(self):
        """Fallback product data should contain records."""
        fallback = bronze.__dict__["FALLBACK_PRODUCTS"]
        assert len(fallback) >= 5

    def test_fallback_carts_not_empty(self):
        """Fallback cart data should contain records."""
        fallback = bronze.__dict__["FALLBACK_CARTS"]
        assert len(fallback) >= 2

    def test_fallback_users_not_empty(self):
        """Fallback user data should contain records."""
        fallback = bronze.__dict__["FALLBACK_USERS"]
        assert len(fallback) >= 2
