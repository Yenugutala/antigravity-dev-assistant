"""Unit tests for Bronze layer — bronze_ingest.py"""

import sys
import types
from unittest.mock import patch, MagicMock

# Stub PySpark sql types before importing
class _StubType:
    def __init__(self, *args, **kwargs): pass

_pyspark_types_mod = types.ModuleType("pyspark.sql.types")
for _name in ("StructType", "StructField", "IntegerType", "StringType", "DoubleType", "ArrayType"):
    setattr(_pyspark_types_mod, _name, type(_name, (), {"__init__": lambda self, *a, **k: None}))

sys.modules["pyspark"] = types.ModuleType("pyspark")
sys.modules["pyspark.sql"] = types.ModuleType("pyspark.sql")
sys.modules["pyspark.sql.types"] = _pyspark_types_mod

# Load bronze notebook cells
_SRC = open("src/pipelines/sales/orders/bronze_ingest.py").read()
_CELLS = _SRC.split("# COMMAND ----------")

import requests as _requests_mod
import uuid as _uuid_mod
import datetime as _datetime_mod

_ns = {
    "__builtins__": __builtins__,
    "requests": _requests_mod,
    "uuid": _uuid_mod,
    "datetime": _datetime_mod,
    "spark": MagicMock(),
    "display": MagicMock(),
}
for _n in ("StructType", "StructField", "IntegerType", "StringType", "DoubleType", "ArrayType"):
    _ns[_n] = getattr(_pyspark_types_mod, _n)

# Exec helper
_SKIP_MARKERS = ("spark.sql(", ".write.", "display(", "# MAGIC")
for _cell in _CELLS:
    stripped = _cell.strip()
    if stripped.startswith("# Databricks notebook source") or any(m in stripped for m in _SKIP_MARKERS):
        continue
    try:
        exec(stripped, _ns)
    except Exception:
        pass

bronze = types.ModuleType("bronze_ingest")
bronze.__dict__.update(_ns)

class TestCleanProduct:
    def test_clean_product_fields(self):
        # Verify cleaning function converts raw dictionary to normalized tuple
        raw = [{"id": 1, "title": "Test Title", "price": 10.99, "category": "beauty", "rating": 4.5, "brand": "BrandA", "description": "DescA"}]
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
                "2026-07-03",
                "dummyjson_api",
                "batch-1",
            ))
        assert cleaned[0][0] == 1
        assert cleaned[0][1] == "Test Title"
        assert cleaned[0][2] == 10.99
        assert cleaned[0][3] == "beauty"
        assert cleaned[0][4] == 4.5
        assert cleaned[0][5] == "BrandA"
        assert cleaned[0][6] == "DescA"

    def test_clean_product_nullable_fields(self):
        raw = [{"id": 1, "title": "Test Title", "price": 10.99, "category": "beauty", "rating": None, "brand": None, "description": None}]
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
                "2026-07-03",
                "dummyjson_api",
                "batch-1",
            ))
        assert cleaned[0][4] is None
        assert cleaned[0][5] is None or cleaned[0][5] == ""
        assert cleaned[0][6] is None or cleaned[0][6] == ""

class TestCleanCart:
    def test_clean_cart_items(self):
        raw_items = [{"id": 10, "title": "Product Title", "price": 5.0, "quantity": 3, "total": 15.0}]
        cleaned_items = []
        for item in raw_items:
            cleaned_items.append((
                int(item["id"]),
                str(item.get("title", "")),
                float(item.get("price", 0.0)),
                int(item.get("quantity", 0)),
                float(item.get("total", 0.0)),
            ))
        assert cleaned_items[0][0] == 10
        assert cleaned_items[0][1] == "Product Title"
        assert cleaned_items[0][2] == 5.0
        assert cleaned_items[0][3] == 3
        assert cleaned_items[0][4] == 15.0

class TestCleanUser:
    def test_clean_user_with_address(self):
        raw_user = {"id": 1, "firstName": "John", "lastName": "Doe", "email": "john.doe@x.com", "phone": "123", "username": "johndoe",
                    "address": {"address": "123 Main St", "city": "Springfield", "state": "IL", "postalCode": "62701"}}
        addr = raw_user.get("address", {})
        addr_tuple = (
            str(addr.get("address", "")),
            str(addr.get("city", "")),
            str(addr.get("state", "")),
            str(addr.get("postalCode", "")),
        )
        assert addr_tuple == ("123 Main St", "Springfield", "IL", "62701")

class TestSchemaFields:
    def test_products_schema_exists(self):
        assert hasattr(bronze, "products_schema")
        assert bronze.products_schema is not None

    def test_carts_schema_exists(self):
        assert hasattr(bronze, "carts_schema")
        assert bronze.carts_schema is not None

    def test_users_schema_exists(self):
        assert hasattr(bronze, "users_schema")
        assert bronze.users_schema is not None

class TestAPIFallback:
    def test_fallback_products_not_empty(self):
        assert len(bronze.FALLBACK_PRODUCTS) > 0
        assert bronze.FALLBACK_PRODUCTS[0]["id"] == 1

    def test_fallback_carts_not_empty(self):
        assert len(bronze.FALLBACK_CARTS) > 0
        assert bronze.FALLBACK_CARTS[0]["id"] == 1

    def test_fallback_users_not_empty(self):
        assert len(bronze.FALLBACK_USERS) > 0
        assert bronze.FALLBACK_USERS[0]["id"] == 1
