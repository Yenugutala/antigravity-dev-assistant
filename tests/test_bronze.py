"""
Unit tests for Bronze Layer: Sales Orders — Raw Data Ingestion
Runs locally without a Databricks cluster.
"""

import sys
import types
import pytest
from unittest.mock import patch


# ── Stub PySpark types in sys.modules ────────────────────────────

class _StubStructField:
    def __init__(self, name="", dataType=None, nullable=True):
        self.name = name
        self.dataType = dataType
        self.nullable = nullable

class _StubStructType:
    def __init__(self, fields=None):
        self.fields = fields or []

class _StubArrayType:
    def __init__(self, elementType=None, containsNull=True):
        self.elementType = elementType
        self.containsNull = containsNull

class _StubType:
    pass

pyspark_mod = types.ModuleType("pyspark")
pyspark_sql_mod = types.ModuleType("pyspark.sql")
pyspark_types_mod = types.ModuleType("pyspark.sql.types")

for cls_name in ["StringType", "IntegerType", "DoubleType", "LongType", "BooleanType", "TimestampType"]:
    setattr(pyspark_types_mod, cls_name, type(cls_name, (_StubType,), {"__init__": lambda self: None}))

pyspark_types_mod.StructType = _StubStructType
pyspark_types_mod.StructField = _StubStructField
pyspark_types_mod.ArrayType = _StubArrayType

sys.modules["pyspark"] = pyspark_mod
sys.modules["pyspark.sql"] = pyspark_sql_mod
sys.modules["pyspark.sql.types"] = pyspark_types_mod


# ── Load Bronze Module via Cell Extraction ───────────────────────

import os
import requests
import uuid
from datetime import datetime

BRONZE_PATH = os.path.join(
    os.path.dirname(__file__), "..", "src", "pipelines", "sales", "orders", "bronze_ingest.py"
)

def _load_bronze_module():
    """Load bronze module by executing safe cells (skip Databricks-specific ones)."""
    with open(BRONZE_PATH, "r") as f:
        source = f.read()

    cells = source.split("# COMMAND ----------")
    module_dict = {
        "__builtins__": __builtins__,
        "requests": requests,
        "uuid": uuid,
        "datetime": datetime,
        "StructType": _StubStructType,
        "StructField": _StubStructField,
        "ArrayType": _StubArrayType,
        "StringType": pyspark_types_mod.StringType,
        "IntegerType": pyspark_types_mod.IntegerType,
        "DoubleType": pyspark_types_mod.DoubleType,
        "print": print,
    }

    skip_keywords = ["spark.", "display(", ".write.", "dbutils.", "# Databricks notebook source"]

    for cell in cells:
        cell_stripped = cell.strip()
        if not cell_stripped:
            continue
        if any(kw in cell_stripped for kw in skip_keywords):
            continue
        try:
            exec(cell_stripped, module_dict)
        except Exception:
            pass

    return module_dict


@pytest.fixture(scope="module")
def bronze():
    return _load_bronze_module()


# ── Tests ────────────────────────────────────────────────────────

class TestCleanProduct:
    def test_clean_product_basic(self, bronze):
        """Test that clean_product strips fields and casts numerics correctly."""
        raw = {
            "id": 1, "title": "Test Product", "price": 9.99,
            "category": "beauty", "rating": 4.5, "brand": "TestBrand",
            "description": "A test product", "extra_field": "should_be_stripped",
            "thumbnail": "http://example.com/img.jpg"
        }
        result = bronze["clean_product"](raw)
        assert result["id"] == 1
        assert result["title"] == "Test Product"
        assert isinstance(result["price"], float)
        assert result["price"] == 9.99
        assert result["category"] == "beauty"
        assert result["rating"] == 4.5
        assert "extra_field" not in result
        assert "thumbnail" not in result
        assert "_ingestion_timestamp" in result
        assert "_source" in result
        assert "_batch_id" in result


class TestCleanCart:
    def test_clean_cart_with_items(self, bronze):
        """Test that clean_cart processes nested products array correctly."""
        raw = {
            "id": 1, "userId": 10,
            "totalProducts": 2, "totalQuantity": 3, "total": 50.0,
            "products": [
                {"id": 1, "title": "Item A", "price": 10.0, "quantity": 2, "total": 20.0},
                {"id": 2, "title": "Item B", "price": 30.0, "quantity": 1, "total": 30.0},
            ]
        }
        result = bronze["clean_cart"](raw)
        assert result["id"] == 1
        assert result["userId"] == 10
        assert len(result["products"]) == 2
        assert isinstance(result["products"][0]["price"], float)
        assert result["products"][0]["quantity"] == 2
        assert result["total"] == 50.0


class TestCleanUser:
    def test_clean_user_flattens_address(self, bronze):
        """Test that clean_user normalizes user data and preserves address."""
        raw = {
            "id": 1, "firstName": "Emily", "lastName": "Johnson",
            "email": "emily@test.com", "phone": "+1-555-0101",
            "username": "emilys",
            "address": {
                "address": "626 Main St", "city": "Phoenix",
                "state": "Mississippi", "postalCode": "29112"
            }
        }
        result = bronze["clean_user"](raw)
        assert result["id"] == 1
        assert result["firstName"] == "Emily"
        assert result["address"]["city"] == "Phoenix"
        assert result["address"]["postalCode"] == "29112"
        assert "_source" in result


class TestSchemaFields:
    def test_product_schema_has_required_fields(self, bronze):
        """Test that product_schema contains all required fields."""
        schema = bronze["product_schema"]
        field_names = [f.name for f in schema.fields]
        assert "id" in field_names
        assert "title" in field_names
        assert "price" in field_names
        assert "category" in field_names
        assert "_ingestion_timestamp" in field_names
        assert "_source" in field_names
        assert "_batch_id" in field_names


class TestApiFallback:
    @patch("requests.get")
    def test_fallback_on_api_failure(self, mock_get, bronze):
        """Test that fetch_api_data returns fallback data when API fails."""
        mock_get.side_effect = Exception("Connection timeout")
        result = bronze["fetch_api_data"]("products", "products")
        assert len(result) > 0
        assert result[0]["id"] == 1
        assert "title" in result[0]
