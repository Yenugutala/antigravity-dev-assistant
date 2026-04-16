"""Unit tests for Bronze Layer: Sales Orders — Raw Data Ingestion."""

import pytest
import sys
import types
import pathlib
from unittest.mock import patch, MagicMock


# ---------------------------------------------------------------------------
# Stub PySpark types so tests run locally without pyspark installed
# ---------------------------------------------------------------------------

class _SparkType:
    pass

class StringType(_SparkType):
    pass

class IntegerType(_SparkType):
    pass

class DoubleType(_SparkType):
    pass

class TimestampType(_SparkType):
    pass

class StructField:
    def __init__(self, name, dataType, nullable=True):
        self.name = name
        self.dataType = dataType
        self.nullable = nullable

class StructType:
    def __init__(self, fields=None):
        self.fields = fields or []

class ArrayType:
    def __init__(self, elementType, containsNull=True):
        self.elementType = elementType
        self.containsNull = containsNull

# Install stubs into sys.modules before loading bronze
_pyspark_types_mod = types.ModuleType("pyspark.sql.types")
_pyspark_types_mod.StructType = StructType
_pyspark_types_mod.StructField = StructField
_pyspark_types_mod.StringType = StringType
_pyspark_types_mod.IntegerType = IntegerType
_pyspark_types_mod.DoubleType = DoubleType
_pyspark_types_mod.ArrayType = ArrayType
_pyspark_types_mod.TimestampType = TimestampType

_pyspark_funcs_mod = types.ModuleType("pyspark.sql.functions")
_pyspark_funcs_mod.lit = MagicMock()
_pyspark_funcs_mod.current_timestamp = MagicMock()

for mod_name, mod_obj in [
    ("pyspark", types.ModuleType("pyspark")),
    ("pyspark.sql", types.ModuleType("pyspark.sql")),
    ("pyspark.sql.types", _pyspark_types_mod),
    ("pyspark.sql.functions", _pyspark_funcs_mod),
]:
    sys.modules[mod_name] = mod_obj

# ---------------------------------------------------------------------------
# Import bronze module helpers by exec-ing the file (skip Databricks cells)
# ---------------------------------------------------------------------------

_BRONZE_PATH = pathlib.Path(__file__).resolve().parents[1] / "src" / "pipelines" / "sales" / "orders" / "bronze_ingest.py"

def _load_bronze_module():
    """Load bronze_ingest.py as a module, extracting safe code blocks."""
    import requests, uuid
    from datetime import datetime
    source = _BRONZE_PATH.read_text()
    mod = types.ModuleType("bronze_ingest")
    mod.__dict__["spark"] = MagicMock()
    mod.__dict__["display"] = MagicMock()
    mod.__dict__["current_timestamp"] = MagicMock()
    mod.__dict__["lit"] = MagicMock()
    mod.__dict__["requests"] = requests
    mod.__dict__["uuid"] = uuid
    mod.__dict__["datetime"] = datetime
    # Inject PySpark type stubs
    mod.__dict__["StructType"] = StructType
    mod.__dict__["StructField"] = StructField
    mod.__dict__["StringType"] = StringType
    mod.__dict__["IntegerType"] = IntegerType
    mod.__dict__["DoubleType"] = DoubleType
    mod.__dict__["ArrayType"] = ArrayType
    mod.__dict__["TimestampType"] = TimestampType
    # Extract code between COMMAND markers, skip Databricks-specific cells
    cells = source.split("# COMMAND ----------")
    safe_code = []
    for cell in cells:
        stripped = cell.strip()
        if "# MAGIC" in stripped:
            continue
        if "spark." in stripped:
            continue
        if "display(" in stripped:
            continue
        if ".write." in stripped:
            continue
        if stripped.startswith("# Databricks notebook source"):
            continue
        if not stripped:
            continue
        safe_code.append(cell)
    exec(compile("\n".join(safe_code), str(_BRONZE_PATH), "exec"), mod.__dict__)
    return mod

bronze = _load_bronze_module()


# ===========================================================================
# Tests
# ===========================================================================

class TestCleanProduct:
    def test_basic_product(self):
        raw = {"id": 1, "title": "Widget", "price": 9.99, "category": "tools",
               "rating": 4.5, "brand": "Acme", "description": "A widget"}
        result = bronze.clean_product(raw)
        assert result["id"] == 1
        assert result["price"] == 9.99
        assert "thumbnail" not in result


class TestCleanCart:
    def test_basic_cart(self):
        raw = {"id": 1, "userId": 10, "totalProducts": 2, "totalQuantity": 3,
               "total": 29.99, "products": [
                   {"id": 1, "title": "A", "price": 9.99, "quantity": 2, "total": 19.98},
               ]}
        result = bronze.clean_cart(raw)
        assert result["id"] == 1
        assert len(result["products"]) == 1
        assert isinstance(result["products"][0]["price"], float)


class TestCleanUser:
    def test_basic_user(self):
        raw = {"id": 1, "firstName": "Alice", "lastName": "Smith",
               "email": "alice@test.com", "phone": "555-0100",
               "username": "alice", "address": {
                   "address": "123 Main St", "city": "NYC",
                   "state": "NY", "postalCode": "10001"}}
        result = bronze.clean_user(raw)
        assert result["id"] == 1
        assert result["address"]["city"] == "NYC"


class TestSchemas:
    def test_product_schema_fields(self):
        schema = bronze.PRODUCT_SCHEMA
        field_names = [f.name for f in schema.fields]
        assert "id" in field_names
        assert "title" in field_names
        assert "price" in field_names


class TestFetchFromAPI:
    @patch("requests.get")
    def test_api_failure_uses_fallback(self, mock_get):
        mock_get.side_effect = Exception("Connection timeout")
        fallback = [{"id": 99, "title": "FB", "price": 1.0, "category": "x",
                      "rating": 1.0, "brand": "F", "description": "Fallback"}]
        result = bronze.fetch_from_api("/products", "products", fallback, bronze.clean_product)
        assert len(result) == 1
        assert result[0]["id"] == 99
