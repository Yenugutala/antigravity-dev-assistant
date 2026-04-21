"""Bronze layer unit tests — runs locally without Databricks cluster."""
import sys
import types
import os
from unittest.mock import patch, MagicMock

# ---- Stub PySpark types in sys.modules ----
_pyspark = types.ModuleType("pyspark")
_pyspark_sql = types.ModuleType("pyspark.sql")
_pyspark_types = types.ModuleType("pyspark.sql.types")
_pyspark_functions = types.ModuleType("pyspark.sql.functions")

for name in ["StructType", "StructField", "StringType", "IntegerType",
             "DoubleType", "ArrayType", "TimestampType"]:
    setattr(_pyspark_types, name, MagicMock())

for name in ["lit", "current_timestamp", "col"]:
    setattr(_pyspark_functions, name, MagicMock())

_pyspark_sql.types = _pyspark_types
_pyspark_sql.functions = _pyspark_functions
_pyspark.sql = _pyspark_sql

sys.modules["pyspark"] = _pyspark
sys.modules["pyspark.sql"] = _pyspark_sql
sys.modules["pyspark.sql.types"] = _pyspark_types
sys.modules["pyspark.sql.functions"] = _pyspark_functions

# ---- Load bronze module via cell-based extraction ----
BRONZE_PATH = os.path.join(
    os.path.dirname(__file__), "..", "src", "pipelines", "sales", "orders", "bronze_ingest.py"
)


def _load_bronze_functions() -> dict:
    """Load clean functions and fallback data from bronze notebook."""
    with open(BRONZE_PATH, "r") as f:
        source = f.read()

    cells = source.split("# COMMAND ----------")
    module_dict = {
        "__builtins__": __builtins__,
        "spark": MagicMock(),
        "display": MagicMock(),
        "dbutils": MagicMock(),
    }

    # Pre-inject standard imports
    import requests as _requests
    import uuid as _uuid
    import datetime as _datetime
    module_dict["requests"] = _requests
    module_dict["uuid"] = _uuid
    module_dict["datetime"] = _datetime

    # Inject PySpark stubs into module dict
    for name in ["StructType", "StructField", "StringType", "IntegerType",
                 "DoubleType", "ArrayType", "TimestampType"]:
        module_dict[name] = MagicMock()
    for name in ["lit", "current_timestamp"]:
        module_dict[name] = MagicMock()

    skip_keywords = ["# MAGIC", "spark.", "display(", ".write.", ".saveAsTable("]

    for cell in cells:
        cell = cell.strip()
        if not cell or cell.startswith("# Databricks notebook source"):
            continue
        if any(kw in cell for kw in skip_keywords):
            continue
        try:
            exec(cell, module_dict)
        except Exception:
            pass

    return module_dict


_bronze = _load_bronze_functions()


# ---- Tests ----

class TestCleanProduct:
    def test_strips_extra_fields_and_casts(self):
        raw = {
            "id": 1, "title": "Test Product", "price": 29.99, "category": "beauty",
            "rating": 4.5, "brand": "TestBrand", "description": "A test product",
            "thumbnail": "http://img.com/1", "images": ["http://img.com/1"],
            "discountPercentage": 5.0, "stock": 100,
        }
        result = _bronze["clean_product"](raw)
        assert result["id"] == 1
        assert result["price"] == 29.99
        assert result["category"] == "beauty"
        assert "thumbnail" not in result
        assert "images" not in result
        assert "stock" not in result


class TestCleanCart:
    def test_strips_extra_fields_and_cleans_products(self):
        raw = {
            "id": 1, "userId": 42, "totalProducts": 2, "totalQuantity": 3,
            "total": 59.97, "discountedTotal": 55.0,
            "products": [
                {"id": 10, "title": "Item A", "price": 19.99, "quantity": 2, "total": 39.98,
                 "discountPercentage": 5.0, "discountedTotal": 37.98},
                {"id": 20, "title": "Item B", "price": 19.99, "quantity": 1, "total": 19.99,
                 "discountPercentage": 0, "discountedTotal": 19.99},
            ],
        }
        result = _bronze["clean_cart"](raw)
        assert result["id"] == 1
        assert result["userId"] == 42
        assert len(result["products"]) == 2
        assert result["products"][0]["price"] == 19.99
        assert "discountedTotal" not in result
        assert "discountPercentage" not in result["products"][0]


class TestCleanUser:
    def test_strips_extra_fields_and_keeps_address(self):
        raw = {
            "id": 1, "firstName": "Emily", "lastName": "Johnson",
            "email": "emily@example.com", "phone": "+1-555-0101",
            "username": "emjohnson",
            "address": {"address": "123 Main St", "city": "New York",
                        "state": "NY", "postalCode": "10001"},
            "birthDate": "1990-01-01", "bloodGroup": "A+", "age": 34,
        }
        result = _bronze["clean_user"](raw)
        assert result["id"] == 1
        assert result["firstName"] == "Emily"
        assert result["address"]["city"] == "New York"
        assert "birthDate" not in result
        assert "bloodGroup" not in result


class TestSchemaFields:
    def test_product_has_expected_keys(self):
        raw = {"id": 1, "title": "T", "price": 10.0, "category": "c",
               "rating": 4.0, "brand": "B", "description": "D"}
        result = _bronze["clean_product"](raw)
        expected = {"id", "title", "price", "category", "rating", "brand", "description"}
        assert set(result.keys()) == expected

    def test_cart_has_expected_keys(self):
        raw = {"id": 1, "userId": 1, "totalProducts": 1, "totalQuantity": 1,
               "total": 10.0, "products": [{"id": 1, "title": "A", "price": 10.0,
               "quantity": 1, "total": 10.0}]}
        result = _bronze["clean_cart"](raw)
        expected = {"id", "userId", "totalProducts", "totalQuantity", "total", "products"}
        assert set(result.keys()) == expected


class TestApiFallback:
    @patch("requests.get")
    def test_returns_fallback_on_api_failure(self, mock_get):
        mock_get.side_effect = Exception("API unreachable")
        result = _bronze["fetch_data"]("/products", "products")
        assert len(result) > 0
        assert result[0]["id"] == 1
