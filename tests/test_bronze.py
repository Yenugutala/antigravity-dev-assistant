"""Unit tests for Bronze Ingestion pipeline."""

import sys
import types
import pytest
from unittest.mock import patch, MagicMock


# ---------------------------------------------------------------------------
# PySpark stubs — allows local test execution without a Spark cluster
# ---------------------------------------------------------------------------
pyspark_types = types.ModuleType("pyspark.sql.types")
for cls_name in [
    "StructType", "StructField", "StringType", "IntegerType",
    "DoubleType", "ArrayType", "Row",
]:
    setattr(pyspark_types, cls_name, MagicMock)

for mod_path in [
    "pyspark", "pyspark.sql", "pyspark.sql.types",
    "pyspark.sql.functions", "pyspark.sql.session",
]:
    sys.modules[mod_path] = types.ModuleType(mod_path)

sys.modules["pyspark.sql.types"] = pyspark_types


# ---------------------------------------------------------------------------
# Load bronze module via cell-based extraction
# ---------------------------------------------------------------------------
BRONZE_PATH = "src/pipelines/sales/orders/bronze_ingest.py"


def _load_bronze_module():
    """Load bronze notebook as a module using cell-based extraction."""
    with open(BRONZE_PATH, "r") as f:
        source = f.read()

    cells = source.split("# COMMAND ----------")
    mod = types.ModuleType("bronze_ingest")
    mod.__dict__.update({
        "spark": MagicMock(),
        "dbutils": MagicMock(),
        "requests": MagicMock(),
        "uuid": __import__("uuid"),
        "datetime": __import__("datetime"),
    })

    # Execute imports + fallback data + helper cells (cells 0-4, skip ingestion)
    for cell in cells[:5]:
        cleaned = cell.replace("# Databricks notebook source", "").strip()
        if cleaned:
            try:
                exec(cleaned, mod.__dict__)
            except Exception:
                pass

    return mod


@pytest.fixture(scope="module")
def bronze():
    return _load_bronze_module()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestBronzeProducts:
    def test_clean_product(self, bronze):
        """Fallback products have required fields with correct types."""
        products = bronze.FALLBACK_PRODUCTS
        for p in products:
            assert isinstance(p["id"], int)
            assert isinstance(p["price"], (int, float))
            assert isinstance(p["title"], str)
            assert isinstance(p["category"], str)

    def test_product_schema_fields(self, bronze):
        """Products schema has all required columns."""
        expected_fields = {"id", "title", "price", "category", "rating", "brand", "description"}
        actual_fields = {p for prod in bronze.FALLBACK_PRODUCTS for p in prod.keys()}
        assert expected_fields.issubset(actual_fields)


class TestBronzeCarts:
    def test_clean_cart(self, bronze):
        """Fallback carts have required fields and nested products."""
        carts = bronze.FALLBACK_CARTS
        for c in carts:
            assert isinstance(c["id"], int)
            assert isinstance(c["userId"], int)
            assert isinstance(c["products"], list)
            assert len(c["products"]) > 0

    def test_cart_item_fields(self, bronze):
        """Cart items have id, price, quantity, total."""
        cart = bronze.FALLBACK_CARTS[0]
        for item in cart["products"]:
            assert "id" in item
            assert "price" in item
            assert "quantity" in item
            assert "total" in item


class TestBronzeUsers:
    def test_clean_user(self, bronze):
        """Fallback users have required fields."""
        users = bronze.FALLBACK_USERS
        for u in users:
            assert isinstance(u["id"], int)
            assert isinstance(u["email"], str)
            assert isinstance(u["username"], str)
            assert "firstName" in u
            assert "lastName" in u


class TestBronzeAPIFallback:
    @patch("requests.get")
    def test_api_fallback_on_failure(self, mock_get, bronze):
        """When API fails, fallback data is returned."""
        mock_get.side_effect = Exception("Connection timeout")
        data, source = bronze.fetch_api_data("/products", "products", bronze.FALLBACK_PRODUCTS)
        assert source == "fallback_sample"
        assert len(data) == len(bronze.FALLBACK_PRODUCTS)
