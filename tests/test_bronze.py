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
# Clean Function Tests
# ===========================================================================

class TestCleanProduct:
    """Tests for clean_product() helper."""

    def test_basic_product(self):
        raw = {"id": 1, "title": "Widget", "price": 9.99, "category": "tools",
               "rating": 4.5, "brand": "Acme", "description": "A widget"}
        result = bronze.clean_product(raw)
        assert result["id"] == 1
        assert result["title"] == "Widget"
        assert result["price"] == 9.99
        assert result["category"] == "tools"
        assert result["rating"] == 4.5
        assert result["brand"] == "Acme"

    def test_numeric_casting(self):
        raw = {"id": "7", "title": "X", "price": "19.99", "category": "c",
               "rating": "3.5", "brand": "B", "description": "D"}
        result = bronze.clean_product(raw)
        assert isinstance(result["id"], int)
        assert isinstance(result["price"], float)
        assert isinstance(result["rating"], float)

    def test_nullable_fields(self):
        raw = {"id": 1, "title": "X", "price": 5.0, "category": "c",
               "rating": None, "brand": None, "description": None}
        result = bronze.clean_product(raw)
        assert result["rating"] is None
        assert result["brand"] is None
        assert result["description"] is None

    def test_missing_optional_defaults(self):
        raw = {"id": 1, "title": "X", "price": 0.0, "category": "c"}
        result = bronze.clean_product(raw)
        assert result["title"] == "X"
        assert result["price"] == 0.0
        assert result["rating"] is None
        assert result["brand"] is None

    def test_strips_extra_fields(self):
        raw = {"id": 1, "title": "X", "price": 5.0, "category": "c",
               "rating": 4.0, "brand": "B", "description": "D",
               "thumbnail": "http://img.png", "stock": 50}
        result = bronze.clean_product(raw)
        assert "thumbnail" not in result
        assert "stock" not in result


class TestCleanCart:
    """Tests for clean_cart() helper."""

    def test_basic_cart(self):
        raw = {"id": 1, "userId": 10, "totalProducts": 2, "totalQuantity": 3,
               "total": 29.99, "products": [
                   {"id": 1, "title": "A", "price": 9.99, "quantity": 2, "total": 19.98},
                   {"id": 2, "title": "B", "price": 10.01, "quantity": 1, "total": 10.01},
               ]}
        result = bronze.clean_cart(raw)
        assert result["id"] == 1
        assert result["userId"] == 10
        assert len(result["products"]) == 2

    def test_cart_product_numeric_casting(self):
        raw = {"id": "1", "userId": "10", "totalProducts": "2", "totalQuantity": "3",
               "total": "29.99", "products": [
                   {"id": "5", "title": "A", "price": "9.99", "quantity": "2", "total": "19.98"},
               ]}
        result = bronze.clean_cart(raw)
        assert isinstance(result["id"], int)
        assert isinstance(result["userId"], int)
        assert isinstance(result["products"][0]["id"], int)
        assert isinstance(result["products"][0]["price"], float)
        assert isinstance(result["products"][0]["quantity"], int)

    def test_empty_products_list(self):
        raw = {"id": 1, "userId": 1, "totalProducts": 0, "totalQuantity": 0,
               "total": 0.0, "products": []}
        result = bronze.clean_cart(raw)
        assert result["products"] == []

    def test_strips_extra_cart_fields(self):
        raw = {"id": 1, "userId": 1, "totalProducts": 1, "totalQuantity": 1,
               "total": 5.0, "discountedTotal": 4.5, "products": [
                   {"id": 1, "title": "A", "price": 5.0, "quantity": 1,
                    "total": 5.0, "discountPercentage": 10, "thumbnail": "x"},
               ]}
        result = bronze.clean_cart(raw)
        assert "discountedTotal" not in result
        assert "discountPercentage" not in result["products"][0]
        assert "thumbnail" not in result["products"][0]


class TestCleanUser:
    """Tests for clean_user() helper."""

    def test_basic_user(self):
        raw = {"id": 1, "firstName": "Alice", "lastName": "Smith",
               "email": "alice@test.com", "phone": "555-0100",
               "username": "alice", "address": {
                   "address": "123 Main St", "city": "NYC",
                   "state": "NY", "postalCode": "10001"}}
        result = bronze.clean_user(raw)
        assert result["id"] == 1
        assert result["firstName"] == "Alice"
        assert result["address"]["city"] == "NYC"
        assert result["address"]["postalCode"] == "10001"

    def test_missing_address(self):
        raw = {"id": 1, "firstName": "X", "lastName": "Y",
               "email": "x@y.com", "phone": "", "username": "xy"}
        result = bronze.clean_user(raw)
        assert result["address"]["city"] == ""
        assert result["address"]["postalCode"] == ""

    def test_strips_extra_user_fields(self):
        raw = {"id": 1, "firstName": "A", "lastName": "B",
               "email": "a@b.com", "phone": "123", "username": "ab",
               "address": {"address": "1 St", "city": "C", "state": "S", "postalCode": "0"},
               "age": 30, "image": "http://img.png", "bloodGroup": "O+"}
        result = bronze.clean_user(raw)
        assert "age" not in result
        assert "image" not in result


# ===========================================================================
# Schema Validation Tests
# ===========================================================================

class TestSchemas:
    """Validate the explicit StructType schemas."""

    def test_product_schema_fields(self):
        schema = bronze.PRODUCT_SCHEMA
        field_names = [f.name for f in schema.fields]
        assert "id" in field_names
        assert "title" in field_names
        assert "price" in field_names
        assert "category" in field_names
        assert "rating" in field_names
        assert "brand" in field_names

    def test_product_schema_types(self):
        schema = bronze.PRODUCT_SCHEMA
        type_map = {f.name: type(f.dataType) for f in schema.fields}
        assert type_map["id"] == IntegerType
        assert type_map["price"] == DoubleType
        assert type_map["title"] == StringType

    def test_cart_schema_has_products_array(self):
        schema = bronze.CART_SCHEMA
        products_field = next(f for f in schema.fields if f.name == "products")
        assert isinstance(products_field.dataType, ArrayType)

    def test_user_schema_has_address_struct(self):
        schema = bronze.USER_SCHEMA
        address_field = next(f for f in schema.fields if f.name == "address")
        assert isinstance(address_field.dataType, StructType)

    def test_address_schema_fields(self):
        field_names = [f.name for f in bronze.ADDRESS_SCHEMA.fields]
        assert "address" in field_names
        assert "city" in field_names
        assert "postalCode" in field_names


# ===========================================================================
# Fallback Data Tests
# ===========================================================================

class TestFallbackData:
    """Validate embedded fallback sample data."""

    def test_fallback_products_count(self):
        assert len(bronze.FALLBACK_PRODUCTS) == 10

    def test_fallback_carts_count(self):
        assert len(bronze.FALLBACK_CARTS) == 3

    def test_fallback_users_count(self):
        assert len(bronze.FALLBACK_USERS) == 3

    def test_fallback_products_have_required_fields(self):
        for p in bronze.FALLBACK_PRODUCTS:
            assert "id" in p
            assert "title" in p
            assert "price" in p
            assert "category" in p

    def test_fallback_carts_have_products(self):
        for c in bronze.FALLBACK_CARTS:
            assert "products" in c
            assert len(c["products"]) > 0


# ===========================================================================
# API Fetch Tests
# ===========================================================================

class TestFetchFromAPI:
    """Tests for fetch_from_api() with mocked requests."""

    @patch("requests.get")
    def test_successful_api_call(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {"products": [
            {"id": 1, "title": "T", "price": 5.0, "category": "c",
             "rating": 4.0, "brand": "B", "description": "D"},
        ]}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        result = bronze.fetch_from_api("/products", "products", [], bronze.clean_product)
        assert len(result) == 1
        assert result[0]["id"] == 1

    @patch("requests.get")
    def test_api_failure_uses_fallback(self, mock_get):
        mock_get.side_effect = Exception("Connection timeout")
        fallback = [{"id": 99, "title": "FB", "price": 1.0, "category": "x",
                      "rating": 1.0, "brand": "F", "description": "Fallback"}]
        result = bronze.fetch_from_api("/products", "products", fallback, bronze.clean_product)
        assert len(result) == 1
        assert result[0]["id"] == 99
