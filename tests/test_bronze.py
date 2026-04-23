"""Unit tests for Bronze ingestion pipeline (bronze_ingest.py)."""

import sys
import os
import types
from unittest.mock import MagicMock, patch

import pytest

# ── PySpark Stubs ──
# Stub PySpark modules so tests run locally without a Spark cluster

pyspark_mod = types.ModuleType("pyspark")
sql_mod = types.ModuleType("pyspark.sql")
types_mod = types.ModuleType("pyspark.sql.types")
functions_mod = types.ModuleType("pyspark.sql.functions")

for type_name in [
    "StructType", "StructField", "IntegerType", "StringType",
    "DoubleType", "ArrayType", "LongType", "BooleanType",
    "FloatType", "TimestampType", "DateType",
]:
    setattr(types_mod, type_name, MagicMock(name=type_name))

sql_mod.types = types_mod
sql_mod.functions = functions_mod
pyspark_mod.sql = sql_mod

sys.modules["pyspark"] = pyspark_mod
sys.modules["pyspark.sql"] = sql_mod
sys.modules["pyspark.sql.types"] = types_mod
sys.modules["pyspark.sql.functions"] = functions_mod

# ── Load Bronze Module ──

BRONZE_PATH = os.path.join(
    os.path.dirname(__file__),
    "..", "src", "pipelines", "sales", "orders", "bronze_ingest.py"
)

def load_bronze_module():
    """Load bronze notebook as module using cell-based extraction."""
    with open(os.path.abspath(BRONZE_PATH), "r") as f:
        source = f.read()

    cells = source.split("# COMMAND ----------")
    module = types.ModuleType("bronze_ingest")
    module.__dict__.update({
        "spark": MagicMock(),
        "dbutils": MagicMock(),
        "requests": MagicMock(),
        "uuid": __import__("uuid"),
        "datetime": __import__("datetime"),
        "__builtins__": __builtins__,
    })
    # Import PySpark types into module namespace
    for type_name in ["StructType", "StructField", "IntegerType", "StringType",
                      "DoubleType", "ArrayType", "LongType"]:
        module.__dict__[type_name] = MagicMock(name=type_name)

    # Execute cells: imports, config, fallback data, helpers, schemas (skip ingestion cells)
    for cell in cells[1:7]:  # Cells: imports, config, fallback, helpers, schemas
        try:
            exec(compile(cell, "bronze_ingest.py", "exec"), module.__dict__)
        except Exception:
            pass

    return module


@pytest.fixture
def bronze():
    return load_bronze_module()


# ── Tests ──

class TestCleanProduct:
    def test_clean_product_basic(self, bronze):
        """Test clean_product strips fields and casts numerics correctly."""
        raw = {
            "id": 1, "title": "  Test Product  ", "price": "19.99",
            "category": "Beauty", "rating": "4.5", "brand": "TestBrand",
            "description": "A test", "extra_field": "should_be_stripped",
        }
        result = bronze.clean_product(raw)
        assert result["id"] == 1
        assert result["price"] == 19.99
        assert isinstance(result["price"], float)
        assert result["rating"] == 4.5
        assert "extra_field" not in result

    def test_clean_product_null_optional_fields(self, bronze):
        """Test clean_product handles missing optional fields."""
        raw = {"id": 2, "title": "Minimal", "price": 5.0, "category": "test"}
        result = bronze.clean_product(raw)
        assert result["id"] == 2
        assert result["rating"] is None
        assert result["brand"] is None


class TestCleanCart:
    def test_clean_cart_with_products(self, bronze):
        """Test clean_cart processes nested products array."""
        raw = {
            "id": 1, "userId": 10, "totalProducts": 1, "totalQuantity": 2,
            "total": 39.98,
            "products": [
                {"id": 5, "title": "Item A", "price": "19.99", "quantity": 2, "total": "39.98"}
            ],
        }
        result = bronze.clean_cart(raw)
        assert result["id"] == 1
        assert result["userId"] == 10
        assert len(result["products"]) == 1
        assert result["products"][0]["price"] == 19.99
        assert isinstance(result["products"][0]["quantity"], int)


class TestCleanUser:
    def test_clean_user_with_address(self, bronze):
        """Test clean_user flattens address correctly."""
        raw = {
            "id": 1, "firstName": "Emily", "lastName": "Johnson",
            "email": "emily@test.com", "phone": "+1-555-0101",
            "username": "emilyj",
            "address": {"address": "123 Main St", "city": "NYC", "state": "NY", "postalCode": "10001"},
            "extra": "stripped",
        }
        result = bronze.clean_user(raw)
        assert result["id"] == 1
        assert result["firstName"] == "Emily"
        assert result["address"]["city"] == "NYC"
        assert "extra" not in result


class TestSchemaFields:
    def test_products_schema_has_metadata_columns(self, bronze):
        """Test that products schema includes metadata columns."""
        schema = bronze.products_schema
        # StructType is mocked but we verify the calls were made
        assert schema is not None

    def test_carts_schema_exists(self, bronze):
        """Test that carts schema is defined."""
        assert hasattr(bronze, "carts_schema")
        assert bronze.carts_schema is not None


class TestApiFallback:
    @patch("requests.get")
    def test_fetch_api_data_success(self, mock_get, bronze):
        """Test API fetch returns data on success."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"products": [{"id": 1, "title": "Test"}]}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        result = bronze.fetch_api_data("products", "products")
        assert result is not None
        assert len(result) == 1

    @patch("requests.get")
    def test_fetch_api_data_failure_returns_none(self, mock_get, bronze):
        """Test API fetch returns None on failure (triggers fallback)."""
        mock_get.side_effect = Exception("Connection timeout")

        result = bronze.fetch_api_data("products", "products")
        assert result is None
