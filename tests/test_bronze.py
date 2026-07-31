import os
import sys
from unittest.mock import MagicMock, patch
import pytest

# Stub PySpark types in sys.modules to prevent import failures
class MockStructType:
    def __init__(self, fields=None):
        self.fields = fields or []
class MockStructField:
    def __init__(self, name, dataType, nullable=True):
        self.name = name
        self.dataType = dataType
        self.nullable = nullable
class MockStringType: pass
class MockIntegerType: pass
class MockDoubleType: pass
class MockArrayType:
    def __init__(self, elementType, containsNull=True):
        self.elementType = elementType
        self.containsNull = containsNull

sys.modules['pyspark'] = MagicMock()
sys.modules['pyspark.sql'] = MagicMock()
sys.modules['pyspark.sql.types'] = MagicMock()
sys.modules['pyspark.sql.types'].StructType = MockStructType
sys.modules['pyspark.sql.types'].StructField = MockStructField
sys.modules['pyspark.sql.types'].StringType = MockStringType
sys.modules['pyspark.sql.types'].IntegerType = MockIntegerType
sys.modules['pyspark.sql.types'].DoubleType = MockDoubleType
sys.modules['pyspark.sql.types'].ArrayType = MockArrayType

# Mock classes for notebook execution
class MockDataFrame:
    def __init__(self, data, schema):
        self.data = data
        self.schema = schema
        self.write = MagicMock()
        self.write.format = MagicMock(return_value=self.write)
        self.write.mode = MagicMock(return_value=self.write)
        self.write.saveAsTable = MagicMock()

class MockSparkSession:
    def __init__(self):
        self.queries = []
        self.created_dfs = {}
    def sql(self, query):
        self.queries.append(query)
        res = MagicMock()
        res.show = MagicMock()
        return res
    def createDataFrame(self, data, schema):
        df = MockDataFrame(data, schema)
        # Identify which table this belongs to based on schema field count or specific fields
        field_names = [f.name for f in schema.fields]
        if "brand" in field_names:
            self.created_dfs["products"] = df
        elif "totalProducts" in field_names:
            self.created_dfs["carts"] = df
        elif "firstName" in field_names:
            self.created_dfs["users"] = df
        return df

def run_bronze_notebook(spark_mock):
    notebook_path = "src/pipelines/sales/orders/bronze_ingest.py"
    # Fallback search if running from subdirectory
    for _ in range(5):
        if os.path.exists(notebook_path):
            break
        notebook_path = os.path.join("..", notebook_path)
        
    with open(notebook_path, "r") as f:
        code = f.read()
    
    # Pre-inject standard imports + stubs
    dbutils_mock = MagicMock()
    del dbutils_mock.entrypoint # Force AttributeError to trigger fallback local pathing
    
    namespace = {
        "spark": spark_mock,
        "dbutils": dbutils_mock,
        "requests": sys.modules['requests'] # Will be patched in tests
    }
    
    cells = code.split("# COMMAND ----------")
    for cell in cells:
        if cell.strip():
            # Exclude the "notebook source" header line if it contains syntax errors
            exec(cell, namespace)
            
    return namespace

# COMMAND ----------
# TESTS

@patch("requests.get")
def test_bronze_api_success(mock_get):
    # Mock API returns 200 with valid JSON structure
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "products": [{"id": 10, "title": "Mock Product", "price": 12.50, "category": "beauty", "rating": 4.5, "brand": "Generic", "description": "Desc"}],
        "carts": [{"id": 20, "userId": 30, "totalProducts": 1, "totalQuantity": 1, "total": 12.50, "products": [{"id": 10, "title": "Mock Product", "price": 12.50, "quantity": 1, "total": 12.50}]}],
        "users": [{"id": 30, "firstName": "Alice", "lastName": "Smith", "email": "alice@gmail.com", "phone": "123", "username": "alice", "address": {"address": "123 St", "city": "NYC", "state": "NY", "postalCode": "10001"}}]
    }
    mock_get.return_value = mock_response

    spark_mock = MockSparkSession()
    namespace = run_bronze_notebook(spark_mock)
    
    # Assert tables were written
    assert "products" in spark_mock.created_dfs
    assert "carts" in spark_mock.created_dfs
    assert "users" in spark_mock.created_dfs
    
    # Verify metadata fields are populated and preprocessed types
    products_df = spark_mock.created_dfs["products"]
    assert len(products_df.data) == 1
    assert products_df.data[0]["id"] == 10
    assert products_df.data[0]["price"] == 12.50
    assert products_df.data[0]["_source"] == "sales_hub_api"
    assert "_ingestion_timestamp" in products_df.data[0]
    assert "_batch_id" in products_df.data[0]

@patch("requests.get")
def test_bronze_api_failure_fallback(mock_get):
    # Mock API fails (e.g. 500)
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_get.return_value = mock_response

    spark_mock = MockSparkSession()
    namespace = run_bronze_notebook(spark_mock)
    
    # Verify fallback data is used and _source is "fallback_sample"
    products_df = spark_mock.created_dfs["products"]
    assert len(products_df.data) == len(namespace["FALLBACK_PRODUCTS"])
    assert products_df.data[0]["_source"] == "fallback_sample"

def test_bronze_schema_verification():
    spark_mock = MockSparkSession()
    namespace = run_bronze_notebook(spark_mock)
    
    # Verify schemas match structural requirements
    products_df = spark_mock.created_dfs["products"]
    fields = {f.name: f.dataType for f in products_df.schema.fields}
    assert "id" in fields
    assert "price" in fields
    assert "_ingestion_timestamp" in fields

@patch("requests.get")
def test_under_the_hood_redirection(mock_get):
    # Verify E-commerce domain is rewritten to DummyJSON domain for API requests
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"products": [], "carts": [], "users": []}
    mock_get.return_value = mock_response

    spark_mock = MockSparkSession()
    run_bronze_notebook(spark_mock)
    
    # Assert requests.get was called with working dummyjson URL
    for call in mock_get.call_args_list:
        url = call[0][0]
        assert "dummyjson.com" in url
        assert "api.sales-hub.com" not in url
        assert "limit=0" in url
