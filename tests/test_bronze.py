import os
import sys
import pytest
from unittest.mock import MagicMock, patch

# Define PySpark stub types
class StructTypeStub:
    def __init__(self, fields=None):
        self.fields = fields or []
class StructFieldStub:
    def __init__(self, name, dataType, nullable=True):
        self.name = name
        self.dataType = dataType
        self.nullable = nullable
class DataTypeStub:
    pass
class IntegerTypeStub(DataTypeStub):
    pass
class StringTypeStub(DataTypeStub):
    pass
class DoubleTypeStub(DataTypeStub):
    pass
class ArrayTypeStub(DataTypeStub):
    def __init__(self, elementType, containsNull=True):
        self.elementType = elementType
        self.containsNull = containsNull

# Inject PySpark stubs into sys.modules before any imports
pyspark_mock = MagicMock()
pyspark_sql = MagicMock()
pyspark_sql_types = MagicMock()
pyspark_sql_functions = MagicMock()

pyspark_sql_types.StructType = StructTypeStub
pyspark_sql_types.StructField = StructFieldStub
pyspark_sql_types.IntegerType = IntegerTypeStub
pyspark_sql_types.StringType = StringTypeStub
pyspark_sql_types.DoubleType = DoubleTypeStub
pyspark_sql_types.ArrayType = ArrayTypeStub
pyspark_sql_functions.lit = lambda x: x

sys.modules["pyspark"] = pyspark_mock
sys.modules["pyspark.sql"] = pyspark_sql
sys.modules["pyspark.sql.types"] = pyspark_sql_types
sys.modules["pyspark.sql.functions"] = pyspark_sql_functions

def execute_notebook(notebook_path, spark_mock, dbutils_mock):
    with open(notebook_path, "r") as f:
        code = f.read()
    cells = code.split("# COMMAND ----------")
    namespace = {
        "spark": spark_mock,
        "dbutils": dbutils_mock,
        "display": lambda x: x,
        "print": lambda *args: None
    }
    for cell in cells:
        clean_cell = cell.replace("# Databricks notebook source", "")
        exec(clean_cell, namespace)
    return namespace

@pytest.fixture
def mock_spark():
    spark = MagicMock()
    df_mock = MagicMock()
    df_mock.withColumn.return_value = df_mock
    spark.createDataFrame.return_value = df_mock
    return spark

@pytest.fixture
def mock_dbutils():
    dbutils = MagicMock()
    # Mock the notebook path context chain
    dbutils.entrypoint.getDbutils().notebook().getContext().notebookPath().get.return_value = "/src/pipelines/sales/orders/bronze_ingest"
    return dbutils

def test_preprocess_products():
    # Setup dummy raw product item
    raw = [{
        "id": "10",
        "title": "Test Product",
        "price": "19.99",
        "category": "beauty",
        "rating": 4.5,
        "brand": "BrandX",
        "description": "DescX"
    }]
    
    # Import the function from notebook namespace by running setup cells
    spark = MagicMock()
    dbutils = MagicMock()
    dbutils.entrypoint.getDbutils().notebook().getContext().notebookPath().get.side_effect = Exception("Fallback")
    
    # Executing notebook code to get function
    ns = execute_notebook("src/pipelines/sales/orders/bronze_ingest.py", spark, dbutils)
    preprocess_fn = ns["preprocess_products"]
    
    clean = preprocess_fn(raw)
    assert len(clean) == 1
    assert clean[0]["id"] == 10
    assert isinstance(clean[0]["price"], float)
    assert clean[0]["price"] == 19.99
    assert clean[0]["category"] == "beauty"

def test_preprocess_carts():
    raw = [{
        "id": "1",
        "userId": "100",
        "totalProducts": "2",
        "totalQuantity": "5",
        "total": "50.5",
        "products": [{
            "id": "10",
            "title": "Prod A",
            "price": "10.10",
            "quantity": "5",
            "total": "50.5"
        }]
    }]
    
    ns = execute_notebook("src/pipelines/sales/orders/bronze_ingest.py", MagicMock(), MagicMock())
    preprocess_fn = ns["preprocess_carts"]
    
    clean = preprocess_fn(raw)
    assert len(clean) == 1
    assert clean[0]["id"] == 1
    assert clean[0]["userId"] == 100
    assert clean[0]["products"][0]["price"] == 10.10
    assert clean[0]["products"][0]["quantity"] == 5

def test_preprocess_users():
    raw = [{
        "id": "5",
        "firstName": "Alice",
        "lastName": "Smith",
        "email": "alice@gmail.com",
        "phone": "555-1234",
        "username": "alicesmith",
        "address": {
            "address": "456 Oak Rd",
            "city": "Austin",
            "state": "TX",
            "postalCode": "73301"
        }
    }]
    
    ns = execute_notebook("src/pipelines/sales/orders/bronze_ingest.py", MagicMock(), MagicMock())
    preprocess_fn = ns["preprocess_users"]
    
    clean = preprocess_fn(raw)
    assert len(clean) == 1
    assert clean[0]["id"] == 5
    assert clean[0]["firstName"] == "Alice"
    assert clean[0]["address"]["city"] == "Austin"

@patch("requests.get")
def test_bronze_ingest_success(mock_get, mock_spark, mock_dbutils):
    # Setup mock response
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "products": [{"id": 1, "title": "P1", "price": 1.0, "category": "C1"}],
        "carts": [{"id": 1, "userId": 1, "products": []}],
        "users": [{"id": 1, "email": "e1", "username": "u1"}]
    }
    mock_get.return_value = mock_resp
    
    ns = execute_notebook("src/pipelines/sales/orders/bronze_ingest.py", mock_spark, mock_dbutils)
    
    # Assert API was called
    assert mock_get.call_count >= 1
    # Assert DataFrame creation was called
    assert mock_spark.createDataFrame.call_count >= 3

@patch("requests.get")
def test_bronze_ingest_api_down(mock_get, mock_spark, mock_dbutils):
    # Setup mock response to fail
    mock_get.side_effect = Exception("API connection timed out")
    
    ns = execute_notebook("src/pipelines/sales/orders/bronze_ingest.py", mock_spark, mock_dbutils)
    
    # Even though API failed, the notebook should use fallback data and proceed
    assert mock_spark.createDataFrame.call_count >= 3
    # Verify fallback metadata sources
    assert ns["source_name"] == "fallback_sample"
