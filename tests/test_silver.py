import sys
import pytest
from unittest.mock import MagicMock

# Define PySpark stubs
sys.modules["pyspark"] = MagicMock()
sys.modules["pyspark.sql"] = MagicMock()
sys.modules["pyspark.sql.types"] = MagicMock()
sys.modules["pyspark.sql.functions"] = MagicMock()

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
    spark.sql.return_value = df_mock
    return spark

@pytest.fixture
def mock_dbutils():
    dbutils = MagicMock()
    dbutils.entrypoint.getDbutils().notebook().getContext().notebookPath().get.return_value = "/src/pipelines/sales/orders/silver_cleanse"
    return dbutils

def test_silver_products_query(mock_spark, mock_dbutils):
    ns = execute_notebook("src/pipelines/sales/orders/silver_cleanse.py", mock_spark, mock_dbutils)
    
    # Verify that spark.sql was called to create products conformed table
    calls = [c[0][0] for c in mock_spark.sql.call_args_list]
    products_query = next((q for q in calls if "s_salesorders.products" in q), None)
    
    assert products_query is not None
    # Verify transformations and filters are in the SQL query
    assert "TRIM(title)" in products_query
    assert "CAST(price AS DOUBLE)" in products_query
    assert "TRIM(LOWER(category))" in products_query
    assert "ROW_NUMBER() OVER" in products_query
    assert "PARTITION BY product_id" in products_query
    assert "price >= 0" in products_query

def test_silver_orders_query_explosion(mock_spark, mock_dbutils):
    ns = execute_notebook("src/pipelines/sales/orders/silver_cleanse.py", mock_spark, mock_dbutils)
    
    calls = [c[0][0] for c in mock_spark.sql.call_args_list]
    orders_query = next((q for q in calls if "s_salesorders.orders" in q), None)
    
    assert orders_query is not None
    # Explosion safety checks: make sure LATERAL VIEW EXPLODE is used safely
    assert "LATERAL VIEW EXPLODE" in orders_query
    assert "LEFT JOIN" in orders_query
    assert "ROUND" in orders_query

def test_silver_customers_query(mock_spark, mock_dbutils):
    ns = execute_notebook("src/pipelines/sales/orders/silver_cleanse.py", mock_spark, mock_dbutils)
    
    calls = [c[0][0] for c in mock_spark.sql.call_args_list]
    customers_query = next((q for q in calls if "s_salesorders.customers" in q), None)
    
    assert customers_query is not None
    assert "TRIM(LOWER(email))" in customers_query
    assert "firstName AS first_name" in customers_query
    assert "address.city AS city" in customers_query

def test_silver_quarantine_query(mock_spark, mock_dbutils):
    ns = execute_notebook("src/pipelines/sales/orders/silver_cleanse.py", mock_spark, mock_dbutils)
    
    calls = [c[0][0] for c in mock_spark.sql.call_args_list]
    quarantine_query = next((q for q in calls if "s_salesorders.orders_quarantine" in q), None)
    
    assert quarantine_query is not None
    assert "WHERE id IS NULL" in quarantine_query
    assert "Missing cart ID" in quarantine_query

def test_silver_notebook_runs_successfully(mock_spark, mock_dbutils):
    ns = execute_notebook("src/pipelines/sales/orders/silver_cleanse.py", mock_spark, mock_dbutils)
    
    # Assert variables loaded correctly
    assert ns["BATCH_ID"] is not None
    assert "b_products" in ns
    assert "s_orders" in ns
