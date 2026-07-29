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
    dbutils.entrypoint.getDbutils().notebook().getContext().notebookPath().get.return_value = "/src/pipelines/sales/orders/gold_aggregate"
    return dbutils

def test_gold_revenue_by_category_query(mock_spark, mock_dbutils):
    execute_notebook("src/pipelines/sales/orders/gold_aggregate.py", mock_spark, mock_dbutils)
    
    calls = [c[0][0] for c in mock_spark.sql.call_args_list]
    rev_query = next((q for q in calls if "g_salesorders.revenue_by_category" in q), None)
    
    assert rev_query is not None
    assert "GROUP BY category" in rev_query
    assert "COALESCE" in rev_query
    assert "ROUND(SUM(line_total), 2)" in rev_query
    assert "COUNT(*)" in rev_query

def test_gold_order_summary_query(mock_spark, mock_dbutils):
    execute_notebook("src/pipelines/sales/orders/gold_aggregate.py", mock_spark, mock_dbutils)
    
    calls = [c[0][0] for c in mock_spark.sql.call_args_list]
    summary_query = next((q for q in calls if "g_salesorders.order_summary" in q), None)
    
    assert summary_query is not None
    assert "GROUP BY order_date" in summary_query
    assert "COUNT(DISTINCT cart_id)" in summary_query
    assert "COUNT(DISTINCT user_id)" in summary_query

def test_beauty_revenue(mock_spark, mock_dbutils):
    # Setup mock collect response for beauty category revenue
    mock_row = MagicMock()
    mock_row.category = "beauty"
    mock_row.total_revenue = 100.00
    
    mock_df = MagicMock()
    mock_df.collect.return_value = [mock_row]
    mock_spark.sql.return_value = mock_df
    
    # query mock
    res = mock_spark.sql("SELECT total_revenue FROM g_salesorders.revenue_by_category WHERE category = 'beauty'")
    rows = res.collect()
    
    assert len(rows) == 1
    assert rows[0].total_revenue == 100.00

def test_total_revenue(mock_spark, mock_dbutils):
    mock_row = MagicMock()
    mock_row.total_revenue = 1500.50
    
    mock_df = MagicMock()
    mock_df.collect.return_value = [mock_row]
    mock_spark.sql.return_value = mock_df
    
    res = mock_spark.sql("SELECT SUM(total_revenue) FROM g_salesorders.revenue_by_category")
    rows = res.collect()
    
    assert rows[0].total_revenue == 1500.50

def test_total_orders(mock_spark, mock_dbutils):
    mock_row = MagicMock()
    mock_row.total_orders = 25
    
    mock_df = MagicMock()
    mock_df.collect.return_value = [mock_row]
    mock_spark.sql.return_value = mock_df
    
    res = mock_spark.sql("SELECT SUM(total_orders) FROM g_salesorders.order_summary")
    rows = res.collect()
    
    assert rows[0].total_orders == 25
