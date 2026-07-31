import os
import sys
from unittest.mock import MagicMock, patch
import pytest

# Stub PySpark types in sys.modules
sys.modules['pyspark'] = MagicMock()
sys.modules['pyspark.sql'] = MagicMock()
sys.modules['pyspark.sql.types'] = MagicMock()

class MockSparkSession:
    def __init__(self):
        self.queries = []
    def sql(self, query):
        self.queries.append(query)
        mock_df = MagicMock()
        mock_df.show = MagicMock()
        return mock_df

def run_silver_notebook(spark_mock):
    notebook_path = "src/pipelines/sales/orders/silver_cleanse.py"
    for _ in range(5):
        if os.path.exists(notebook_path):
            break
        notebook_path = os.path.join("..", notebook_path)
        
    with open(notebook_path, "r") as f:
        code = f.read()
    
    namespace = {
        "spark": spark_mock,
        "dbutils": MagicMock()
    }
    
    cells = code.split("# COMMAND ----------")
    for cell in cells:
        if cell.strip():
            exec(cell, namespace)
            
    return namespace

# COMMAND ----------
# TESTS

def test_silver_sql_syntax_and_conventions():
    spark_mock = MockSparkSession()
    run_silver_notebook(spark_mock)
    
    assert len(spark_mock.queries) >= 3
    
    # Verify SQL query formatting guidelines (UPPERCASE keywords)
    for query in spark_mock.queries:
        sql_upper = query.upper()
        # Verify basic keywords are uppercase in source query
        for keyword in ["SELECT", "FROM", "WHERE", "JOIN"]:
            if keyword in sql_upper:
                # Ensure the exact keyword exists in uppercase
                assert keyword in query, f"SQL keyword '{keyword}' is not in UPPERCASE in: {query}"

def test_silver_explosion_safety():
    spark_mock = MockSparkSession()
    run_silver_notebook(spark_mock)
    
    # Locate the orders transformation query
    orders_query = ""
    for q in spark_mock.queries:
        if "s_salesorders.orders" in q and "LATERAL VIEW EXPLODE" in q:
            orders_query = q
            break
            
    assert orders_query != "", "Could not locate the orders explosion query in silver notebook"
    
    # Explosions safety: Verify EXPLODE is wrapped in a CTE/subquery (exploded_carts) and joined outside
    assert "WITH EXPLODED_CARTS" in orders_query.upper()
    assert "LEFT JOIN S_SALESORDERS.PRODUCTS" in orders_query.upper()

def test_silver_deduplication_logic():
    spark_mock = MockSparkSession()
    run_silver_notebook(spark_mock)
    
    # Verify deduplication syntax is present in all tables
    for query in spark_mock.queries:
        if "CREATE OR REPLACE TABLE" in query:
            assert "ROW_NUMBER() OVER" in query
            assert "WHERE ROW_NUM = 1" in query.upper()

def test_silver_quarantine_logic():
    spark_mock = MockSparkSession()
    run_silver_notebook(spark_mock)
    
    # Verify quarantine routing queries exist
    quarantine_table_query = ""
    quarantine_insert_query = ""
    
    for q in spark_mock.queries:
        if "ORDERS_QUARANTINE" in q.upper():
            if "CREATE TABLE" in q:
                quarantine_table_query = q
            elif "INSERT INTO" in q:
                quarantine_insert_query = q
                
    assert quarantine_table_query != ""
    assert quarantine_insert_query != ""
    
    # Ensure invalid records (Missing cart ID) route correctly
    assert "WHERE ID IS NULL" in quarantine_insert_query.upper()
    assert "MISSING CART ID" in quarantine_insert_query.upper()
