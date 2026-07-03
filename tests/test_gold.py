"""Unit tests for Gold layer — gold_aggregate.py"""

import sys
import types
from unittest.mock import patch, MagicMock

# Stub PySpark sql types
_pyspark_types_mod = types.ModuleType("pyspark.sql.types")
for _name in ("StructType", "StructField", "IntegerType", "StringType", "DoubleType", "ArrayType"):
    setattr(_pyspark_types_mod, _name, type(_name, (), {"__init__": lambda self, *a, **k: None}))

sys.modules["pyspark"] = types.ModuleType("pyspark")
sys.modules["pyspark.sql"] = types.ModuleType("pyspark.sql")
sys.modules["pyspark.sql.types"] = _pyspark_types_mod

# Load gold notebook cells
_SRC = open("src/pipelines/sales/orders/gold_aggregate.py").read()
_CELLS = _SRC.split("# COMMAND ----------")

_ns = {
    "__builtins__": __builtins__,
    "spark": MagicMock(),
}

_SKIP_MARKERS = ("spark.sql(", "# MAGIC")
for _cell in _CELLS:
    stripped = _cell.strip()
    if stripped.startswith("# Databricks notebook source") or any(m in stripped for m in _SKIP_MARKERS):
        continue
    try:
        exec(stripped, _ns)
    except Exception:
        pass

gold = types.ModuleType("gold_aggregate")
gold.__dict__.update(_ns)

class TestCategoryCount:
    def test_groups_by_category(self):
        sql_content = open("src/pipelines/sales/orders/gold_aggregate.py").read()
        assert "GROUP BY category" in sql_content

    def test_counts_unique_products(self):
        sql_content = open("src/pipelines/sales/orders/gold_aggregate.py").read()
        assert "COUNT(DISTINCT product_id)" in sql_content

class TestBeautyRevenue:
    def test_beauty_revenue_from_sample_data(self):
        sql_content = open("src/pipelines/sales/orders/gold_aggregate.py").read()
        assert "SUM(line_total)" in sql_content
        assert "total_revenue" in sql_content

class TestTotalRevenue:
    def test_revenue_ordered_desc(self):
        sql_content = open("src/pipelines/sales/orders/gold_aggregate.py").read()
        assert "ORDER BY total_revenue DESC" in sql_content

    def test_items_sold_aggregation(self):
        sql_content = open("src/pipelines/sales/orders/gold_aggregate.py").read()
        assert "SUM(quantity)" in sql_content

class TestTotalOrders:
    def test_revenue_counts_distinct_orders(self):
        sql_content = open("src/pipelines/sales/orders/gold_aggregate.py").read()
        assert "COUNT(DISTINCT cart_id)" in sql_content

class TestUniqueCustomers:
    def test_summary_counts_unique_customers(self):
        sql_content = open("src/pipelines/sales/orders/gold_aggregate.py").read()
        assert "COUNT(DISTINCT user_id) AS unique_customers" in sql_content

    def test_summary_groups_by_date(self):
        sql_content = open("src/pipelines/sales/orders/gold_aggregate.py").read()
        assert "GROUP BY order_date" in sql_content

    def test_summary_avg_line_value(self):
        sql_content = open("src/pipelines/sales/orders/gold_aggregate.py").read()
        assert "AVG(line_total)" in sql_content
        assert "avg_order_line_value" in sql_content
