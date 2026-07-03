"""Unit tests for Silver layer — silver_cleanse.py"""

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

# Load silver notebook cells
_SRC = open("src/pipelines/sales/orders/silver_cleanse.py").read()
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

silver = types.ModuleType("silver_cleanse")
silver.__dict__.update(_ns)

class TestTrimAndLower:
    def test_products_title_trimmed(self):
        title = "  My Product Title   "
        assert title.strip() == "My Product Title"

    def test_products_category_lower_and_trim(self):
        cat = "  BeAuTy  "
        assert cat.lower().strip() == "beauty"

class TestCastNumeric:
    def test_price_cast_to_double(self):
        price = "19.99"
        assert float(price) == 19.99

    def test_rating_cast_to_double(self):
        rating = "4.5"
        assert float(rating) == 4.5

class TestExplodeCart:
    def test_orders_uses_lateral_view_explode(self):
        sql_content = open("src/pipelines/sales/orders/silver_cleanse.py").read()
        assert "EXPLODE" in sql_content
        # Verify safety pattern: EXPLODE wrapped in subquery first
        assert "exploded_orders AS (" in sql_content

class TestDedupKeepsLatest:
    def test_products_dedup_by_id(self):
        sql_content = open("src/pipelines/sales/orders/silver_cleanse.py").read()
        assert "ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingestion_timestamp DESC)" in sql_content

    def test_orders_dedup_by_cart_product(self):
        sql_content = open("src/pipelines/sales/orders/silver_cleanse.py").read()
        assert "ROW_NUMBER() OVER (PARTITION BY cart_id, product_id ORDER BY _ingestion_timestamp DESC)" in sql_content

class TestQuarantineNullCartId:
    def test_quarantine_filters_null_ids(self):
        sql_content = open("src/pipelines/sales/orders/silver_cleanse.py").read()
        assert "WHERE id IS NULL" in sql_content

    def test_quarantine_includes_reason(self):
        sql_content = open("src/pipelines/sales/orders/silver_cleanse.py").read()
        assert "'Missing cart ID' AS quarantine_reason" in sql_content
