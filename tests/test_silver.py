"""Unit tests for Silver Cleansing pipeline."""

import sys
import types
import pytest


# ---------------------------------------------------------------------------
# PySpark stubs
# ---------------------------------------------------------------------------
for mod_path in [
    "pyspark", "pyspark.sql", "pyspark.sql.types",
    "pyspark.sql.functions", "pyspark.sql.session",
]:
    if mod_path not in sys.modules:
        sys.modules[mod_path] = types.ModuleType(mod_path)


# ---------------------------------------------------------------------------
# Load silver module via cell-based extraction
# ---------------------------------------------------------------------------
SILVER_PATH = "src/pipelines/sales/orders/silver_cleanse.py"


def _load_silver_cells():
    """Load silver notebook cells without executing."""
    with open(SILVER_PATH, "r") as f:
        source = f.read()
    cells = source.split("# COMMAND ----------")
    return [c.replace("# Databricks notebook source", "").strip() for c in cells if c.strip()]


@pytest.fixture(scope="module")
def silver_cells():
    return _load_silver_cells()


def _extract_sql(cell_text):
    """Extract SQL string from a spark.sql() call in a cell."""
    start = cell_text.find('spark.sql("""')
    if start == -1:
        return ""
    start += len('spark.sql("""')
    end = cell_text.find('""")', start)
    return cell_text[start:end].strip() if end != -1 else ""


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestSilverProducts:
    def test_trim_and_lower_transforms(self, silver_cells):
        """Products SQL applies TRIM to title and LOWER+TRIM to category."""
        sql = _extract_sql(silver_cells[2])
        assert "TRIM(title)" in sql
        assert "LOWER(TRIM(category))" in sql

    def test_cast_numeric_types(self, silver_cells):
        """Products SQL casts price and rating to DOUBLE."""
        sql = _extract_sql(silver_cells[2])
        assert "CAST(price AS DOUBLE)" in sql
        assert "CAST(rating AS DOUBLE)" in sql


class TestSilverOrders:
    def test_explode_cart_products(self, silver_cells):
        """Orders SQL uses LATERAL VIEW EXPLODE in a subquery."""
        sql = _extract_sql(silver_cells[3])
        assert "LATERAL VIEW EXPLODE" in sql
        # Verify EXPLODE is inside a subquery, not at top level with JOIN
        explode_pos = sql.find("LATERAL VIEW EXPLODE")
        join_pos = sql.find("LEFT JOIN")
        # The EXPLODE must be in a nested subquery before the JOIN
        assert explode_pos < join_pos

    def test_dedup_keeps_latest(self, silver_cells):
        """Orders SQL deduplicates by cart_id + product_id, keeping latest."""
        sql = _extract_sql(silver_cells[3])
        assert "ROW_NUMBER()" in sql
        assert "PARTITION BY" in sql
        assert "row_num = 1" in sql


class TestSilverQuarantine:
    def test_quarantine_null_cart_id(self, silver_cells):
        """Quarantine SQL filters records where id IS NULL."""
        sql = _extract_sql(silver_cells[5])
        assert "id IS NULL" in sql
        assert "quarantine_reason" in sql.lower() or "Missing cart ID" in sql
