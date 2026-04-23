"""Unit tests for Silver cleansing pipeline (silver_cleanse.py)."""

import sys
import os
import types

import pytest

# ── PySpark Stubs ──

pyspark_mod = types.ModuleType("pyspark")
sql_mod = types.ModuleType("pyspark.sql")
types_mod = types.ModuleType("pyspark.sql.types")
functions_mod = types.ModuleType("pyspark.sql.functions")

sys.modules.setdefault("pyspark", pyspark_mod)
sys.modules.setdefault("pyspark.sql", sql_mod)
sys.modules.setdefault("pyspark.sql.types", types_mod)
sys.modules.setdefault("pyspark.sql.functions", functions_mod)


# ── Load Silver SQL Statements ──

SILVER_PATH = os.path.join(
    os.path.dirname(__file__),
    "..", "src", "pipelines", "sales", "orders", "silver_cleanse.py"
)


def load_silver_sql():
    """Extract SQL statements from silver notebook cells."""
    with open(os.path.abspath(SILVER_PATH), "r") as f:
        source = f.read()

    cells = source.split("# COMMAND ----------")
    sql_statements = []
    for cell in cells:
        # Find spark.sql(""" ... """) calls
        if 'spark.sql("""' in cell:
            start = cell.index('spark.sql("""') + len('spark.sql("""')
            end = cell.index('""")', start)
            sql_statements.append(cell[start:end].strip())
    return sql_statements


@pytest.fixture
def silver_sql():
    return load_silver_sql()


# ── Tests ──

class TestProductsCleansing:
    def test_products_trims_title(self, silver_sql):
        """Test that products SQL applies TRIM to title."""
        products_sql = silver_sql[0]  # First SQL = products
        assert "TRIM(title)" in products_sql

    def test_products_lowercases_category(self, silver_sql):
        """Test that products SQL applies LOWER and TRIM to category."""
        products_sql = silver_sql[0]
        assert "LOWER(TRIM(category))" in products_sql

    def test_products_casts_numeric(self, silver_sql):
        """Test that products SQL casts price and rating to DOUBLE."""
        products_sql = silver_sql[0]
        assert "CAST(price AS DOUBLE)" in products_sql
        assert "CAST(rating AS DOUBLE)" in products_sql


class TestOrdersExplode:
    def test_orders_uses_lateral_view_explode(self, silver_sql):
        """Test that orders SQL uses LATERAL VIEW EXPLODE for cart items."""
        orders_sql = silver_sql[1]  # Second SQL = orders
        assert "LATERAL VIEW EXPLODE" in orders_sql

    def test_orders_explode_in_subquery(self, silver_sql):
        """Test EXPLODE is wrapped in subquery, not combined with JOIN."""
        orders_sql = silver_sql[1]
        # The LATERAL VIEW EXPLODE should be inside a subquery (FROM clause)
        # and JOIN should be on the outer query
        explode_pos = orders_sql.index("LATERAL VIEW EXPLODE")
        join_pos = orders_sql.index("LEFT JOIN")
        # EXPLODE comes before JOIN (in subquery)
        assert explode_pos < join_pos


class TestDeduplication:
    def test_products_dedup_keeps_latest(self, silver_sql):
        """Test products uses ROW_NUMBER with DESC timestamp for dedup."""
        products_sql = silver_sql[0]
        assert "ROW_NUMBER()" in products_sql
        assert "PARTITION BY id" in products_sql
        assert "_ingestion_timestamp DESC" in products_sql
        assert "row_num = 1" in products_sql

    def test_orders_dedup_by_cart_product(self, silver_sql):
        """Test orders dedup partitions by cart_id and product_id."""
        orders_sql = silver_sql[1]
        assert "PARTITION BY e.cart_id, e.product_id" in orders_sql
        assert "row_num = 1" in orders_sql


class TestQuarantine:
    def test_quarantine_filters_null_cart_id(self, silver_sql):
        """Test quarantine SQL captures records where id IS NULL."""
        quarantine_sql = silver_sql[3]  # Fourth SQL = quarantine
        assert "WHERE id IS NULL" in quarantine_sql
        assert "orders_quarantine" in quarantine_sql
        assert "Missing cart ID" in quarantine_sql
