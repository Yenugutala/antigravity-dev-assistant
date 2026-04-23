"""Unit tests for Gold aggregation pipeline (gold_aggregate.py)."""

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


# ── Load Gold SQL Statements ──

GOLD_PATH = os.path.join(
    os.path.dirname(__file__),
    "..", "src", "pipelines", "sales", "orders", "gold_aggregate.py"
)


def load_gold_sql():
    """Extract SQL statements from gold notebook cells."""
    with open(os.path.abspath(GOLD_PATH), "r") as f:
        source = f.read()

    cells = source.split("# COMMAND ----------")
    sql_statements = []
    for cell in cells:
        if 'spark.sql("""' in cell:
            start = cell.index('spark.sql("""') + len('spark.sql("""')
            end = cell.index('""")', start)
            sql_statements.append(cell[start:end].strip())
    return sql_statements


@pytest.fixture
def gold_sql():
    return load_gold_sql()


# ── Tests ──

class TestRevenueByCategorySQL:
    def test_category_group_by(self, gold_sql):
        """Test revenue_by_category groups by category."""
        rev_sql = gold_sql[0]
        assert "GROUP BY category" in rev_sql

    def test_beauty_revenue_uses_round_sum(self, gold_sql):
        """Test revenue calculation uses ROUND(SUM(line_total), 2)."""
        rev_sql = gold_sql[0]
        assert "ROUND(SUM(line_total), 2)" in rev_sql
        assert "total_revenue" in rev_sql

    def test_total_items_sold_metric(self, gold_sql):
        """Test total_items_sold uses SUM(quantity)."""
        rev_sql = gold_sql[0]
        assert "SUM(quantity)" in rev_sql
        assert "total_items_sold" in rev_sql

    def test_unique_products_metric(self, gold_sql):
        """Test unique_products uses COUNT(DISTINCT product_id)."""
        rev_sql = gold_sql[0]
        assert "COUNT(DISTINCT product_id)" in rev_sql
        assert "unique_products" in rev_sql

    def test_revenue_ordered_desc(self, gold_sql):
        """Test results are ordered by total_revenue DESC."""
        rev_sql = gold_sql[0]
        assert "ORDER BY total_revenue DESC" in rev_sql


class TestOrderSummarySQL:
    def test_order_summary_groups_by_date(self, gold_sql):
        """Test order_summary groups by order_date."""
        summary_sql = gold_sql[1]
        assert "GROUP BY order_date" in summary_sql

    def test_total_orders_metric(self, gold_sql):
        """Test total_orders uses COUNT(DISTINCT cart_id)."""
        summary_sql = gold_sql[1]
        assert "COUNT(DISTINCT cart_id)" in summary_sql
        assert "total_orders" in summary_sql

    def test_unique_customers_metric(self, gold_sql):
        """Test unique_customers uses COUNT(DISTINCT user_id)."""
        summary_sql = gold_sql[1]
        assert "COUNT(DISTINCT user_id)" in summary_sql
        assert "unique_customers" in summary_sql

    def test_total_revenue_metric(self, gold_sql):
        """Test total_revenue uses ROUND(SUM(line_total), 2)."""
        summary_sql = gold_sql[1]
        assert "ROUND(SUM(line_total), 2)" in summary_sql

    def test_avg_order_line_value_metric(self, gold_sql):
        """Test avg_order_line_value uses ROUND(AVG(line_total), 2)."""
        summary_sql = gold_sql[1]
        assert "ROUND(AVG(line_total), 2)" in summary_sql
        assert "avg_order_line_value" in summary_sql
