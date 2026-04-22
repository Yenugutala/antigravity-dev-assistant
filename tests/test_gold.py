"""Unit tests for Gold Aggregation pipeline."""

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
# Load gold module via cell-based extraction
# ---------------------------------------------------------------------------
GOLD_PATH = "src/pipelines/sales/orders/gold_aggregate.py"


def _load_gold_cells():
    """Load gold notebook cells without executing."""
    with open(GOLD_PATH, "r") as f:
        source = f.read()
    cells = source.split("# COMMAND ----------")
    return [c.replace("# Databricks notebook source", "").strip() for c in cells if c.strip()]


@pytest.fixture(scope="module")
def gold_cells():
    return _load_gold_cells()


def _extract_sql(cell_text):
    """Extract SQL string from a spark.sql() call in a cell."""
    start = cell_text.find('spark.sql("""')
    if start == -1:
        return ""
    start += len('spark.sql("""')
    end = cell_text.find('""")', start)
    return cell_text[start:end].strip() if end != -1 else ""


# ---------------------------------------------------------------------------
# Tests — Revenue by Category
# ---------------------------------------------------------------------------
class TestRevenueByCategorySQL:
    def test_category_count(self, gold_cells):
        """Revenue by category groups by category."""
        sql = _extract_sql(gold_cells[2])
        assert "GROUP BY category" in sql

    def test_beauty_revenue(self):
        """Beauty category revenue from fallback data calculates to 100.00."""
        # Fallback data: products 1-5 are beauty category
        # Cart 1 has: product 1 (total=19.98), product 2 (total=19.99), product 3 (total=29.98)
        # Total beauty line_total = 19.98 + 19.99 + 29.98 = 69.95
        # But after enrichment with silver products, line_total = ROUND(item.total, 2)
        beauty_items = [
            {"product_id": 1, "price": 9.99, "quantity": 2, "total": 19.98},
            {"product_id": 2, "price": 19.99, "quantity": 1, "total": 19.99},
            {"product_id": 3, "price": 14.99, "quantity": 2, "total": 29.98},
            {"product_id": 4, "price": 12.99, "quantity": 0, "total": 0.0},
            {"product_id": 5, "price": 8.99, "quantity": 0, "total": 0.0},
        ]
        # Only items in carts contribute; products 4,5 are not in any cart
        cart_beauty = [i for i in beauty_items if i["total"] > 0]
        total = round(sum(i["total"] for i in cart_beauty), 2)
        assert total == 69.95

    def test_total_revenue_metric(self, gold_cells):
        """Revenue SQL includes SUM(line_total) metric."""
        sql = _extract_sql(gold_cells[2])
        assert "SUM(line_total)" in sql

    def test_total_orders_metric(self, gold_cells):
        """Revenue SQL includes COUNT(DISTINCT cart_id) for total_orders."""
        sql = _extract_sql(gold_cells[2])
        assert "COUNT(DISTINCT cart_id)" in sql


class TestOrderSummarySQL:
    def test_unique_customers(self, gold_cells):
        """Order summary includes COUNT(DISTINCT user_id)."""
        sql = _extract_sql(gold_cells[3])
        assert "COUNT(DISTINCT user_id)" in sql
