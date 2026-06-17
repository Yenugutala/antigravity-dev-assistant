"""Unit tests for Silver layer — silver_cleanse.py"""

import sys
import types


# ---------------------------------------------------------------------------
# PySpark stubs
# ---------------------------------------------------------------------------
_pyspark_types_mod = types.ModuleType("pyspark.sql.types")
for _name in ("StructType", "StructField", "IntegerType", "StringType", "DoubleType", "ArrayType"):
    setattr(_pyspark_types_mod, _name, type(_name, (), {"__init__": lambda self, *a, **kw: None}))

sys.modules.setdefault("pyspark", types.ModuleType("pyspark"))
sys.modules.setdefault("pyspark.sql", types.ModuleType("pyspark.sql"))
sys.modules.setdefault("pyspark.sql.types", _pyspark_types_mod)

# ---------------------------------------------------------------------------
# Load silver module — extract SQL strings from notebook cells
# ---------------------------------------------------------------------------
_SRC = open("src/pipelines/sales/orders/silver_cleanse.py").read()
_CELLS = _SRC.split("# COMMAND ----------")

# Collect all spark.sql() calls as raw SQL strings
_SQL_STATEMENTS: list[str] = []
for _cell in _CELLS:
    stripped = _cell.strip()
    if 'spark.sql("""' in stripped or "spark.sql('''" in stripped:
        # Extract SQL between triple quotes
        for delim in ('"""', "'''"):
            start_marker = f"spark.sql({delim}"
            if start_marker in stripped:
                start_idx = stripped.index(start_marker) + len(start_marker)
                end_idx = stripped.index(delim, start_idx)
                sql = stripped[start_idx:end_idx].strip()
                _SQL_STATEMENTS.append(sql)
                break


# ===== TESTS =============================================================

class TestTrimAndLower:
    """Test string cleansing rules in silver SQL."""

    def test_products_title_trimmed(self):
        """Products SQL should TRIM the title column."""
        products_sql = [s for s in _SQL_STATEMENTS if "s_salesorders.products" in s and "product_id" in s]
        assert len(products_sql) > 0
        sql = products_sql[0]
        assert "TRIM(title)" in sql

    def test_products_category_lower_and_trim(self):
        """Products SQL should LOWER and TRIM the category column."""
        products_sql = [s for s in _SQL_STATEMENTS if "s_salesorders.products" in s and "product_id" in s]
        assert len(products_sql) > 0
        sql = products_sql[0]
        assert "LOWER(TRIM(category))" in sql


class TestCastNumeric:
    """Test numeric casting rules."""

    def test_price_cast_to_double(self):
        """Products SQL should CAST price to DOUBLE."""
        products_sql = [s for s in _SQL_STATEMENTS if "s_salesorders.products" in s and "product_id" in s]
        assert len(products_sql) > 0
        sql = products_sql[0]
        assert "CAST(price AS DOUBLE)" in sql

    def test_rating_cast_to_double(self):
        """Products SQL should CAST rating to DOUBLE."""
        products_sql = [s for s in _SQL_STATEMENTS if "s_salesorders.products" in s and "product_id" in s]
        assert len(products_sql) > 0
        sql = products_sql[0]
        assert "CAST(rating AS DOUBLE)" in sql


class TestExplodeCart:
    """Test cart explosion into order line items."""

    def test_orders_uses_lateral_view_explode(self):
        """Orders SQL should use LATERAL VIEW EXPLODE for cart products."""
        orders_sql = [s for s in _SQL_STATEMENTS if "s_salesorders.orders" in s and "cart_id" in s and "quarantine" not in s]
        assert len(orders_sql) > 0
        sql = orders_sql[0]
        assert "LATERAL VIEW EXPLODE" in sql

    def test_explode_wrapped_in_subquery(self):
        """LATERAL VIEW EXPLODE must be in a subquery, not combined with JOIN."""
        orders_sql = [s for s in _SQL_STATEMENTS if "s_salesorders.orders" in s and "cart_id" in s and "quarantine" not in s]
        assert len(orders_sql) > 0
        sql = orders_sql[0]
        # The EXPLODE should be inside a subquery (parenthesized FROM)
        # Verify LEFT JOIN is NOT in the same FROM clause as LATERAL VIEW
        explode_idx = sql.index("LATERAL VIEW EXPLODE")
        left_join_idx = sql.index("LEFT JOIN")
        # There should be a closing paren between EXPLODE and LEFT JOIN
        between = sql[explode_idx:left_join_idx]
        assert ")" in between, "EXPLODE must be wrapped in subquery before JOIN"


class TestDedupKeepsLatest:
    """Test deduplication logic."""

    def test_products_dedup_by_id(self):
        """Products should be deduplicated by id with ROW_NUMBER."""
        products_sql = [s for s in _SQL_STATEMENTS if "s_salesorders.products" in s and "product_id" in s]
        assert len(products_sql) > 0
        sql = products_sql[0]
        assert "ROW_NUMBER()" in sql
        assert "PARTITION BY id" in sql
        assert "row_num = 1" in sql

    def test_orders_dedup_by_cart_product(self):
        """Orders should be deduplicated by cart_id + product_id."""
        orders_sql = [s for s in _SQL_STATEMENTS if "s_salesorders.orders" in s and "cart_id" in s and "quarantine" not in s]
        assert len(orders_sql) > 0
        sql = orders_sql[0]
        assert "ROW_NUMBER()" in sql
        assert "cart_id" in sql
        assert "product_id" in sql


class TestQuarantineNullCartId:
    """Test quarantine logic for invalid records."""

    def test_quarantine_filters_null_ids(self):
        """Quarantine SQL should filter WHERE id IS NULL."""
        quarantine_sql = [s for s in _SQL_STATEMENTS if "orders_quarantine" in s]
        assert len(quarantine_sql) > 0
        sql = quarantine_sql[0]
        assert "id IS NULL" in sql

    def test_quarantine_includes_reason(self):
        """Quarantine should include a reason column."""
        quarantine_sql = [s for s in _SQL_STATEMENTS if "orders_quarantine" in s]
        assert len(quarantine_sql) > 0
        sql = quarantine_sql[0]
        assert "quarantine_reason" in sql
