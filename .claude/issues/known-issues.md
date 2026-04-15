# Known Issues & Lessons Learned

## Issue #1: PySpark not installed locally
- **When**: Running `pytest tests/test_bronze.py`
- **Error**: `ModuleNotFoundError: No module named 'pyspark'`
- **Fix**: Stub PySpark types in `sys.modules` before importing bronze module. Inject `StructType`, `StructField`, etc. as local stub classes.
- **Prevention**: Always stub PySpark types when generating Bronze tests for local execution.

## Issue #2: Bronze module loading — cell-based extraction needed
- **When**: Loading `bronze_ingest.py` in test via line-by-line filtering
- **Error**: `IndentationError: unexpected indent` — skipping individual lines breaks Python indentation
- **Fix**: Split source by `# COMMAND ----------` markers and skip entire cells (not individual lines) that contain Databricks-specific code (`spark.`, `display(`, `.write.`, `# MAGIC`).
- **Prevention**: Always use cell-based extraction when loading Databricks notebooks in tests.

## Issue #3: Missing module injections for Bronze tests
- **When**: exec-ing bronze module code in test
- **Error**: `NameError: name 'uuid' is not defined`
- **Fix**: Pre-inject all standard library imports (`requests`, `uuid`, `datetime`) and PySpark type stubs into the module dict before exec.
- **Prevention**: When filtering out import cells (because they contain PySpark imports), inject all their standard library imports manually.

## Issue #4: Mock patch target for dynamically loaded module
- **When**: `@patch("bronze_ingest.requests.get")` in API fetch tests
- **Error**: `ModuleNotFoundError: No module named 'bronze_ingest'`
- **Fix**: Use `@patch("requests.get")` instead — patch the actual `requests` module since the bronze module shares it.
- **Prevention**: For dynamically loaded modules (via exec), patch the real module, not the virtual module name.

## Issue #5: Databricks Genie Code modifies cells
- **When**: Running `run_pipeline.py` in Databricks, clicking "Diagnose error"
- **Error**: Genie Code changed `%run ../src/pipelines/sales/orders/silver_cleanse` to `%sql ../src/pipelines/sales/orders/silver_cleanse`, causing `PARSE_SYNTAX_ERROR`
- **Fix**: Hard Reset in Databricks Repos to restore original code from Git. Avoid using Genie Code "Diagnose error" on `%run` cells.
- **Prevention**: Warn user that Genie Code Quick Fix can modify cells. Turn off "Genie Code Quick Fix" before running pipelines. Always Hard Reset after Genie makes unwanted changes.

## Issue #6: Old tables in default schema from previous runs
- **When**: Previous pipeline used `default` schema with `_bronze`/`_silver` suffixes
- **Error**: Stale tables like `default.carts_bronze`, `default.products_silver` remain
- **Fix**: Run cleanup SQL to drop old tables and schemas before demo
- **Prevention**: Always use schema-qualified names (`b_salesorders`, `s_salesorders`, `g_salesorders`). Run cleanup notebook before demos.

## Issue #7: LATERAL VIEW EXPLODE + LEFT JOIN — Spark SQL syntax error
- **When**: Running Silver orders query in Databricks
- **Error**: `[PARSE_SYNTAX_ERROR] Syntax error at or near 'LEFT': missing ')'. SQLSTATE: 42601`
- **Cause**: Spark SQL does NOT allow `LEFT JOIN` directly after `LATERAL VIEW EXPLODE` in the same FROM clause. The parser expects the FROM clause to end after LATERAL VIEW.
- **Bad SQL**: `FROM carts c LATERAL VIEW EXPLODE(c.products) AS item LEFT JOIN products p ON ...`
- **Fix**: Wrap the EXPLODE in a subquery first, then LEFT JOIN on the outer query:
  ```sql
  FROM (
      SELECT ... FROM carts c LATERAL VIEW EXPLODE(c.products) AS item
  ) e
  LEFT JOIN products p ON e.product_id = p.product_id
  ```
- **Prevention**: NEVER combine LATERAL VIEW EXPLODE with JOIN in the same FROM clause. Always use a subquery for EXPLODE first, then JOIN the result.
