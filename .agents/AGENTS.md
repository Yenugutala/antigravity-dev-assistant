# AI Pipeline Accelerator — Customization Rules

These are the rules and guidelines that Antigravity must follow when working on the AI Pipeline Accelerator codebase.

---

## 1. Project-Wide Conventions

- **Branch Protection**: Never commit directly to `main` or `master` branch. Pushes should go to `develop`.
- **Notebook File Format**: All `.py` files in `src/` and `notebooks/` MUST start with `# Databricks notebook source` as the very first line. Use `# COMMAND ----------` to separate notebook cells.
- **Idempotency**: All pipeline code must be idempotent and re-runnable. Use `CREATE OR REPLACE TABLE` or `INSERT OVERWRITE` to ensure this.
- **Configuration**: No hardcoded values in code — always use `config.yml` for configurations (e.g., table names, schemas, API URLs).
- **Secrets**: No secrets in code — use Databricks Secrets or environment variables.
- **Metadata Columns**: Every ingestion layer table must automatically include metadata columns prefixed with `_`:
  - `_ingestion_timestamp` — ISO string of when data was ingested.
  - `_source` — source identifier (e.g., "dummyjson_api" or "fallback_sample").
  - `_batch_id` — unique batch identifier (UUID).

---

## 2. Medallion Architecture Rules

### 2.1 Bronze Layer (Raw Data Ingestion)
- **Explicit Schemas**: ALWAYS use explicit Spark `StructType` schemas — NEVER rely on schema inference.
- **API Fallback**: ALWAYS embed fallback sample data so that the pipeline can run successfully even if the source API is unreachable.
- **Preprocessing**: 
  - Strip unnecessary fields from the source data before DataFrame creation.
  - Cast ALL numerics to consistent types (e.g., `float` for double fields, `int` for integer fields) before creating DataFrame to prevent `CANNOT_MERGE_TYPE` errors.
  - Extract API response data using the `wrapper_key` defined in the spec.
- **Naming**: Write to schema `b_<pipeline>` (e.g., `b_salesorders.products`).

### 2.2 Silver Layer (Cleansing & Transformation)
- **Spark SQL Syntax**: UPPERCASE all SQL keywords (SELECT, FROM, WHERE, JOIN, GROUP BY, etc.). Columns should be in snake_case.
- **Explosion Safety**: NEVER combine `LATERAL VIEW EXPLODE` and `JOIN` in the same `FROM` clause (causes `PARSE_SYNTAX_ERROR` in Spark). Always wrap `EXPLODE` in a subquery first, then `LEFT JOIN` the result.
- **Deduplication**: Use `ROW_NUMBER() OVER (PARTITION BY <key> ORDER BY _ingestion_timestamp DESC)` and filter with `WHERE row_num = 1`.
- **Schema & Naming**: Each notebook MUST start with `CREATE SCHEMA IF NOT EXISTS s_<pipeline>`. Write to `s_<pipeline>.<table>`.
- **Execution**: Use `spark.sql(""" ... """)` for executing SQL in Python.

### 2.3 Gold Layer (Business Aggregations & Metrics)
- **Aggregations**: Business-level aggregations and metrics reading from silver and writing to gold.
- **Syntax**: UPPERCASE all SQL keywords, snake_case for all column names. Use meaningful aliases for aggregated columns (e.g., `total_revenue`, `avg_order_value`).
- **Grouping**: Always GROUP BY explicit columns (never use positional references like `GROUP BY 1`).
- **Safety**: Use `COALESCE` for nullable aggregation results. Include record counts alongside aggregated metrics.
- **Schema & Naming**: Each notebook MUST start with `CREATE SCHEMA IF NOT EXISTS g_<pipeline>`. Write to `g_<pipeline>.<table>`.

---

## 3. Notebook Organization

1. **Cell 1**: Schema creation (`CREATE SCHEMA IF NOT EXISTS`)
2. **Cell 2**: Imports (imports like `requests`, `uuid`, etc.)
3. **Cell 3**: Configuration (reading `config.yml` and initializing variables like `BATCH_ID`)
4. **Cell 4+**: Ingestion / Transformation logic cells, separated by `# COMMAND ----------`
5. **Final Cell**: Success message and verification query (e.g., displaying target table limits)

### Orchestrator Notebook (`run_pipeline.py`)
- Located in `notebooks/run_pipeline.py`.
- Uses `dbutils.notebook.run()` to invoke the pipeline notebooks sequentially: Bronze -> Silver -> Gold.
- Wrap each run in try/except for robust error handling.
- Print status and execution times after each notebook completes.

---

## 4. Testing Rules

- **Local Execution**: Tests must run locally without requiring a live Databricks cluster.
- **PySpark Stubbing**:
  - Stub PySpark types in `sys.modules` before importing pipeline modules to allow notebook loading.
  - Pre-inject standard imports (`requests`, `uuid`, `datetime`) + PySpark stubs into the module namespace dict.
- **Module Loading via Cell Execution**:
  - Load notebooks in tests by splitting the notebook source code by `# COMMAND ----------`.
  - Do NOT filter line-by-line (which breaks multi-line statements).
  - Execute cells using `exec()` inside a prepared namespace.
- **Mocking**:
  - Mock `requests.get` using `@patch("requests.get")` directly.
  - Mock `spark.sql()` and `spark.createDataFrame()`.
- **Test Structure**:
  - Use `pytest`.
  - One test file per pipeline stage (e.g., `tests/test_bronze.py` tests `bronze_ingest.py`).
  - Test success, failure (e.g., API down using fallback data), and edge cases.
- **Assertions**:
  - Verify schema matches expected `StructType`.
  - Verify row counts and deduplication logic.
  - Verify metadata fields are populated.
