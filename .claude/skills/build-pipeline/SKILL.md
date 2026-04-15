---
name: build-pipeline
description: >
  Master orchestrator skill that reads pipeline specification files and automatically
  generates all artifacts: HLD document, Bronze PySpark code, Silver Spark SQL code,
  Gold Spark SQL code, unit tests, and a Databricks run notebook.
  Invoke with "build pipeline", "generate pipeline", or when the user provides a spec file path.
---

# Build Pipeline — Master Generator

## Purpose
Read pipeline specifications (Markdown + YAML frontmatter) and auto-generate all artifacts
for a Databricks Medallion Architecture pipeline (Bronze → Silver → Gold).

## Usage
```
/build-pipeline specs/bronze-spec.md specs/silver-spec.md specs/gold-spec.md
```

## Process

### Step 1: Read and Validate Specs
1. Read the three spec files: bronze-spec.md, silver-spec.md, gold-spec.md
2. Parse YAML frontmatter to extract: domain, entity, source config, schemas, business rules
3. Validate all required fields are present
4. Extract schema names: `b_<pipeline>`, `s_<pipeline>`, `g_<pipeline>`

### Step 2: Generate HLD Document
Create `docs/<domain>-<entity>-hld.md` with:
- Solution Overview (1 paragraph)
- Architecture: Bronze (PySpark) → Silver (Spark SQL) → Gold (Spark SQL)
- Data Flow Diagram (text-based)
- Technology Stack: Databricks, Delta Lake, PySpark, Spark SQL
- Table Structure using schema naming: `b_<pipeline>.<table>`, `s_<pipeline>.<table>`, `g_<pipeline>.<table>`

### Step 3: Generate Bronze Pipeline Code (PySpark)
Create `src/pipelines/<domain>/<entity>/bronze_ingest.py`:
- MUST start with `# Databricks notebook source`
- MUST begin with `CREATE SCHEMA IF NOT EXISTS b_<pipeline>`
- MUST define explicit `StructType` schemas for ALL DataFrames (never rely on schema inference)
- MUST embed fallback sample data so pipeline works if API is down
- MUST include clean helper functions that strip unnecessary fields and cast all numerics (`float()` for prices/ratings, `int()` for IDs)
- Fetch data from source API using `requests.get()` with `wrapper_key` extraction
- Add metadata columns: `_ingestion_timestamp`, `_source`, `_batch_id`
- Write as Delta table: `b_<pipeline>.<table>` (e.g., `b_salesorders.products`)
- Display sample data at the end

### Step 4: Generate Silver Pipeline Code (Spark SQL)
Create `src/pipelines/<domain>/<entity>/silver_cleanse.py`:
- MUST start with `# Databricks notebook source`
- MUST begin with `CREATE SCHEMA IF NOT EXISTS s_<pipeline>`
- Use `spark.sql()` for all transformations (PySQL pattern)
- Apply cleansing rules from spec (TRIM, CAST, LOWER)
- Deduplicate using `ROW_NUMBER() OVER (PARTITION BY key ORDER BY timestamp DESC)`
- Use `LATERAL VIEW EXPLODE` for array columns
- Quarantine invalid records to `s_<pipeline>.orders_quarantine`
- Write cleansed data to `s_<pipeline>.<table>`

### Step 5: Generate Gold Pipeline Code (Spark SQL)
Create `src/pipelines/<domain>/<entity>/gold_aggregate.py`:
- MUST start with `# Databricks notebook source`
- MUST begin with `CREATE SCHEMA IF NOT EXISTS g_<pipeline>`
- Use `spark.sql()` for all aggregations
- Create Gold tables as defined in gold spec's aggregations
- Read from `s_<pipeline>.<table>`, write to `g_<pipeline>.<table>`
- Display final aggregated results at the end

### Step 6: Generate Master Notebook
Create `notebooks/run_pipeline.py`:
- MUST start with `# Databricks notebook source`
- Run Bronze, Silver, Gold notebooks in sequence using `%run` magic commands
- Display summary with schema-qualified table names and row counts
- Include timing for each stage

### Step 7: Generate Unit Tests
Create `tests/test_bronze.py`, `tests/test_silver.py`, `tests/test_gold.py`:
- Use pytest
- Test transformation functions in isolation
- Test schema validation
- Mock API calls for Bronze tests
- DO NOT add `# Databricks notebook source` header to test files
- Gold test `test_beauty_revenue` should use expected value `100.00` (intentional demo failure — actual is `99.93`)

### Step 8: Print Summary
After generating all files, print:
- List of all generated files
- How to run tests: `pytest tests/ -v`
- How to push to Databricks: `git push → Databricks Repos → Pull`
- How to run in Databricks: Open `notebooks/run_pipeline.py` → Run All

## Code Generation Rules
1. All .py files in `src/` and `notebooks/` MUST start with `# Databricks notebook source`
2. Use `# COMMAND ----------` to separate notebook cells
3. Use `display()` instead of `print()` for DataFrames in notebooks
4. No hardcoded paths — use config values
5. All SQL uses UPPERCASE keywords, snake_case for columns
6. All Python uses snake_case with type hints and docstrings
7. Schema naming: `b_<pipeline>` (Bronze), `s_<pipeline>` (Silver), `g_<pipeline>` (Gold)
8. Every notebook starts with `CREATE SCHEMA IF NOT EXISTS <schema>`
9. Always use explicit `StructType` schemas — NEVER rely on Spark schema inference
10. Always embed fallback sample data for API sources
11. Always preprocess API data: strip unnecessary fields, cast numerics consistently
12. API responses wrapped in keys (e.g., `{"products": [...]}`); extract with `wrapper_key`
