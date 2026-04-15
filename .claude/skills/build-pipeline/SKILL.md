---
name: build-pipeline
description: >
  Master orchestrator skill that reads a pipeline specification file and automatically
  generates all artifacts: HLD document, LLD document, Bronze PySpark code, Silver Spark SQL code,
  Gold Spark SQL code, unit tests, and a Databricks run notebook.
  Invoke with "build pipeline", "generate pipeline", or when the user provides a spec file path.
---

# Build Pipeline — Master Generator

## Purpose
Read a pipeline specification (Markdown + YAML frontmatter) and auto-generate all artifacts
for a Databricks Medallion Architecture pipeline (Bronze → Silver → Gold).

## Usage
```
/build-pipeline specs/<domain>-<entity>-spec.md
```

## Process

### Step 1: Read and Validate Spec
1. Read the spec file provided as argument
2. Parse YAML frontmatter to extract: domain, entity, source config, schemas, business rules
3. Validate all required fields are present
4. If any field is missing, ask the user to fill it in before proceeding

### Step 2: Generate HLD Document
Create `docs/<domain>-<entity>-hld.md` with:
- Solution Overview (1 paragraph)
- Architecture: Bronze (PySpark) → Silver (Spark SQL) → Gold (Spark SQL)
- Data Flow Diagram (text-based)
- Technology Stack: Databricks, Delta Lake, PySpark, Spark SQL
- Table Structure: `default.<entity>_bronze`, `default.<entity>_silver`, `default.<entity>_gold_*`
- Data Quality Strategy
- Monitoring approach

### Step 3: Generate LLD Document
Create `docs/<domain>-<entity>-lld.md` with:
- Complete schema definitions for each layer (Bronze, Silver, Gold)
- Column-by-column mapping from Bronze → Silver
- Transformation logic pseudocode
- Deduplication logic with SQL examples
- Aggregation definitions with SQL examples
- Data quality check specifications
- Error handling and quarantine table design

### Step 4: Generate Bronze Pipeline Code (PySpark)
Create `src/pipelines/<domain>/<entity>/bronze_ingest.py`:
- MUST start with `# Databricks notebook source`
- Fetch data from source (API call, file read, etc.)
- Convert to Spark DataFrame
- Add metadata columns: `_ingestion_timestamp`, `_source`, `_batch_id`
- Write as Delta table: `default.<entity>_bronze` (and related tables)
- Display sample data at the end
- Include error handling with try/except
- Read config from `config.yml`

### Step 5: Generate Silver Pipeline Code (Spark SQL)
Create `src/pipelines/<domain>/<entity>/silver_cleanse.py`:
- MUST start with `# Databricks notebook source`
- Use `spark.sql()` for all transformations (PySQL pattern)
- Apply cleansing rules from spec (TRIM, CAST, COALESCE)
- Deduplicate using ROW_NUMBER() OVER (PARTITION BY key ORDER BY timestamp DESC)
- Quarantine invalid records to `default.<entity>_quarantine`
- Write cleansed data to `default.<entity>_silver` (and related tables)
- Display sample data and row counts at the end

### Step 6: Generate Gold Pipeline Code (Spark SQL)
Create `src/pipelines/<domain>/<entity>/gold_aggregate.py`:
- MUST start with `# Databricks notebook source`
- Use `spark.sql()` for all aggregations
- Create Gold tables as defined in spec's gold.aggregations
- Include business-readable column aliases
- Display final aggregated results at the end

### Step 7: Generate Master Notebook
Create `notebooks/run_pipeline.py`:
- MUST start with `# Databricks notebook source`
- Run Bronze, Silver, Gold notebooks in sequence using `%run` magic commands
- Display summary: tables created, row counts, sample Gold data
- Include timing for each stage

### Step 8: Generate Unit Tests
Create `tests/test_bronze.py`, `tests/test_silver.py`, `tests/test_gold.py`:
- Use pytest
- Test transformation functions in isolation
- Test schema validation
- Test data quality checks
- Mock API calls for Bronze tests
- DO NOT add `# Databricks notebook source` header to test files

### Step 9: Print Summary
After generating all files, print:
- List of all generated files
- How to run tests: `pytest tests/ -v`
- How to push to Databricks: `git push → Databricks Repos → Pull`
- How to run in Databricks: Open `notebooks/run_pipeline.py` → Run All

## Code Generation Rules
1. All .py files in `src/` and `notebooks/` MUST start with `# Databricks notebook source`
2. Use `# COMMAND ----------` to separate notebook cells
3. Use `display()` instead of `print()` for DataFrames in notebooks
4. Read config from `config.yml` using yaml.safe_load
5. No hardcoded paths — use config values
6. All SQL uses UPPERCASE keywords
7. All Python uses snake_case
8. Add type hints to all functions
9. Include docstrings for all functions
10. Handle errors gracefully with try/except and logging
