---
paths:
  - "tests/**/*.py"
---

# Testing Rules

## Local Execution (No Databricks Cluster)
- Always stub PySpark types in `sys.modules` before importing pipeline modules
- Pre-inject standard imports (`requests`, `uuid`, `datetime`) + PySpark stubs into module dict

## Module Loading
- Use cell-based extraction: split notebook source by `# COMMAND ----------`
- Do NOT use line-by-line filtering — it breaks multi-line statements
- Execute cells with `exec()` in a prepared module namespace

## Mocking
- Use `@patch("requests.get")` — NOT `@patch("module_name.requests.get")`
- Mock `spark.sql()` and `spark.createDataFrame()` for SQL-based tests
- Mock API responses with realistic DummyJSON data structure

## Test Structure
- Use `pytest` as the test framework
- Each test file corresponds to one pipeline file (e.g., `test_bronze.py` tests `bronze_ingest.py`)
- Test both success and failure paths
- Test with fallback/sample data scenarios

## Assertions
- Verify DataFrame schema matches expected StructType
- Verify row counts after transformations
- Verify deduplication logic removes duplicates correctly
- Verify metadata columns (_ingestion_timestamp, _source) are populated
