---
paths:
  - "src/**/bronze*.py"
---

# Bronze Layer Rules

## Schema & Data
- ALWAYS use explicit `StructType` schemas — NEVER rely on schema inference
- ALWAYS embed fallback sample data so pipeline works even if source is unreachable
- Strip unnecessary fields from source data before DataFrame creation
- Cast ALL numerics to consistent types before creating DataFrame (prevents CANNOT_MERGE_TYPE)
- Extract API response data using the `wrapper_key` defined in the spec

## Metadata Columns
- `_ingestion_timestamp` — when data was ingested
- `_source` — source identifier (e.g., "dummyjson_api" or "fallback_sample")
- `_batch_id` — unique batch identifier (UUID)

## File Format
- Must start with `# Databricks notebook source`
- Use `# COMMAND ----------` to separate notebook cells
- Write to `b_<pipeline>.<table>` (e.g., `b_salesorders.products`)
