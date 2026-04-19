---
paths:
  - "src/**/bronze*.py"
---

# Bronze Layer Rules

## API Source
- Use `https://dummyjson.com` — FakeStore API is DEAD
- DummyJSON wraps responses: `{"products": [...]}`, `{"carts": [...]}`, `{"users": [...]}`
- Cart items have `id`/`price`/`quantity`/`total`; users have `firstName`/`lastName` (not nested); no `date` on carts

## Schema & Data
- ALWAYS use explicit `StructType` schemas — NEVER rely on schema inference
- ALWAYS embed fallback sample data so demo never fails even if API is down
- Strip unnecessary fields from API response before DataFrame creation
- Cast ALL numerics to `float` before creating DataFrame (prevents CANNOT_MERGE_TYPE)

## Metadata Columns
- `_ingestion_timestamp` — when data was ingested
- `_source` — source identifier (e.g., "dummyjson_api" or "fallback_sample")
- `_batch_id` — unique batch identifier (UUID)

## File Format
- Must start with `# Databricks notebook source`
- Use `# COMMAND ----------` to separate notebook cells
- Write to `b_<pipeline>.<table>` (e.g., `b_salesorders.products`)
