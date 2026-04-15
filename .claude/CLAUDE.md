# AI Pipeline Accelerator — Project Conventions

## Overview
Spec-driven data pipeline generator using Databricks Medallion Architecture (Bronze → Silver → Gold).
User writes a spec → runs `/build-pipeline` → all code, tests, docs, and notebooks auto-generate.

## Architecture
- **Bronze**: PySpark code — raw data ingestion from APIs/files
- **Silver**: Spark SQL (PySQL) — cleansing, deduplication, conformance
- **Gold**: Spark SQL (PySQL) — business aggregations and metrics

## Rules
- Never commit directly to main/master branch
- All .py files in `src/` and `notebooks/` MUST start with `# Databricks notebook source`
- All pipeline code must be idempotent and re-runnable
- No hardcoded values — use config.yml for all configuration
- No secrets in code — use Databricks Secrets or environment variables
- Tables use naming: `default.<entity>_<layer>` (e.g., `default.orders_bronze`)
- Metadata columns prefixed with `_` (e.g., `_ingestion_timestamp`, `_source`)

## Code Style
- Python: snake_case for files and functions
- SQL: UPPERCASE keywords, snake_case for columns
- Always add type hints to function signatures
- Keep functions small and focused

## Testing
- pytest for all tests
- Tests must be runnable locally (no Databricks cluster needed for unit tests)
