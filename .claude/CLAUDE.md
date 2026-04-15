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
- Three schemas per pipeline: `b_<pipeline>` (Bronze), `s_<pipeline>` (Silver), `g_<pipeline>` (Gold)
- Table naming: `<schema>.<table>` (e.g., `b_salesorders.products`, `s_salesorders.orders`, `g_salesorders.revenue_by_category`)
- Each pipeline notebook MUST start with `CREATE SCHEMA IF NOT EXISTS <schema_name>`
- Metadata columns prefixed with `_` (e.g., `_ingestion_timestamp`, `_source`)
- Always use explicit Spark `StructType` schemas — never rely on schema inference
- Always embed fallback sample data for API sources
- Preprocess API data: strip unnecessary fields, cast all numerics to consistent types

## Code Style
- Python: snake_case for files and functions
- SQL: UPPERCASE keywords, snake_case for columns
- Always add type hints to function signatures
- Keep functions small and focused

## Testing
- pytest for all tests
- Tests must be runnable locally (no Databricks cluster needed for unit tests)

## Demo Flow
- BRD at `docs/business-requirement.md` describes business need (no technical details)
- Three separate specs: `specs/bronze-spec.md`, `specs/silver-spec.md`, `specs/gold-spec.md`
- Code generation regenerates from specs; code MUST work first time
