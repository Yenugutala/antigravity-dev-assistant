---
paths:
  - "notebooks/**/*.py"
---

# Databricks Notebook Rules

## File Header
- Every .py file MUST start with `# Databricks notebook source` as the very first line
- Use `# COMMAND ----------` to separate cells

## Cell Organization
1. First cell: Schema creation (`CREATE SCHEMA IF NOT EXISTS`)
2. Import cells: All imports
3. Configuration cells: Read from config.yml
4. Logic cells: Main pipeline logic
5. Final cell: Success message / validation query

## Orchestration Notebook (run_pipeline.py)
- Uses `dbutils.notebook.run()` to call pipeline notebooks sequentially
- Order: Bronze -> Silver -> Gold
- Include error handling with try/except around each notebook run
- Print status after each notebook completes

## Best Practices
- All code must be idempotent and re-runnable
- No hardcoded values — use config.yml
- No secrets in code — use Databricks Secrets or environment variables
- Use `CREATE OR REPLACE TABLE` or `INSERT OVERWRITE` for idempotency
