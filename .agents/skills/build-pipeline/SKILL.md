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
Read pipeline specifications and auto-generate all artifacts for a Databricks Medallion Architecture pipeline (Bronze → Silver → Gold) matching the rules defined in `.agents/AGENTS.md`.

## Usage
Provide the paths to the specification files:
```
specs/bronze-spec.md specs/silver-spec.md specs/gold-spec.md
```

## Process

### Step 1: Read and Validate Specs
1. Read the three spec files: bronze-spec.md, silver-spec.md, gold-spec.md
2. Parse Markdown tables to extract: domain, entity, source config, schemas, business rules
3. Validate all required fields are present
4. Extract schema names: `b_<pipeline>`, `s_<pipeline>`, `g_<pipeline>`

### Step 2: Generate HLD Document
Create `docs/<domain>-<entity>-hld.md` with:
- Solution Overview (1 paragraph)
- Architecture: Bronze (PySpark) → Silver (Spark SQL) → Gold (Spark SQL)
- Data Flow Diagram (text-based)
- Technology Stack: Databricks, Delta Lake, PySpark, Spark SQL
- Table Structure using schema-qualified names from specs

### Step 3: Generate Bronze Pipeline Code (PySpark)
Create `src/pipelines/<domain>/<entity>/bronze_ingest.py`
- Follow Bronze Layer rules in `.agents/AGENTS.md`
- Use schemas and endpoints defined in bronze-spec.md
- Fetch data from source API, add metadata columns, write as Delta tables
- Display sample data at the end

### Step 4: Generate Silver Pipeline Code (Spark SQL)
Create `src/pipelines/<domain>/<entity>/silver_cleanse.py`
- Follow Silver Layer rules in `.agents/AGENTS.md`
- Apply cleansing and transformation rules from silver-spec.md
- Write cleansed data to `s_<pipeline>.<table>`

### Step 5: Generate Gold Pipeline Code (Spark SQL)
Create `src/pipelines/<domain>/<entity>/gold_aggregate.py`
- Follow Gold Layer rules in `.agents/AGENTS.md`
- Create aggregation tables defined in gold-spec.md
- Read from silver tables, write to gold tables
- Display final aggregated results at the end

### Step 6: Generate Master Notebook
Create `notebooks/run_pipeline.py`
- Follow Notebook rules in `.agents/AGENTS.md`
- Run Bronze → Silver → Gold notebooks in sequence
- Display summary with table names and row counts
- Include timing for each stage

### Step 7: Generate Unit Tests (~5 per layer)
Create `tests/test_bronze.py`, `tests/test_silver.py`, `tests/test_gold.py`
- Follow Testing rules in `.agents/AGENTS.md`
- DO NOT add `# Databricks notebook source` header to test files
- Generate ~5 focused tests per layer (~15 total) — keep lean for fast demo
- Gold test `test_beauty_revenue` should use expected value `100.00`

**Bronze tests (~5)**: clean_product, clean_cart, clean_user, schema fields, API fallback
**Silver tests (~5)**: trim/lower, cast numeric, explode cart, dedup keeps latest, quarantine null cart_id
**Gold tests (~5)**: category count, beauty revenue, total revenue, total orders, unique customers

### Step 8: Print Summary
After generating ALL files (code + tests), print:
- List of all 8 generated files (5 code + 3 test)
- How to run tests: `pytest tests/ -v`
- How to push to Databricks: `git push → Databricks Repos → Pull`
- How to run in Databricks: Open `notebooks/run_pipeline.py` → Run All
