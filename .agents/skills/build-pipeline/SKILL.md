---
name: build-pipeline
description: >
  Master orchestrator skill that reads pipeline specification files and automatically
  generates all artifacts: HLD document, Bronze PySpark code, Silver Spark SQL code,
  Gold Spark SQL code, unit tests, and a Databricks run notebook.
  Invoke with "build pipeline", "generate pipeline", "./build-pipeline skill", or when the user provides a spec file path.
---

# Build Pipeline — Master Generator

## Purpose
Enables automated generation of a production-ready Databricks Medallion Architecture pipeline (Bronze → Silver → Gold) by reading markdown-based pipeline specifications. It enforces consistent design pattern compliance, local testing setups, zero-dependency runtime helpers, and Databricks job path resolution rules.

## Usage
Triggered when the user says:
- `./build-pipeline skill` (loads this skill and triggers auto-generation using the default spec files: `specs/bronze-spec.md`, `specs/silver-spec.md`, and `specs/gold-spec.md`)
- `build pipeline` or `generate pipeline`
- Or provides the paths to the specification files:
```bash
specs/bronze-spec.md specs/silver-spec.md specs/gold-spec.md
```

---

## Detailed Implementation Process

### Step 1: Read and Validate Specifications & Rules
1. **Rule Verification**: Read [AGENTS.md](file:///.agents/AGENTS.md) to ensure absolute alignment with all project-wide naming conventions, metadata columns, medallion structures, path resolutions, zero-dependency config parsing, and testing rules.
2. **Source Parsing**: Read the spec files (`bronze-spec.md`, `silver-spec.md`, `gold-spec.md`).
3. **Metadata Extraction**: Parse Markdown tables to extract domain, entity, API endpoint configs, source wrapper keys, data schemas, and custom business validation rules.
4. **Target Schema Names**: Construct proper qualified schema names following the conventions:
   - Bronze: `b_<pipeline>` (e.g., `b_sales.orders`)
   - Silver: `s_<pipeline>` (e.g., `s_sales.orders_clean`)
   - Gold: `g_<pipeline>` (e.g., `g_sales.order_metrics`)

### Step 2: Generate High-Level Design (HLD) Document
Create `docs/<domain>-<entity>-hld.md` with:
- **Solution Overview**: High-level overview of the ingestion and transformation logic.
- **Architecture Diagram**: A text-based flow diagram tracing Bronze → Silver → Gold.
- **Technology Stack**: Specify Databricks, Delta Lake, PySpark, Spark SQL, and testing frameworks.
- **Table Catalog**: Schema definition lists matching the exact structure from the specifications.

### Step 3: Generate Bronze Pipeline Code (PySpark)
Create `src/pipelines/<domain>/<entity>/bronze_ingest.py`:
- **Header Requirement**: Start with `# Databricks notebook source` on line 1.
- **Workspace Path Resolution**: Resolve the absolute path to `config.yml` using the hybrid context check (looks for Databricks runtime context first, then falls back to relative lookup for local pytest runs).
- **Zero-Dependency YAML Parsing**: Use the custom indentation-based line reader instead of importing `yaml` or `pyyaml`.
- **API Extraction & Fallback**: Integrate fallback sample data inside the notebook to ensure execution when APIs are unreachable. Strip unnecessary fields and pre-cast numeric fields to target Spark types before creating the DataFrame.
- **Under-the-Hood API Redirection**: To keep the source API generic for clients (e.g. `https://api.sales-hub.com`), the code should read the API URL from `config.yml` (which is `https://api.sales-hub.com`), but replace `"https://api.sales-hub.com"` with `"https://dummyjson.com"` when making the actual HTTP requests so that the live API call works successfully.
- **Metadata Addition**: Automatically append `_ingestion_timestamp` (ISO string), `_source` (identifier string), and `_batch_id` (UUID).
- **Write Target**: Write idempotently to `b_<pipeline>.<table>`.

### Step 5: Generate Silver Pipeline Code (Spark SQL)
Create `src/pipelines/<domain>/<entity>/silver_cleanse.py`:
- **Header Requirement**: Start with `# Databricks notebook source` on line 1.
- **SQL Execution**: Wrap cleansing statements in PySpark `spark.sql("""...""")` commands.
- **SQL Standards**: UPPERCASE all SQL keywords (`SELECT`, `FROM`, `LEFT JOIN`, `WHERE`, `ROW_NUMBER`). Column names should be in `snake_case`.
- **Explosion Safety**: Wrap `EXPLODE` operations in a subquery first, then perform joins to avoid Spark parser errors.
- **Deduplication**: Implement `ROW_NUMBER() OVER (PARTITION BY <key> ORDER BY _ingestion_timestamp DESC)` to filter for the latest record where `row_num = 1`.
- **Write Target**: Write to `s_<pipeline>.<table>`.

### Step 5: Generate Gold Pipeline Code (Spark SQL)
Create `src/pipelines/<domain>/<entity>/gold_aggregate.py`:
- **Header Requirement**: Start with `# Databricks notebook source` on line 1.
- **SQL Aggregations**: Apply metrics calculations, business aggregation logic, and explicit groupings.
- **Grouping Rules**: Avoid positional groupings (e.g., use `GROUP BY category` instead of `GROUP BY 1`).
- **Null Safety**: Wrap aggregate measures in `COALESCE` statements and always include record counts alongside metric outputs.
- **Write Target**: Write to `g_<pipeline>.<table>`.

### Step 6: Generate Master Orchestrator Notebook
Create `notebooks/run_pipeline.py`:
- **Header Requirement**: Start with `# Databricks notebook source` on line 1.
- **Relative Path Resolution**: Set `PIPELINE_PATH` going up one level using `../src/pipelines/<domain>/<entity>` so `dbutils.notebook.run()` resolves paths correctly during Databricks Jobs.
- **Orchestration Flow**: Execute notebooks sequentially: Bronze → Silver → Gold inside `try/except` statements.
- **Reporting**: Display runtime duration for each stage and show final record counts.

### Step 7: Generate Unit Tests
Create `tests/test_bronze.py`, `tests/test_silver.py`, and `tests/test_gold.py`:
- **Format Constraint**: DO NOT include `# Databricks notebook source` in test files.
- **Environment Isolation**: Mock the `requests.get` call using `unittest.mock.patch` to simulate API endpoints.
- **PySpark Stubbing**: Stub PySpark classes in `sys.modules` to allow executing notebooks outside of active Spark sessions.
- **Cell Extraction**: Parse notebook files, split them by `# COMMAND ----------`, and execute them using Python's `exec()` with a mock Spark namespace.
- **Target Assertions**: Validate schema structures (`StructType`), row count outcomes, metadata population, and expected calculations.

---

## Code Generation Checklist

| Artifact | Language/Format | Location | Key Enforcements |
| :--- | :--- | :--- | :--- |
| **HLD Document** | Markdown | `docs/<domain>-<entity>-hld.md` | Clear flow, qualified schema catalogs |
| **Bronze Code** | PySpark Notebook | `src/pipelines/.../bronze_ingest.py` | API Fallbacks, Zero-yaml parser, metadata fields |
| **Silver Code** | PySpark/Spark SQL | `src/pipelines/.../silver_cleanse.py` | Spark SQL UPPERCASE, EXPLODE subqueries, dedup |
| **Gold Code** | PySpark/Spark SQL | `src/pipelines/.../gold_aggregate.py` | UPPERCASE SQL, explicit groupings, null checks |
| **Orchestrator** | PySpark Notebook | `notebooks/run_pipeline.py` | Relative path orchestration, timing stats |
| **Unit Tests** | Python (pytest) | `tests/test_*.py` | Exec cell-by-cell, PySpark mocks, patch requests |

---

## Best Practices & Pitfalls
* **DO** verify that the zero-dependency YAML reader matches the target `config.yml` structure perfectly.
* **DO** use absolute paths `/Workspace/<path>` within Databricks and fall back to local relative paths during pytest.
* **DON'T** let PySpark infer schema structures dynamically; always enforce rigid `StructType` schemas to avoid type coercion errors.
* **DON'T** import external packages that are not pre-installed on basic Databricks serverless compute nodes (e.g. `pyyaml`, `pandas`).
