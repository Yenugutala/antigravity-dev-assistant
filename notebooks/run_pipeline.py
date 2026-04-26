# Databricks notebook source

# COMMAND ----------

# Master Orchestrator: Sales Orders Pipeline
# Runs Bronze -> Silver -> Gold notebooks in sequence
# Displays timing and row count summary

# COMMAND ----------

import time

# COMMAND ----------

# ── Pipeline Configuration ───────────────────────────────────────

PIPELINE_NAME = "Sales Orders"
NOTEBOOKS = [
    ("Bronze - Raw Ingestion", "../src/pipelines/sales/orders/bronze_ingest"),
    ("Silver - Cleansing", "../src/pipelines/sales/orders/silver_cleanse"),
    ("Gold - Aggregation", "../src/pipelines/sales/orders/gold_aggregate"),
]

# COMMAND ----------

# ── Run Pipeline ─────────────────────────────────────────────────

print("=" * 60)
print(f"Starting {PIPELINE_NAME} Pipeline")
print("=" * 60)

total_start = time.time()
results = []

for stage_name, notebook_path in NOTEBOOKS:
    print(f"\nRunning: {stage_name}")
    stage_start = time.time()
    try:
        dbutils.notebook.run(notebook_path, timeout_seconds=600)
        elapsed = round(time.time() - stage_start, 1)
        results.append((stage_name, "SUCCESS", f"{elapsed}s"))
        print(f"  {stage_name} completed in {elapsed}s")
    except Exception as e:
        elapsed = round(time.time() - stage_start, 1)
        results.append((stage_name, "FAILED", f"{elapsed}s"))
        print(f"  {stage_name} failed after {elapsed}s: {e}")
        raise

total_elapsed = round(time.time() - total_start, 1)

# COMMAND ----------

# ── Summary ──────────────────────────────────────────────────────

print("\n" + "=" * 60)
print(f"{PIPELINE_NAME} Pipeline - Summary")
print("=" * 60)

for stage_name, status, elapsed in results:
    icon = "OK" if status == "SUCCESS" else "FAIL"
    print(f"  [{icon}] {stage_name}: {status} ({elapsed})")

print(f"\nTotal elapsed: {total_elapsed}s")
print("=" * 60)

# COMMAND ----------

# ── Table Row Counts ─────────────────────────────────────────────

print("\nTable Row Counts:")
print("-" * 40)

tables = [
    "b_salesorders.products",
    "b_salesorders.carts",
    "b_salesorders.users",
    "s_salesorders.products",
    "s_salesorders.orders",
    "s_salesorders.customers",
    "s_salesorders.orders_quarantine",
    "g_salesorders.revenue_by_category",
    "g_salesorders.order_summary",
]

for table in tables:
    try:
        count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {table}").collect()[0]["cnt"]
        print(f"  {table}: {count} rows")
    except Exception:
        print(f"  {table}: (not found)")

print("\nPipeline complete! Ready for dashboards.")
