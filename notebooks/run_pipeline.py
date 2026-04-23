# Databricks notebook source

# COMMAND ----------
# Master Orchestrator: Sales Orders Pipeline
# Runs Bronze → Silver → Gold notebooks in sequence
# Tracks timing and row counts for each stage

# COMMAND ----------
import time

# COMMAND ----------
# ── Pipeline Configuration ──

PIPELINE_NAME = "Sales Orders"
NOTEBOOKS = [
    ("Bronze - Raw Ingestion", "../src/pipelines/sales/orders/bronze_ingest"),
    ("Silver - Cleansing", "../src/pipelines/sales/orders/silver_cleanse"),
    ("Gold - Aggregations", "../src/pipelines/sales/orders/gold_aggregate"),
]

# COMMAND ----------
# ── Run Pipeline ──

print(f"{'='*60}")
print(f"  {PIPELINE_NAME} Pipeline — Starting")
print(f"{'='*60}\n")

pipeline_start = time.time()
results = []

for stage_name, notebook_path in NOTEBOOKS:
    print(f"▶ Running: {stage_name}")
    stage_start = time.time()
    try:
        dbutils.notebook.run(notebook_path, timeout_seconds=600)
        elapsed = round(time.time() - stage_start, 1)
        results.append((stage_name, "SUCCESS", f"{elapsed}s"))
        print(f"  ✓ {stage_name} completed in {elapsed}s\n")
    except Exception as e:
        elapsed = round(time.time() - stage_start, 1)
        results.append((stage_name, "FAILED", f"{elapsed}s"))
        print(f"  ✗ {stage_name} failed after {elapsed}s: {e}\n")
        raise

total_time = round(time.time() - pipeline_start, 1)

# COMMAND ----------
# ── Summary ──

print(f"\n{'='*60}")
print(f"  Pipeline Summary — {total_time}s total")
print(f"{'='*60}")
for stage, status, elapsed in results:
    print(f"  {status:8s} | {elapsed:6s} | {stage}")

# Table row counts
print(f"\n{'─'*60}")
print("  Table Row Counts:")
tables = [
    "b_salesorders.products", "b_salesorders.carts", "b_salesorders.users",
    "s_salesorders.products", "s_salesorders.orders", "s_salesorders.customers",
    "s_salesorders.orders_quarantine",
    "g_salesorders.revenue_by_category", "g_salesorders.order_summary",
]
for table in tables:
    try:
        count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {table}").collect()[0]["cnt"]
        print(f"    {table:45s} {count:>6d} rows")
    except Exception:
        print(f"    {table:45s}   N/A")

print(f"{'='*60}")
print("✓ Pipeline complete")
