# Databricks notebook source

# COMMAND ----------

# Cell 1: Pipeline Orchestrator — Sales Orders
import time

PIPELINE_NAME = "Sales Orders"
NOTEBOOKS = [
    ("Bronze Ingestion", "/Repos/ai-pipeline-accelerator/src/pipelines/sales/orders/bronze_ingest"),
    ("Silver Cleansing", "/Repos/ai-pipeline-accelerator/src/pipelines/sales/orders/silver_cleanse"),
    ("Gold Aggregation", "/Repos/ai-pipeline-accelerator/src/pipelines/sales/orders/gold_aggregate"),
]

print(f"Starting {PIPELINE_NAME} Pipeline")
print("=" * 60)

results = []
pipeline_start = time.time()

for stage_name, notebook_path in NOTEBOOKS:
    print(f"\nRunning: {stage_name}...")
    stage_start = time.time()
    try:
        dbutils.notebook.run(notebook_path, timeout_seconds=600)
        elapsed = round(time.time() - stage_start, 1)
        results.append((stage_name, "SUCCESS", elapsed))
        print(f"  {stage_name}: SUCCESS ({elapsed}s)")
    except Exception as e:
        elapsed = round(time.time() - stage_start, 1)
        results.append((stage_name, f"FAILED: {e}", elapsed))
        print(f"  {stage_name}: FAILED ({elapsed}s) — {e}")

# COMMAND ----------

# Cell 2: Pipeline Summary
pipeline_elapsed = round(time.time() - pipeline_start, 1)

print()
print("=" * 60)
print(f"PIPELINE COMPLETE — {PIPELINE_NAME}")
print("=" * 60)
print(f"Total time: {pipeline_elapsed}s")
print()
print(f"{'Stage':<25} {'Status':<15} {'Time':>8}")
print("-" * 50)
for stage_name, status, elapsed in results:
    print(f"{stage_name:<25} {status:<15} {elapsed:>7.1f}s")
print()

# Show final table row counts
print("Final Table Row Counts:")
for table in [
    "b_salesorders.products", "b_salesorders.carts", "b_salesorders.users",
    "s_salesorders.products", "s_salesorders.orders", "s_salesorders.customers",
    "g_salesorders.revenue_by_category", "g_salesorders.order_summary",
]:
    try:
        count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {table}").collect()[0]["cnt"]
        print(f"  {table:<45} {count:>6} rows")
    except Exception:
        print(f"  {table:<45}  ERROR")
