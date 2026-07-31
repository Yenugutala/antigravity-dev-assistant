# Databricks notebook source
# COMMAND ----------
# IMPORTS
import time

# COMMAND ----------
# CONFIGURATION
PIPELINE_PATH = "../src/pipelines/sales/orders"

# COMMAND ----------
# ORCHESTRATION FLOW
stages = [
    {"name": "Bronze Ingestion", "notebook": f"{PIPELINE_PATH}/bronze_ingest"},
    {"name": "Silver Cleansing", "notebook": f"{PIPELINE_PATH}/silver_cleanse"},
    {"name": "Gold Aggregations", "notebook": f"{PIPELINE_PATH}/gold_aggregate"}
]

print("Starting Medallion Pipeline Orchestrator Run...")

for stage in stages:
    start_time = time.time()
    print(f"Executing stage: {stage['name']}...")
    try:
        # Run notebook using Databricks utilities
        result = dbutils.notebook.run(stage["notebook"], 600)
        end_time = time.time()
        elapsed = end_time - start_time
        print(f"Stage '{stage['name']}' completed successfully in {elapsed:.2f} seconds. Result: {result}")
    except Exception as e:
        end_time = time.time()
        elapsed = end_time - start_time
        print(f"ERROR: Stage '{stage['name']}' failed after {elapsed:.2f} seconds. Details: {e}")
        raise e

# COMMAND ----------
# SUCCESS SUMMARY
print("Medallion Pipeline Orchestrator completed successfully.")
