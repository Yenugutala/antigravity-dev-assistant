# Databricks notebook source
# Cell 1: Schema creation
# (No schema needed for orchestrator, but we follow the notebook organization convention)
print("Initializing Sales Orders Orchestrator...")

# COMMAND ----------
# Cell 2: Imports
import time
import os

# COMMAND ----------
# Cell 3: Configuration
PIPELINE_PATH = "../src/pipelines/sales/orders"

# COMMAND ----------
# Cell 4: Execute Bronze Notebook
print("Starting Bronze Ingestion stage...")
start_time_bronze = time.time()
try:
    dbutils.notebook.run(f"{PIPELINE_PATH}/bronze_ingest", 0)
    duration_bronze = time.time() - start_time_bronze
    print(f"Bronze Ingestion completed successfully in {duration_bronze:.2f} seconds.")
    status_bronze = "SUCCESS"
except Exception as e:
    duration_bronze = time.time() - start_time_bronze
    print(f"Bronze Ingestion failed after {duration_bronze:.2f} seconds: {str(e)}")
    status_bronze = "FAILED"
    raise e

# COMMAND ----------
# Cell 5: Execute Silver Notebook
print("Starting Silver Cleansing stage...")
start_time_silver = time.time()
try:
    dbutils.notebook.run(f"{PIPELINE_PATH}/silver_cleanse", 0)
    duration_silver = time.time() - start_time_silver
    print(f"Silver Cleansing completed successfully in {duration_silver:.2f} seconds.")
    status_silver = "SUCCESS"
except Exception as e:
    duration_silver = time.time() - start_time_silver
    print(f"Silver Cleansing failed after {duration_silver:.2f} seconds: {str(e)}")
    status_silver = "FAILED"
    raise e

# COMMAND ----------
# Cell 6: Execute Gold Notebook
print("Starting Gold Aggregation stage...")
start_time_gold = time.time()
try:
    dbutils.notebook.run(f"{PIPELINE_PATH}/gold_aggregate", 0)
    duration_gold = time.time() - start_time_gold
    print(f"Gold Aggregation completed successfully in {duration_gold:.2f} seconds.")
    status_gold = "SUCCESS"
except Exception as e:
    duration_gold = time.time() - start_time_gold
    print(f"Gold Aggregation failed after {duration_gold:.2f} seconds: {str(e)}")
    status_gold = "FAILED"
    raise e

# COMMAND ----------
# Cell 7: Execution Summary
print("==================================================")
print("Pipeline Run Summary:")
print(f"Bronze Layer: {status_bronze} ({duration_bronze:.2f}s)")
print(f"Silver Layer: {status_silver} ({duration_silver:.2f}s)")
print(f"Gold Layer:   {status_gold} ({duration_gold:.2f}s)")
print("==================================================")
