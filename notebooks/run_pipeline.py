# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Pipeline Orchestrator (Run Pipeline)
# MAGIC Sequentially triggers Bronze (Ingestion) -> Silver (Cleansing) -> Gold (Aggregations) notebooks.

# COMMAND ----------

# Configuration
PIPELINE_PATH = "src/pipelines/sales/orders"

# Run Bronze Layer Ingestion
try:
    print("🚀 Starting Bronze Layer Ingestion...")
    dbutils.notebook.run(f"{PIPELINE_PATH}/bronze_ingest", 0)
    print("✅ Bronze Ingestion Completed Successfully.")
except Exception as e:
    print(f"❌ Bronze Ingestion Failed: {e}")
    raise e

# COMMAND ----------

# Run Silver Layer Cleansing
try:
    print("🚀 Starting Silver Layer Cleansing...")
    dbutils.notebook.run(f"{PIPELINE_PATH}/silver_cleanse", 0)
    print("✅ Silver Cleansing Completed Successfully.")
except Exception as e:
    print(f"❌ Silver Cleansing Failed: {e}")
    raise e

# COMMAND ----------

# Run Gold Layer Aggregations
try:
    print("🚀 Starting Gold Layer Aggregations...")
    dbutils.notebook.run(f"{PIPELINE_PATH}/gold_aggregate", 0)
    print("✅ Gold Aggregations Completed Successfully.")
except Exception as e:
    print(f"❌ Gold Aggregations Failed: {e}")
    raise e

# COMMAND ----------

# Summary verification
print("🎉 Medallion Pipeline Run Orchestrated Successfully.")
