# Databricks notebook source
# CELL 1: Orchestrator Imports & Configuration
import time
import os

try:
    notebook_path = dbutils.entrypoint.getDbutils().notebook().getContext().notebookPath().get()
    if "/src/" in notebook_path:
        repo_root = notebook_path.split("/src/")[0]
    elif "/notebooks/" in notebook_path:
        repo_root = notebook_path.split("/notebooks/")[0]
    else:
        repo_root = notebook_path
    config_path = f"/Workspace{repo_root}/config.yml"
except Exception:
    config_path = "config.yml"
    for _ in range(5):
        if os.path.exists(config_path):
            break
        config_path = os.path.join("..", config_path)

config = {}
current_path = []
with open(config_path, "r") as f:
    for line in f:
        stripped = line.lstrip()
        if not stripped or stripped.startswith("#"):
            continue
        indent = len(line) - len(stripped)
        level = indent // 2
        current_path = current_path[:level]
        
        if ":" in stripped:
            parts = stripped.split(":", 1)
            key = parts[0].strip()
            val = parts[1].split("#")[0].strip()
            
            if val.startswith('"') and val.endswith('"'):
                val = val[1:-1]
            elif val.startswith("'") and val.endswith("'"):
                val = val[1:-1]
                
            target = config
            for p in current_path:
                target = target[p]
                
            if not val:
                target[key] = {}
                current_path.append(key)
            else:
                target[key] = val

# Define pipeline execution base path
PIPELINE_PATH = "../src/pipelines/sales/orders"

# COMMAND ----------
# CELL 2: Run Bronze Layer Ingestion
print("Starting Bronze Layer Ingestion...")
start_time = time.time()
try:
    dbutils.notebook.run(f"{PIPELINE_PATH}/bronze_ingest", 0)
    bronze_duration = time.time() - start_time
    print(f"Bronze Ingestion completed in {bronze_duration:.2f} seconds.")
except Exception as e:
    print(f"Bronze Ingestion failed: {str(e)}")
    raise e

# COMMAND ----------
# CELL 3: Run Silver Layer Cleansing
print("Starting Silver Layer Cleansing & Conformance...")
start_time = time.time()
try:
    dbutils.notebook.run(f"{PIPELINE_PATH}/silver_cleanse", 0)
    silver_duration = time.time() - start_time
    print(f"Silver Cleansing completed in {silver_duration:.2f} seconds.")
except Exception as e:
    print(f"Silver Cleansing failed: {str(e)}")
    raise e

# COMMAND ----------
# CELL 4: Run Gold Layer Aggregations
print("Starting Gold Layer Business Aggregations...")
start_time = time.time()
try:
    dbutils.notebook.run(f"{PIPELINE_PATH}/gold_aggregate", 0)
    gold_duration = time.time() - start_time
    print(f"Gold Aggregations completed in {gold_duration:.2f} seconds.")
except Exception as e:
    print(f"Gold Aggregations failed: {str(e)}")
    raise e

# COMMAND ----------
# CELL 5: Display Verification & Metrics Summary
print("====================================================")
print("Pipeline Run Completed successfully. Summary of tables:")
for layer in ["bronze", "silver", "gold"]:
    print(f"\n{layer.upper()} Layer Tables:")
    for key, table in config["tables"][layer].items():
        try:
            count = spark.sql(f"SELECT COUNT(*) FROM {table}").collect()[0][0]
            print(f" - {table}: {count} rows")
        except Exception:
            print(f" - {table}: Unable to fetch count (table might not exist)")
print("====================================================")
