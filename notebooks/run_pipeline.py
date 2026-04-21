# Databricks notebook source

# COMMAND ----------

# Sales Orders Pipeline — Master Orchestrator
# Runs Bronze → Silver → Gold in sequence

import time

# COMMAND ----------

# ---- Bronze Layer ----

print("Starting Bronze ingestion...")
start = time.time()
try:
    dbutils.notebook.run("../src/pipelines/sales/orders/bronze_ingest", timeout_seconds=600)
    bronze_time = time.time() - start
    print(f"Bronze complete in {bronze_time:.1f}s")
except Exception as e:
    print(f"Bronze FAILED: {e}")
    raise

# COMMAND ----------

# ---- Silver Layer ----

print("Starting Silver cleansing...")
start = time.time()
try:
    dbutils.notebook.run("../src/pipelines/sales/orders/silver_cleanse", timeout_seconds=600)
    silver_time = time.time() - start
    print(f"Silver complete in {silver_time:.1f}s")
except Exception as e:
    print(f"Silver FAILED: {e}")
    raise

# COMMAND ----------

# ---- Gold Layer ----

print("Starting Gold aggregation...")
start = time.time()
try:
    dbutils.notebook.run("../src/pipelines/sales/orders/gold_aggregate", timeout_seconds=600)
    gold_time = time.time() - start
    print(f"Gold complete in {gold_time:.1f}s")
except Exception as e:
    print(f"Gold FAILED: {e}")
    raise

# COMMAND ----------

# ---- Pipeline Summary ----

total_time = bronze_time + silver_time + gold_time

print(f"\n{'='*60}")
print(f" Pipeline Complete — Total Time: {total_time:.1f}s")
print(f"{'='*60}")
print(f"  Bronze : {bronze_time:.1f}s")
print(f"  Silver : {silver_time:.1f}s")
print(f"  Gold   : {gold_time:.1f}s")
print(f"{'='*60}")

tables = [
    "b_salesorders.products", "b_salesorders.carts", "b_salesorders.users",
    "s_salesorders.products", "s_salesorders.orders", "s_salesorders.customers",
    "g_salesorders.revenue_by_category", "g_salesorders.order_summary",
]
print("\nTable Row Counts:")
for t in tables:
    count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {t}").collect()[0]["cnt"]
    print(f"  {t}: {count} rows")
