# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Sales Orders Pipeline — Master Orchestrator
# MAGIC Runs Bronze → Silver → Gold notebooks in sequence.

# COMMAND ----------

import time

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 1: Bronze — Raw Data Ingestion

# COMMAND ----------

start = time.time()
try:
    dbutils.notebook.run("../src/pipelines/sales/orders/bronze_ingest", timeout_seconds=600)
    bronze_time = round(time.time() - start, 2)
    print(f"✅ Bronze completed in {bronze_time}s")
except Exception as e:
    print(f"❌ Bronze failed: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 2: Silver — Cleansing & Transformation

# COMMAND ----------

start = time.time()
try:
    dbutils.notebook.run("../src/pipelines/sales/orders/silver_cleanse", timeout_seconds=600)
    silver_time = round(time.time() - start, 2)
    print(f"✅ Silver completed in {silver_time}s")
except Exception as e:
    print(f"❌ Silver failed: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 3: Gold — Business Aggregations

# COMMAND ----------

start = time.time()
try:
    dbutils.notebook.run("../src/pipelines/sales/orders/gold_aggregate", timeout_seconds=600)
    gold_time = round(time.time() - start, 2)
    print(f"✅ Gold completed in {gold_time}s")
except Exception as e:
    print(f"❌ Gold failed: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Summary

# COMMAND ----------

print("=" * 60)
print("PIPELINE EXECUTION SUMMARY")
print("=" * 60)

tables = {
    "Bronze": ["b_salesorders.products", "b_salesorders.carts", "b_salesorders.users"],
    "Silver": ["s_salesorders.products", "s_salesorders.orders", "s_salesorders.customers", "s_salesorders.orders_quarantine"],
    "Gold": ["g_salesorders.revenue_by_category", "g_salesorders.order_summary"],
}

for layer, table_list in tables.items():
    print(f"\n--- {layer} ---")
    for table in table_list:
        try:
            count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {table}").collect()[0]["cnt"]
            print(f"  {table}: {count} rows")
        except Exception:
            print(f"  {table}: (not available)")

print("\n--- Timing ---")
print(f"  Bronze: {bronze_time}s")
print(f"  Silver: {silver_time}s")
print(f"  Gold:   {gold_time}s")
print(f"  Total:  {round(bronze_time + silver_time + gold_time, 2)}s")
print("=" * 60)
print("🏁 Pipeline complete!")
