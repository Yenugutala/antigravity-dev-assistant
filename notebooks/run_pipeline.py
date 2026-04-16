# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Sales Orders Pipeline — Master Orchestrator
# MAGIC Runs Bronze → Silver → Gold in sequence.

# COMMAND ----------

import time

# COMMAND ----------

# --- Bronze Layer ---
print("=" * 60)
print("STARTING BRONZE LAYER")
print("=" * 60)

bronze_start = time.time()

# COMMAND ----------

# MAGIC %run ../src/pipelines/sales/orders/bronze_ingest

# COMMAND ----------

bronze_duration = time.time() - bronze_start
print(f"Bronze completed in {bronze_duration:.1f}s")

# COMMAND ----------

# --- Silver Layer ---
print("=" * 60)
print("STARTING SILVER LAYER")
print("=" * 60)

silver_start = time.time()

# COMMAND ----------

# MAGIC %run ../src/pipelines/sales/orders/silver_cleanse

# COMMAND ----------

silver_duration = time.time() - silver_start
print(f"Silver completed in {silver_duration:.1f}s")

# COMMAND ----------

# --- Gold Layer ---
print("=" * 60)
print("STARTING GOLD LAYER")
print("=" * 60)

gold_start = time.time()

# COMMAND ----------

# MAGIC %run ../src/pipelines/sales/orders/gold_aggregate

# COMMAND ----------

gold_duration = time.time() - gold_start
print(f"Gold completed in {gold_duration:.1f}s")

# COMMAND ----------

# --- Pipeline Summary ---

total_duration = bronze_duration + silver_duration + gold_duration

print("=" * 60)
print("PIPELINE COMPLETE")
print("=" * 60)
print(f"Total duration: {total_duration:.1f}s")
print(f"  Bronze: {bronze_duration:.1f}s")
print(f"  Silver: {silver_duration:.1f}s")
print(f"  Gold:   {gold_duration:.1f}s")
print()
print("Tables and row counts:")

for schema, tables in [
    ("b_salesorders", ["products", "carts", "users"]),
    ("s_salesorders", ["products", "orders", "customers", "orders_quarantine"]),
    ("g_salesorders", ["revenue_by_category", "order_summary"]),
]:
    for table in tables:
        full_name = f"{schema}.{table}"
        count = spark.table(full_name).count()
        print(f"  {full_name}: {count} rows")

print("=" * 60)
