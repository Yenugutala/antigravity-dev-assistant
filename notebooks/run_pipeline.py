# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Sales Orders Pipeline — Full Run
# MAGIC **Specs**: specs/bronze-spec.md, specs/silver-spec.md, specs/gold-spec.md
# MAGIC **Architecture**: Bronze (PySpark) → Silver (Spark SQL) → Gold (Spark SQL)
# MAGIC **Source**: DummyJSON API (https://dummyjson.com)

# COMMAND ----------

import time

print("=" * 60)
print("  AI PIPELINE ACCELERATOR — Sales Orders Pipeline")
print("  Spec-Driven | Auto-Generated | Medallion Architecture")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 1: Bronze — Raw Data Ingestion (PySpark)

# COMMAND ----------

start = time.time()

# COMMAND ----------

# MAGIC %run ../src/pipelines/sales/orders/bronze_ingest

# COMMAND ----------

bronze_time = round(time.time() - start, 1)
print(f"\n⏱ Bronze completed in {bronze_time}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 2: Silver — Cleansing & Conformance (Spark SQL)

# COMMAND ----------

start = time.time()

# COMMAND ----------

# MAGIC %run ../src/pipelines/sales/orders/silver_cleanse

# COMMAND ----------

silver_time = round(time.time() - start, 1)
print(f"\n⏱ Silver completed in {silver_time}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 3: Gold — Business Aggregations (Spark SQL)

# COMMAND ----------

start = time.time()

# COMMAND ----------

# MAGIC %run ../src/pipelines/sales/orders/gold_aggregate

# COMMAND ----------

gold_time = round(time.time() - start, 1)
print(f"\n⏱ Gold completed in {gold_time}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Complete — Summary

# COMMAND ----------

print("=" * 60)
print("  PIPELINE COMPLETE")
print("=" * 60)
print(f"  Bronze: {bronze_time}s")
print(f"  Silver: {silver_time}s")
print(f"  Gold:   {gold_time}s")
print(f"  Total:  {bronze_time + silver_time + gold_time}s")
print()
print("  Tables Created:")
for schema, tables in [
    ("b_salesorders", ["products", "carts", "users"]),
    ("s_salesorders", ["products", "orders", "customers", "orders_quarantine"]),
    ("g_salesorders", ["revenue_by_category", "order_summary"]),
]:
    for table in tables:
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM {schema}.{table}").collect()[0]["cnt"]
        print(f"    {schema}.{table}: {count} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Results — Revenue by Category

# COMMAND ----------

display(spark.sql("SELECT * FROM g_salesorders.revenue_by_category ORDER BY total_revenue DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Results — Daily Order Summary

# COMMAND ----------

display(spark.sql("SELECT * FROM g_salesorders.order_summary ORDER BY order_date"))
