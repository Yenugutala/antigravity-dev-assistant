# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Layer — Sales Orders Ingestion
# MAGIC **Source**: FakeStore API (https://fakestoreapi.com)
# MAGIC **Output**: products_bronze, carts_bronze, users_bronze (Delta tables)
# MAGIC **Pattern**: PySpark — API call → DataFrame → Delta table

# COMMAND ----------

import requests
import uuid
from datetime import datetime
from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, ArrayType
)

# COMMAND ----------

# Configuration
BATCH_ID = str(uuid.uuid4())
SOURCE = "fakestoreapi"
API_BASE = "https://fakestoreapi.com"

print(f"Bronze Ingestion Started")
print(f"Batch ID: {BATCH_ID}")
print(f"Timestamp: {datetime.now()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ingest Products

# COMMAND ----------

def fetch_api_data(endpoint: str) -> list:
    """Fetch data from FakeStore API endpoint."""
    url = f"{API_BASE}/{endpoint}"
    print(f"Fetching: {url}")
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()
    print(f"  → {len(data)} records fetched")
    return data

# COMMAND ----------

# Fetch and write Products
products_data = fetch_api_data("products")
df_products = spark.createDataFrame(products_data)
df_products = (df_products
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source", lit(SOURCE))
    .withColumn("_batch_id", lit(BATCH_ID))
)

df_products.write.format("delta").mode("overwrite").saveAsTable("default.products_bronze")
print("✓ default.products_bronze written")

# COMMAND ----------

display(spark.sql("SELECT id, title, price, category FROM default.products_bronze LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingest Carts (Orders)

# COMMAND ----------

# Fetch and write Carts
carts_data = fetch_api_data("carts")
df_carts = spark.createDataFrame(carts_data)
df_carts = (df_carts
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source", lit(SOURCE))
    .withColumn("_batch_id", lit(BATCH_ID))
)

df_carts.write.format("delta").mode("overwrite").saveAsTable("default.carts_bronze")
print("✓ default.carts_bronze written")

# COMMAND ----------

display(spark.sql("SELECT id, userId, date, size(products) as num_items FROM default.carts_bronze LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ingest Users (Customers)

# COMMAND ----------

# Fetch and write Users
users_data = fetch_api_data("users")
df_users = spark.createDataFrame(users_data)
df_users = (df_users
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source", lit(SOURCE))
    .withColumn("_batch_id", lit(BATCH_ID))
)

df_users.write.format("delta").mode("overwrite").saveAsTable("default.users_bronze")
print("✓ default.users_bronze written")

# COMMAND ----------

display(spark.sql("SELECT id, email, username FROM default.users_bronze LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Summary

# COMMAND ----------

print("=" * 50)
print("BRONZE INGESTION COMPLETE")
print("=" * 50)
for table in ["products_bronze", "carts_bronze", "users_bronze"]:
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM default.{table}").collect()[0]["cnt"]
    print(f"  default.{table}: {count} rows")
print(f"  Batch ID: {BATCH_ID}")
