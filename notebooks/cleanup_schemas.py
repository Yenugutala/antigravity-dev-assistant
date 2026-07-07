# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Cleanup Antigravity Schemas
# MAGIC Drops all schemas and associated tables/views created for the Antigravity Sales medallion pipeline.

# COMMAND ----------

# Drop Bronze Layer Schema
try:
    print("Dropping Bronze Schema: b_antigravity_sales...")
    spark.sql("DROP SCHEMA IF EXISTS b_antigravity_sales CASCADE")
    print("✅ Bronze Schema Dropped.")
except Exception as e:
    print(f"❌ Failed to drop Bronze schema: {e}")

# COMMAND ----------

# Drop Silver Layer Schema
try:
    print("Dropping Silver Schema: s_antigravity_sales...")
    spark.sql("DROP SCHEMA IF EXISTS s_antigravity_sales CASCADE")
    print("✅ Silver Schema Dropped.")
except Exception as e:
    print(f"❌ Failed to drop Silver schema: {e}")

# COMMAND ----------

# Drop Gold Layer Schema
try:
    print("Dropping Gold Schema: g_antigravity_sales...")
    spark.sql("DROP SCHEMA IF EXISTS g_antigravity_sales CASCADE")
    print("✅ Gold Schema Dropped.")
except Exception as e:
    print(f"❌ Failed to drop Gold schema: {e}")

# COMMAND ----------

print("🎉 All Antigravity schemas have been successfully cleaned up.")
