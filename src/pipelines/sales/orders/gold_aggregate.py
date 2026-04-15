# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer — Sales Orders Business Aggregations
# MAGIC **Input**: orders_silver, customers_silver
# MAGIC **Output**: revenue_by_category, order_summary
# MAGIC **Pattern**: Spark SQL (PySQL) — business-level aggregations

# COMMAND ----------

from datetime import datetime
print(f"Gold Aggregation Started: {datetime.now()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Revenue by Product Category

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE TABLE default.revenue_by_category AS
    SELECT
        category,
        ROUND(SUM(line_total), 2) AS total_revenue,
        SUM(quantity) AS total_items_sold,
        COUNT(DISTINCT cart_id) AS total_orders,
        ROUND(AVG(price), 2) AS avg_price,
        COUNT(DISTINCT product_id) AS unique_products
    FROM default.orders_silver
    GROUP BY category
    ORDER BY total_revenue DESC
""")

print("✓ default.revenue_by_category created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Revenue by Category Results

# COMMAND ----------

display(spark.sql("SELECT * FROM default.revenue_by_category"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Daily Order Summary

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE TABLE default.order_summary AS
    SELECT
        order_date,
        COUNT(DISTINCT cart_id) AS total_orders,
        COUNT(DISTINCT user_id) AS unique_customers,
        ROUND(SUM(line_total), 2) AS total_revenue,
        SUM(quantity) AS total_items,
        ROUND(AVG(line_total), 2) AS avg_order_line_value
    FROM default.orders_silver
    GROUP BY order_date
    ORDER BY order_date
""")

print("✓ default.order_summary created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Daily Order Summary Results

# COMMAND ----------

display(spark.sql("SELECT * FROM default.order_summary"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Summary

# COMMAND ----------

print("=" * 50)
print("GOLD AGGREGATION COMPLETE")
print("=" * 50)
for table in ["revenue_by_category", "order_summary"]:
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM default.{table}").collect()[0]["cnt"]
    print(f"  default.{table}: {count} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Insights

# COMMAND ----------

# Top category by revenue
display(spark.sql("""
    SELECT
        category,
        total_revenue,
        total_items_sold,
        total_orders,
        avg_price
    FROM default.revenue_by_category
    ORDER BY total_revenue DESC
"""))

# COMMAND ----------

# Total business metrics
display(spark.sql("""
    SELECT
        SUM(total_revenue) AS grand_total_revenue,
        SUM(total_orders) AS grand_total_orders,
        SUM(unique_customers) AS total_unique_customers,
        SUM(total_items) AS grand_total_items
    FROM default.order_summary
"""))
