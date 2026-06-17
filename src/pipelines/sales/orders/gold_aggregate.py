# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer: Sales Orders — Business Aggregations
# MAGIC Reads from `s_salesorders`, creates aggregation tables in `g_salesorders`.

# COMMAND ----------

# Create gold schema
spark.sql("CREATE SCHEMA IF NOT EXISTS g_salesorders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Revenue by Category

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE TABLE g_salesorders.revenue_by_category AS
    SELECT
        category,
        ROUND(SUM(line_total), 2) AS total_revenue,
        SUM(quantity) AS total_items_sold,
        COUNT(DISTINCT cart_id) AS total_orders,
        ROUND(AVG(price), 2) AS avg_price,
        COUNT(DISTINCT product_id) AS unique_products
    FROM s_salesorders.orders
    GROUP BY category
    ORDER BY total_revenue DESC
""")

print("✅ g_salesorders.revenue_by_category created")

# COMMAND ----------

display(spark.sql("SELECT * FROM g_salesorders.revenue_by_category"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order Summary

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE TABLE g_salesorders.order_summary AS
    SELECT
        order_date,
        COUNT(DISTINCT cart_id) AS total_orders,
        COUNT(DISTINCT user_id) AS unique_customers,
        ROUND(SUM(line_total), 2) AS total_revenue,
        SUM(quantity) AS total_items,
        ROUND(AVG(line_total), 2) AS avg_order_line_value
    FROM s_salesorders.orders
    GROUP BY order_date
    ORDER BY order_date
""")

print("✅ g_salesorders.order_summary created")

# COMMAND ----------

display(spark.sql("SELECT * FROM g_salesorders.order_summary"))

# COMMAND ----------

print("🏁 Gold aggregation complete — all tables written to g_salesorders schema")
