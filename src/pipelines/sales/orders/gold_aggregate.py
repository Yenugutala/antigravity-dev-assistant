# Databricks notebook source

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS g_salesorders

# COMMAND ----------

# ---- Gold: Revenue by Product Category ----

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

display(spark.sql("SELECT * FROM g_salesorders.revenue_by_category"))

# COMMAND ----------

# ---- Gold: Daily Order Summary ----

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

display(spark.sql("SELECT * FROM g_salesorders.order_summary"))

# COMMAND ----------

print("Gold aggregation complete.")
print("Tables created: g_salesorders.revenue_by_category, g_salesorders.order_summary")
