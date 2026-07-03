# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer: Business Aggregations (PySQL)
# MAGIC Aggregates metrics from `s_antigravity_sales` and saves to `g_antigravity_sales`.

# COMMAND ----------

# Cell 1: Schema Creation
spark.sql("CREATE SCHEMA IF NOT EXISTS g_antigravity_sales")

# COMMAND ----------

# Cell 2: Imports & Configuration
import yaml

with open("config.yml", "r") as f:
    config = yaml.safe_load(f)

# COMMAND ----------

# Cell 3: Revenue by Category
# Source: s_antigravity_sales.orders
# Target: g_antigravity_sales.revenue_by_category
spark.sql("""
CREATE OR REPLACE TABLE g_antigravity_sales.revenue_by_category AS
SELECT 
    category,
    ROUND(SUM(line_total), 2) AS total_revenue,
    SUM(quantity) AS total_items_sold,
    COUNT(DISTINCT cart_id) AS total_orders,
    ROUND(AVG(price), 2) AS avg_price,
    COUNT(DISTINCT product_id) AS unique_products
FROM s_antigravity_sales.orders
GROUP BY category
ORDER BY total_revenue DESC
""")

print("Revenue by category aggregated to g_antigravity_sales.revenue_by_category")

# COMMAND ----------

# Cell 4: Daily Order Summary
# Source: s_antigravity_sales.orders
# Target: g_antigravity_sales.order_summary
spark.sql("""
CREATE OR REPLACE TABLE g_antigravity_sales.order_summary AS
SELECT 
    order_date,
    COUNT(DISTINCT cart_id) AS total_orders,
    COUNT(DISTINCT user_id) AS unique_customers,
    ROUND(SUM(line_total), 2) AS total_revenue,
    SUM(quantity) AS total_items,
    ROUND(AVG(line_total), 2) AS avg_order_line_value
FROM s_antigravity_sales.orders
GROUP BY order_date
ORDER BY order_date
""")

print("Order summary aggregated to g_antigravity_sales.order_summary")
