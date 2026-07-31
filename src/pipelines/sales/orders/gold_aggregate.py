# Databricks notebook source
# COMMAND ----------
# CREATE SCHEMA IF NOT EXISTS
spark.sql("CREATE SCHEMA IF NOT EXISTS g_salesorders")

# COMMAND ----------
# LINT & STANDARDS: UPPERCASE SQL keywords, snake_case columns.

# COMMAND ----------
# PROCESS REVENUE BY CATEGORY
spark.sql("""
CREATE OR REPLACE TABLE g_salesorders.revenue_by_category AS
SELECT 
  category,
  COALESCE(ROUND(SUM(line_total), 2), 0.0) AS total_revenue,
  COALESCE(SUM(quantity), 0) AS total_items_sold,
  COUNT(DISTINCT cart_id) AS total_orders,
  COALESCE(ROUND(AVG(price), 2), 0.0) AS avg_price,
  COUNT(DISTINCT product_id) AS unique_products
FROM s_salesorders.orders
GROUP BY category
ORDER BY total_revenue DESC
""")

# COMMAND ----------
# PROCESS DAILY ORDER SUMMARY
spark.sql("""
CREATE OR REPLACE TABLE g_salesorders.order_summary AS
SELECT 
  order_date,
  COUNT(DISTINCT cart_id) AS total_orders,
  COUNT(DISTINCT user_id) AS unique_customers,
  COALESCE(ROUND(SUM(line_total), 2), 0.0) AS total_revenue,
  COALESCE(SUM(quantity), 0) AS total_items,
  COALESCE(ROUND(AVG(line_total), 2), 0.0) AS avg_order_line_value
FROM s_salesorders.orders
GROUP BY order_date
ORDER BY order_date
""")

# COMMAND ----------
# VERIFICATION QUERY
print("Gold aggregations completed successfully.")
spark.sql("SELECT * FROM g_salesorders.revenue_by_category").show()
