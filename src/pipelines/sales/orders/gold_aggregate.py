# Databricks notebook source

# COMMAND ----------

# Gold Layer: Sales Orders — Business Aggregations
# Reads from s_salesorders, creates business-ready metrics in g_salesorders

# COMMAND ----------

GOLD_SCHEMA = "g_salesorders"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")

# COMMAND ----------

# ── Revenue by Category ──────────────────────────────────────────
# Total revenue, items sold, order count, avg price by product category

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

print("Created g_salesorders.revenue_by_category")
display(spark.sql("SELECT * FROM g_salesorders.revenue_by_category"))

# COMMAND ----------

# ── Daily Order Summary ──────────────────────────────────────────
# Daily order count, unique customers, total revenue, items

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

print("Created g_salesorders.order_summary")
display(spark.sql("SELECT * FROM g_salesorders.order_summary"))

# COMMAND ----------

print("=" * 60)
print("Gold aggregation complete!")
print(f"  Schema: {GOLD_SCHEMA}")
print("  Tables: revenue_by_category, order_summary")
print("=" * 60)
