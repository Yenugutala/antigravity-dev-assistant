# Databricks notebook source

# COMMAND ----------

# Cell 1: Create Gold Schema
spark.sql("CREATE SCHEMA IF NOT EXISTS g_salesorders")

# COMMAND ----------

# Cell 2: Revenue by Category
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

print("g_salesorders.revenue_by_category created")
spark.sql("SELECT * FROM g_salesorders.revenue_by_category").show(truncate=False)

# COMMAND ----------

# Cell 3: Daily Order Summary
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

print("g_salesorders.order_summary created")
spark.sql("SELECT * FROM g_salesorders.order_summary").show(truncate=False)

# COMMAND ----------

# Cell 4: Gold Summary
print("=" * 60)
print("GOLD AGGREGATION COMPLETE")
print("=" * 60)
rev_count = spark.sql("SELECT COUNT(*) AS cnt FROM g_salesorders.revenue_by_category").collect()[0]["cnt"]
summary_count = spark.sql("SELECT COUNT(*) AS cnt FROM g_salesorders.order_summary").collect()[0]["cnt"]
print(f"  g_salesorders.revenue_by_category — {rev_count} categories")
print(f"  g_salesorders.order_summary       — {summary_count} days")
