# Databricks notebook source

# COMMAND ----------
# Gold Aggregations: Sales Orders
# Creates business-level aggregation tables from silver data
# Writes to g_salesorders schema as Delta tables

# COMMAND ----------
SCHEMA_NAME = "g_salesorders"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")

# COMMAND ----------
# ── Revenue by Category ──

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

print("✓ Created g_salesorders.revenue_by_category")
spark.sql("SELECT * FROM g_salesorders.revenue_by_category").show(truncate=False)

# COMMAND ----------
# ── Order Summary ──

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

print("✓ Created g_salesorders.order_summary")
spark.sql("SELECT * FROM g_salesorders.order_summary").show(truncate=False)

# COMMAND ----------
print("✓ Gold aggregations complete")
