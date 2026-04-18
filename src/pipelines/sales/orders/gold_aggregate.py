# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer: Sales Orders — Business Aggregations
# MAGIC Reads from `s_salesorders`, creates business analytics tables in `g_salesorders` schema.

# COMMAND ----------

GOLD_SCHEMA = "g_salesorders"
SILVER_SCHEMA = "s_salesorders"

# COMMAND ----------

# Create Gold schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")

# COMMAND ----------

# --- Revenue by Category ---
# Source: s_salesorders.orders
# Group by: category
# Metrics: total_revenue, total_items_sold, total_orders, avg_price, unique_products

print("Generating revenue by category...")

spark.sql(f"""
    CREATE OR REPLACE TABLE {GOLD_SCHEMA}.revenue_by_category AS
    SELECT
        category,
        ROUND(SUM(line_total), 2) AS total_revenue,
        SUM(quantity) AS total_items_sold,
        COUNT(DISTINCT cart_id) AS total_orders,
        ROUND(AVG(price), 2) AS avg_price,
        COUNT(DISTINCT product_id) AS unique_products
    FROM {SILVER_SCHEMA}.orders
    GROUP BY category
    ORDER BY total_revenue DESC
""")

print(f"Wrote revenue_by_category to {GOLD_SCHEMA}.revenue_by_category")
display(spark.table(f"{GOLD_SCHEMA}.revenue_by_category"))

# COMMAND ----------

# --- Order Summary ---
# Source: s_salesorders.orders
# Group by: order_date
# Metrics: total_orders, unique_customers, total_revenue, total_items, avg_order_line_value

print("Generating order summary...")

spark.sql(f"""
    CREATE OR REPLACE TABLE {GOLD_SCHEMA}.order_summary AS
    SELECT
        order_date,
        COUNT(DISTINCT cart_id) AS total_orders,
        COUNT(DISTINCT user_id) AS unique_customers,
        ROUND(SUM(line_total), 2) AS total_revenue,
        SUM(quantity) AS total_items,
        ROUND(AVG(line_total), 2) AS avg_order_line_value
    FROM {SILVER_SCHEMA}.orders
    GROUP BY order_date
    ORDER BY order_date
""")

print(f"Wrote order_summary to {GOLD_SCHEMA}.order_summary")
display(spark.table(f"{GOLD_SCHEMA}.order_summary"))

# COMMAND ----------

# --- Summary ---

print("=" * 60)
print("GOLD AGGREGATION COMPLETE")
print("=" * 60)
print(f"Schema: {GOLD_SCHEMA}")
print("Tables created:")
for table in ["revenue_by_category", "order_summary"]:
    count = spark.table(f"{GOLD_SCHEMA}.{table}").count()
    print(f"  {GOLD_SCHEMA}.{table}: {count} rows")
print("=" * 60)
