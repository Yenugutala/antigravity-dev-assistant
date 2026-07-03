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
import os

try:
    notebook_path = dbutils.entrypoint.getDbutils().notebook().getContext().notebookPath().get()
    if "/src/" in notebook_path:
        repo_root = notebook_path.split("/src/")[0]
    elif "/notebooks/" in notebook_path:
        repo_root = notebook_path.split("/notebooks/")[0]
    else:
        repo_root = notebook_path
    config_path = f"/Workspace{repo_root}/config.yml"
except Exception:
    config_path = "config.yml"
    for _ in range(5):
        if os.path.exists(config_path):
            break
        config_path = os.path.join("..", config_path)

config = {}
current_path = []
with open(config_path, "r") as f:
    for line in f:
        stripped = line.lstrip()
        if not stripped or stripped.startswith("#"):
            continue
        indent = len(line) - len(stripped)
        level = indent // 2
        current_path = current_path[:level]
        
        if ":" in stripped:
            parts = stripped.split(":", 1)
            key = parts[0].strip()
            val = parts[1].split("#")[0].strip()
            
            if val.startswith('"') and val.endswith('"'):
                val = val[1:-1]
            elif val.startswith("'") and val.endswith("'"):
                val = val[1:-1]
                
            target = config
            for p in current_path:
                target = target[p]
                
            if not val:
                target[key] = {}
                current_path.append(key)
            else:
                target[key] = val

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
