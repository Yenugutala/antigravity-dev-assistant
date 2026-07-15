# Databricks notebook source
# CELL 1: Schema Creation
spark.sql("CREATE SCHEMA IF NOT EXISTS g_salesorders")

# COMMAND ----------
# CELL 2: Imports
import os
import sys

# COMMAND ----------
# CELL 3: Configuration & Path Resolution
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
# CELL 4: Business Aggregation - Revenue by Category
spark.sql(f"""
CREATE OR REPLACE TABLE {config["tables"]["gold"]["revenue_by_category"]} AS
SELECT
  COALESCE(category, 'unknown') AS category,
  ROUND(COALESCE(SUM(line_total), 0.0), 2) AS total_revenue,
  COALESCE(SUM(quantity), 0) AS total_items_sold,
  COALESCE(COUNT(DISTINCT cart_id), 0) AS total_orders,
  ROUND(COALESCE(AVG(price), 0.0), 2) AS avg_price,
  COALESCE(COUNT(DISTINCT product_id), 0) AS unique_products
FROM {config["tables"]["silver"]["orders"]}
GROUP BY category
ORDER BY total_revenue DESC
""")

# COMMAND ----------
# CELL 5: Business Aggregation - Daily Order Summary
spark.sql(f"""
CREATE OR REPLACE TABLE {config["tables"]["gold"]["order_summary"]} AS
SELECT
  order_date,
  COALESCE(COUNT(DISTINCT cart_id), 0) AS total_orders,
  COALESCE(COUNT(DISTINCT user_id), 0) AS unique_customers,
  ROUND(COALESCE(SUM(line_total), 0.0), 2) AS total_revenue,
  COALESCE(SUM(quantity), 0) AS total_items,
  ROUND(COALESCE(AVG(line_total), 0.0), 2) AS avg_order_line_value
FROM {config["tables"]["silver"]["orders"]}
GROUP BY order_date
ORDER BY order_date
""")

# COMMAND ----------
# CELL 6: Aggregations Verification
print("Gold layer business aggregations completed successfully.")
display(spark.sql(f"SELECT * FROM {config['tables']['gold']['revenue_by_category']} LIMIT 5"))
