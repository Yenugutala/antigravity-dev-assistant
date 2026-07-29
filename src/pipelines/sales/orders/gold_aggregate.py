# Databricks notebook source
# Cell 1: Schema creation
spark.sql("CREATE SCHEMA IF NOT EXISTS g_salesorders")

# COMMAND ----------
# Cell 2: Imports
import os
import uuid
import datetime

# COMMAND ----------
# Cell 3: Configuration
# Resolve config.yml path
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

# Zero-dependency configuration parsing
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

BATCH_ID = str(uuid.uuid4())
INGESTION_TIMESTAMP = datetime.datetime.utcnow().isoformat()

# Get Silver and Gold table names from configuration
s_orders = config.get("tables", {}).get("silver", {}).get("orders", "s_salesorders.orders")

g_revenue_by_category = config.get("tables", {}).get("gold", {}).get("revenue_by_category", "g_salesorders.revenue_by_category")
g_order_summary = config.get("tables", {}).get("gold", {}).get("order_summary", "g_salesorders.order_summary")

# COMMAND ----------
# Cell 4: Aggregate Revenue by Category
spark.sql(f"""
CREATE OR REPLACE TABLE {g_revenue_by_category} AS
SELECT
    category,
    COALESCE(ROUND(SUM(line_total), 2), 0.0) AS total_revenue,
    COALESCE(SUM(quantity), 0) AS total_items_sold,
    COUNT(DISTINCT cart_id) AS total_orders,
    COALESCE(ROUND(AVG(price), 2), 0.0) AS avg_price,
    COUNT(DISTINCT product_id) AS unique_products,
    COUNT(*) AS record_count
FROM {s_orders}
GROUP BY category
ORDER BY total_revenue DESC
""")

# COMMAND ----------
# Cell 5: Aggregate Daily Order Summary
spark.sql(f"""
CREATE OR REPLACE TABLE {g_order_summary} AS
SELECT
    order_date,
    COUNT(DISTINCT cart_id) AS total_orders,
    COUNT(DISTINCT user_id) AS unique_customers,
    COALESCE(ROUND(SUM(line_total), 2), 0.0) AS total_revenue,
    COALESCE(SUM(quantity), 0) AS total_items,
    COALESCE(ROUND(AVG(line_total), 2), 0.0) AS avg_order_line_value,
    COUNT(*) AS record_count
FROM {s_orders}
GROUP BY order_date
ORDER BY order_date
""")

# COMMAND ----------
# Cell 6: Verification
print(f"Gold Layer aggregated successfully for batch {BATCH_ID}")
display(spark.sql(f"SELECT count(*) FROM {g_revenue_by_category}"))
display(spark.sql(f"SELECT count(*) FROM {g_order_summary}"))
