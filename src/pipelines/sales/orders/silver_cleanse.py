# Databricks notebook source
# CELL 1: Schema Creation
spark.sql("CREATE SCHEMA IF NOT EXISTS s_salesorders")

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
# CELL 4: Cleanse Products
spark.sql(f"""
CREATE OR REPLACE TABLE {config["tables"]["silver"]["products"]} AS
WITH raw_products AS (
  SELECT
    id AS product_id,
    TRIM(title) AS title,
    CAST(price AS DOUBLE) AS price,
    LOWER(TRIM(category)) AS category,
    CAST(rating AS DOUBLE) AS rating_score,
    brand,
    description,
    _ingestion_timestamp,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingestion_timestamp DESC) AS row_num
  FROM {config["tables"]["bronze"]["products"]}
  WHERE id IS NOT NULL AND price >= 0
)
SELECT
  product_id,
  title,
  price,
  category,
  rating_score,
  brand,
  description,
  _ingestion_timestamp
FROM raw_products
WHERE row_num = 1
""")

# COMMAND ----------
# CELL 5: Cleanse Customers
spark.sql(f"""
CREATE OR REPLACE TABLE {config["tables"]["silver"]["customers"]} AS
WITH raw_users AS (
  SELECT
    id AS customer_id,
    LOWER(TRIM(email)) AS email,
    LOWER(TRIM(username)) AS username,
    firstName AS first_name,
    lastName AS last_name,
    address.city AS city,
    address.address AS street,
    address.postalCode AS zipcode,
    _ingestion_timestamp,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingestion_timestamp DESC) AS row_num
  FROM {config["tables"]["bronze"]["users"]}
  WHERE id IS NOT NULL
)
SELECT
  customer_id,
  email,
  username,
  first_name,
  last_name,
  city,
  street,
  zipcode,
  _ingestion_timestamp
FROM raw_users
WHERE row_num = 1
""")

# COMMAND ----------
# CELL 6: Cleanse Orders & Handle Exploded Items
spark.sql(f"""
CREATE OR REPLACE TABLE {config["tables"]["silver"]["orders"]} AS
WITH exploded_carts AS (
  SELECT
    id AS cart_id,
    userId AS user_id,
    _ingestion_timestamp,
    exploded_product.id AS product_id,
    exploded_product.title AS product_title,
    exploded_product.price AS cart_price,
    exploded_product.quantity,
    exploded_product.total AS cart_total
  FROM {config["tables"]["bronze"]["carts"]}
  LATERAL VIEW EXPLODE(products) AS exploded_product
  WHERE id IS NOT NULL
),
enriched_orders AS (
  SELECT
    ec.cart_id,
    ec.user_id,
    CURRENT_DATE() AS order_date,
    ec.product_id,
    ec.product_title,
    p.category,
    COALESCE(p.price, ec.cart_price) AS price,
    ec.quantity,
    ROUND(COALESCE(ec.cart_total, COALESCE(p.price, ec.cart_price) * ec.quantity), 2) AS line_total,
    ec._ingestion_timestamp
  FROM exploded_carts ec
  LEFT JOIN {config["tables"]["silver"]["products"]} p ON ec.product_id = p.product_id
),
deduped_orders AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY cart_id, product_id ORDER BY _ingestion_timestamp DESC) AS row_num
  FROM enriched_orders
)
SELECT
  cart_id,
  user_id,
  order_date,
  product_id,
  product_title,
  category,
  price,
  quantity,
  line_total,
  _ingestion_timestamp
FROM deduped_orders
WHERE row_num = 1
""")

# COMMAND ----------
# CELL 7: Quarantine Missing Cart ID Records
spark.sql(f"""
CREATE OR REPLACE TABLE {config["tables"]["silver"]["quarantine"]} AS
SELECT
  id AS cart_id,
  userId AS user_id,
  _ingestion_timestamp,
  'Missing cart ID' AS quarantine_reason
FROM {config["tables"]["bronze"]["carts"]}
WHERE id IS NULL
""")

# COMMAND ----------
# CELL 8: Cleansing Verification
print("Silver layer cleansing completed successfully.")
display(spark.sql(f"SELECT * FROM {config['tables']['silver']['orders']} LIMIT 5"))
