# Databricks notebook source
# Cell 1: Schema creation
spark.sql("CREATE SCHEMA IF NOT EXISTS s_salesorders")

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

# Get Bronze and Silver table names from configuration
b_products = config.get("tables", {}).get("bronze", {}).get("products", "b_salesorders.products")
b_carts = config.get("tables", {}).get("bronze", {}).get("carts", "b_salesorders.carts")
b_users = config.get("tables", {}).get("bronze", {}).get("users", "b_salesorders.users")

s_products = config.get("tables", {}).get("silver", {}).get("products", "s_salesorders.products")
s_orders = config.get("tables", {}).get("silver", {}).get("orders", "s_salesorders.orders")
s_customers = config.get("tables", {}).get("silver", {}).get("customers", "s_salesorders.customers")
s_quarantine = config.get("tables", {}).get("silver", {}).get("quarantine", "s_salesorders.orders_quarantine")

# COMMAND ----------
# Cell 4: Cleanse Products Table
spark.sql(f"""
CREATE OR REPLACE TABLE {s_products} AS
WITH raw_products AS (
    SELECT
        id AS product_id,
        TRIM(title) AS title,
        CAST(price AS DOUBLE) AS price,
        TRIM(LOWER(category)) AS category,
        CAST(rating AS DOUBLE) AS rating_score,
        brand,
        description,
        _ingestion_timestamp,
        _batch_id,
        _source
    FROM {b_products}
    WHERE id IS NOT NULL AND price >= 0
),
deduped_products AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _ingestion_timestamp DESC) AS row_num
    FROM raw_products
)
SELECT
    product_id,
    title,
    price,
    category,
    rating_score,
    brand,
    description,
    _ingestion_timestamp,
    _batch_id,
    _source
FROM deduped_products
WHERE row_num = 1
""")

# COMMAND ----------
# Cell 5: Cleanse Orders Table (Explode array and enrich with conformed products)
spark.sql(f"""
CREATE OR REPLACE TABLE {s_orders} AS
WITH exploded_carts AS (
    SELECT
        id AS cart_id,
        userId AS user_id,
        _ingestion_timestamp,
        _batch_id,
        _source,
        exploded_p.id AS product_id,
        exploded_p.title AS product_title,
        exploded_p.price AS cart_price,
        exploded_p.quantity,
        exploded_p.total AS cart_total
    FROM {b_carts}
    LATERAL VIEW EXPLODE(products) AS exploded_p
    WHERE id IS NOT NULL
),
enriched_orders AS (
    SELECT
        ec.cart_id,
        ec.user_id,
        CURRENT_DATE() AS order_date,
        ec.product_id,
        ec.product_title,
        p.category AS category,
        COALESCE(ec.cart_price, p.price) AS price,
        ec.quantity,
        ROUND(COALESCE(ec.cart_total, COALESCE(p.price, ec.cart_price) * ec.quantity), 2) AS line_total,
        ec._ingestion_timestamp,
        ec._batch_id,
        ec._source
    FROM exploded_carts ec
    LEFT JOIN {s_products} p ON ec.product_id = p.product_id
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
    _ingestion_timestamp,
    _batch_id,
    _source
FROM deduped_orders
WHERE row_num = 1
""")

# COMMAND ----------
# Cell 6: Cleanse Customers Table
spark.sql(f"""
CREATE OR REPLACE TABLE {s_customers} AS
WITH raw_customers AS (
    SELECT
        id AS customer_id,
        TRIM(LOWER(email)) AS email,
        TRIM(LOWER(username)) AS username,
        firstName AS first_name,
        lastName AS last_name,
        address.city AS city,
        address.address AS street,
        address.postalCode AS zipcode,
        _ingestion_timestamp,
        _batch_id,
        _source
    FROM {b_users}
    WHERE id IS NOT NULL
),
deduped_customers AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _ingestion_timestamp DESC) AS row_num
    FROM raw_customers
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
    _ingestion_timestamp,
    _batch_id,
    _source
FROM deduped_customers
WHERE row_num = 1
""")

# COMMAND ----------
# Cell 7: Quarantine Invalid Carts (id is NULL)
spark.sql(f"""
CREATE OR REPLACE TABLE {s_quarantine} AS
SELECT
    id AS cart_id,
    userId AS user_id,
    _ingestion_timestamp,
    _batch_id,
    _source,
    'Missing cart ID' AS quarantine_reason
FROM {b_carts}
WHERE id IS NULL
""")

# COMMAND ----------
# Cell 8: Verification
print(f"Silver Layer cleansed successfully for batch {BATCH_ID}")
display(spark.sql(f"SELECT count(*) FROM {s_products}"))
display(spark.sql(f"SELECT count(*) FROM {s_orders}"))
display(spark.sql(f"SELECT count(*) FROM {s_customers}"))
display(spark.sql(f"SELECT count(*) FROM {s_quarantine}"))
