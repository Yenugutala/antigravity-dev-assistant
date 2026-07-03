# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer: Cleansing & Conformance (PySQL)
# MAGIC Reads raw data from `b_antigravity_sales` and cleanses it into `s_antigravity_sales`.

# COMMAND ----------

# Cell 1: Schema Creation
spark.sql("CREATE SCHEMA IF NOT EXISTS s_antigravity_sales")

# COMMAND ----------

# Cell 2: Imports & Configuration
import yaml

with open("config.yml", "r") as f:
    config = yaml.safe_load(f)

# COMMAND ----------

# Cell 3: Cleanse Products
# Source: b_antigravity_sales.products
# Target: s_antigravity_sales.products
spark.sql("""
CREATE OR REPLACE TABLE s_antigravity_sales.products AS
WITH ranked_products AS (
    SELECT 
        CAST(id AS INT) AS product_id,
        TRIM(title) AS title,
        CAST(price AS DOUBLE) AS price,
        LOWER(TRIM(category)) AS category,
        CAST(rating AS DOUBLE) AS rating_score,
        brand,
        description,
        _ingestion_timestamp,
        _source,
        _batch_id,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingestion_timestamp DESC) AS row_num
    FROM b_antigravity_sales.products
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
    _ingestion_timestamp,
    _source,
    _batch_id
FROM ranked_products
WHERE row_num = 1
""")

print("Cleaned products loaded to s_antigravity_sales.products")

# COMMAND ----------

# Cell 4: Cleanse Customers
# Source: b_antigravity_sales.users
# Target: s_antigravity_sales.customers
spark.sql("""
CREATE OR REPLACE TABLE s_antigravity_sales.customers AS
WITH ranked_customers AS (
    SELECT 
        CAST(id AS INT) AS customer_id,
        LOWER(TRIM(email)) AS email,
        LOWER(TRIM(username)) AS username,
        firstName AS first_name,
        lastName AS last_name,
        address.city AS city,
        address.address AS street,
        address.postalCode AS zipcode,
        _ingestion_timestamp,
        _source,
        _batch_id,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingestion_timestamp DESC) AS row_num
    FROM b_antigravity_sales.users
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
    _ingestion_timestamp,
    _source,
    _batch_id
FROM ranked_customers
WHERE row_num = 1
""")

print("Cleaned customers loaded to s_antigravity_sales.customers")

# COMMAND ----------

# Cell 5: Cleanse Orders & Quarantine
# Source: b_antigravity_sales.carts
# Target: s_antigravity_sales.orders and s_antigravity_sales.orders_quarantine

# 1. Quarantine missing cart IDs
spark.sql("""
CREATE OR REPLACE TABLE s_antigravity_sales.orders_quarantine AS
SELECT 
    id AS cart_id,
    userId AS user_id,
    _ingestion_timestamp,
    _source,
    _batch_id,
    'Missing cart ID' AS quarantine_reason
FROM b_antigravity_sales.carts
WHERE id IS NULL
""")

# 2. Cleanse and Deduplicate valid orders
# Wrap EXPLODE in a subquery first, then LEFT JOIN with s_antigravity_sales.products
spark.sql("""
CREATE OR REPLACE TABLE s_antigravity_sales.orders AS
WITH exploded_orders AS (
    SELECT 
        CAST(id AS INT) AS cart_id,
        CAST(userId AS INT) AS user_id,
        _ingestion_timestamp,
        _source,
        _batch_id,
        EXPLODE(products) AS item
    FROM b_antigravity_sales.carts
    WHERE id IS NOT NULL
),
enriched_orders AS (
    SELECT 
        o.cart_id,
        o.user_id,
        CURRENT_DATE() AS order_date,
        CAST(o.item.id AS INT) AS product_id,
        o.item.title AS product_title,
        p.category AS category,
        CAST(o.item.price AS DOUBLE) AS price,
        CAST(o.item.quantity AS INT) AS quantity,
        ROUND(COALESCE(o.item.total, o.item.price * o.item.quantity), 2) AS line_total,
        o._ingestion_timestamp,
        o._source,
        o._batch_id
    FROM exploded_orders o
    LEFT JOIN s_antigravity_sales.products p ON o.item.id = p.product_id
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
    _source,
    _batch_id
FROM deduped_orders
WHERE row_num = 1
""")

print("Cleaned orders loaded to s_antigravity_sales.orders")
