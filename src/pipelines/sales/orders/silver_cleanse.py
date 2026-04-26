# Databricks notebook source

# COMMAND ----------

# Silver Layer: Sales Orders — Cleansing & Transformation
# Reads from b_salesorders, cleanses, deduplicates, and writes to s_salesorders

# COMMAND ----------

SILVER_SCHEMA = "s_salesorders"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")

# COMMAND ----------

# ── Silver Products ──────────────────────────────────────────────
# Rename columns, trim strings, cast types, deduplicate

spark.sql("""
CREATE OR REPLACE TABLE s_salesorders.products AS
SELECT
    product_id,
    title,
    price,
    category,
    rating_score,
    _ingestion_timestamp,
    _source,
    _batch_id
FROM (
    SELECT
        id AS product_id,
        TRIM(title) AS title,
        CAST(price AS DOUBLE) AS price,
        LOWER(TRIM(category)) AS category,
        CAST(rating AS DOUBLE) AS rating_score,
        _ingestion_timestamp,
        _source,
        _batch_id,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingestion_timestamp DESC) AS row_num
    FROM b_salesorders.products
    WHERE id IS NOT NULL AND price >= 0
) deduped
WHERE row_num = 1
""")

print("Created s_salesorders.products")
display(spark.sql("SELECT * FROM s_salesorders.products LIMIT 5"))

# COMMAND ----------

# ── Quarantine: Invalid Orders ───────────────────────────────────
# Route records with null cart_id to quarantine table

spark.sql("""
CREATE OR REPLACE TABLE s_salesorders.orders_quarantine AS
SELECT
    c.id AS cart_id,
    c.userId AS user_id,
    c.total,
    c._ingestion_timestamp,
    'Missing cart ID' AS quarantine_reason
FROM b_salesorders.carts c
WHERE c.id IS NULL
""")

print("Created s_salesorders.orders_quarantine")
display(spark.sql("SELECT * FROM s_salesorders.orders_quarantine LIMIT 5"))

# COMMAND ----------

# ── Silver Orders ────────────────────────────────────────────────
# Explode cart items in subquery FIRST, then LEFT JOIN products
# NEVER combine LATERAL VIEW EXPLODE + JOIN in same FROM clause

spark.sql("""
CREATE OR REPLACE TABLE s_salesorders.orders AS
SELECT
    cart_id,
    user_id,
    order_date,
    product_id,
    product_title,
    category,
    price,
    quantity,
    line_total
FROM (
    SELECT
        e.cart_id,
        e.user_id,
        CURRENT_DATE() AS order_date,
        e.product_id,
        e.product_title,
        p.category,
        COALESCE(p.price, e.item_price) AS price,
        e.quantity,
        ROUND(COALESCE(e.item_total, e.item_price * e.quantity), 2) AS line_total,
        ROW_NUMBER() OVER (
            PARTITION BY e.cart_id, e.product_id
            ORDER BY e._ingestion_timestamp DESC
        ) AS row_num
    FROM (
        SELECT
            c.id AS cart_id,
            c.userId AS user_id,
            item.id AS product_id,
            item.title AS product_title,
            item.price AS item_price,
            item.quantity AS quantity,
            item.total AS item_total,
            c._ingestion_timestamp
        FROM b_salesorders.carts c
        LATERAL VIEW EXPLODE(c.products) AS item
        WHERE c.id IS NOT NULL
    ) e
    LEFT JOIN s_salesorders.products p
        ON e.product_id = p.product_id
) deduped
WHERE row_num = 1
""")

print("Created s_salesorders.orders")
display(spark.sql("SELECT * FROM s_salesorders.orders LIMIT 5"))

# COMMAND ----------

# ── Silver Customers ─────────────────────────────────────────────
# Flatten address, rename columns, deduplicate

spark.sql("""
CREATE OR REPLACE TABLE s_salesorders.customers AS
SELECT
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    username,
    street,
    city,
    zipcode,
    _ingestion_timestamp,
    _source,
    _batch_id
FROM (
    SELECT
        id AS customer_id,
        firstName AS first_name,
        lastName AS last_name,
        LOWER(TRIM(email)) AS email,
        phone,
        LOWER(TRIM(username)) AS username,
        address.address AS street,
        address.city AS city,
        address.postalCode AS zipcode,
        _ingestion_timestamp,
        _source,
        _batch_id,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingestion_timestamp DESC) AS row_num
    FROM b_salesorders.users
    WHERE id IS NOT NULL
) deduped
WHERE row_num = 1
""")

print("Created s_salesorders.customers")
display(spark.sql("SELECT * FROM s_salesorders.customers LIMIT 5"))

# COMMAND ----------

print("=" * 60)
print("Silver cleansing complete!")
print(f"  Schema: {SILVER_SCHEMA}")
print("  Tables: products, orders, customers, orders_quarantine")
print("=" * 60)
