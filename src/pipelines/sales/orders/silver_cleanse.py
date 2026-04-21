# Databricks notebook source

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS s_salesorders

# COMMAND ----------

# ---- Silver: Products ----
# Cleanse, rename, deduplicate products from bronze

spark.sql("""
CREATE OR REPLACE TABLE s_salesorders.products AS
SELECT
    product_id,
    title,
    price,
    category,
    rating_score
FROM (
    SELECT
        id AS product_id,
        TRIM(title) AS title,
        CAST(price AS DOUBLE) AS price,
        LOWER(TRIM(category)) AS category,
        CAST(rating AS DOUBLE) AS rating_score,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingestion_timestamp DESC) AS row_num
    FROM b_salesorders.products
    WHERE id IS NOT NULL AND price >= 0
) deduped
WHERE row_num = 1
""")

display(spark.sql("SELECT * FROM s_salesorders.products LIMIT 5"))

# COMMAND ----------

# ---- Silver: Orders (Exploded Cart Line Items) ----
# Step 1: LATERAL VIEW EXPLODE in subquery (NEVER combine with JOIN)
# Step 2: LEFT JOIN with products for category enrichment in outer query

spark.sql("""
CREATE OR REPLACE TABLE s_salesorders.orders AS
SELECT
    e.cart_id,
    e.user_id,
    e.order_date,
    e.product_id,
    e.product_title,
    p.category,
    e.price,
    e.quantity,
    ROUND(COALESCE(e.item_total, e.price * e.quantity), 2) AS line_total
FROM (
    SELECT
        c.id AS cart_id,
        c.userId AS user_id,
        CURRENT_DATE() AS order_date,
        item.id AS product_id,
        item.title AS product_title,
        item.price AS price,
        item.quantity AS quantity,
        item.total AS item_total,
        c._ingestion_timestamp
    FROM b_salesorders.carts c
    LATERAL VIEW EXPLODE(c.products) AS item
) e
LEFT JOIN s_salesorders.products p ON e.product_id = p.product_id
""")

# COMMAND ----------

# ---- Deduplicate Orders ----

spark.sql("""
CREATE OR REPLACE TABLE s_salesorders.orders AS
SELECT
    cart_id, user_id, order_date, product_id, product_title,
    category, price, quantity, line_total
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY cart_id, product_id ORDER BY cart_id) AS row_num
    FROM s_salesorders.orders
) deduped
WHERE row_num = 1
""")

display(spark.sql("SELECT * FROM s_salesorders.orders LIMIT 5"))

# COMMAND ----------

# ---- Silver: Customers ----
# Flatten address struct, rename columns, deduplicate

spark.sql("""
CREATE OR REPLACE TABLE s_salesorders.customers AS
SELECT
    customer_id,
    first_name,
    last_name,
    email,
    username,
    city,
    street,
    zipcode
FROM (
    SELECT
        id AS customer_id,
        firstName AS first_name,
        lastName AS last_name,
        LOWER(TRIM(email)) AS email,
        LOWER(TRIM(username)) AS username,
        address.city AS city,
        address.address AS street,
        address.postalCode AS zipcode,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingestion_timestamp DESC) AS row_num
    FROM b_salesorders.users
    WHERE id IS NOT NULL
) deduped
WHERE row_num = 1
""")

display(spark.sql("SELECT * FROM s_salesorders.customers LIMIT 5"))

# COMMAND ----------

# ---- Quarantine: Records with null cart IDs ----

spark.sql("""
CREATE OR REPLACE TABLE s_salesorders.orders_quarantine AS
SELECT
    c.id AS cart_id,
    c.userId AS user_id,
    c.total,
    c._ingestion_timestamp,
    'Missing cart ID' AS _quarantine_reason
FROM b_salesorders.carts c
WHERE c.id IS NULL
""")

display(spark.sql("SELECT COUNT(*) AS quarantine_count FROM s_salesorders.orders_quarantine"))

# COMMAND ----------

print("Silver cleansing complete.")
print("Tables created: s_salesorders.products, s_salesorders.orders, s_salesorders.customers, s_salesorders.orders_quarantine")
