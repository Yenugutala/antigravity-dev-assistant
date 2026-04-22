# Databricks notebook source

# COMMAND ----------

# Cell 1: Create Silver Schema
spark.sql("CREATE SCHEMA IF NOT EXISTS s_salesorders")

# COMMAND ----------

# Cell 2: Silver Products — Cleanse and Deduplicate
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
)
WHERE row_num = 1
""")

print("s_salesorders.products created")
spark.sql("SELECT * FROM s_salesorders.products LIMIT 5").show(truncate=False)

# COMMAND ----------

# Cell 3: Silver Orders — Explode Carts, Enrich with Products
# Step 1: Explode cart items into individual rows (subquery)
# Step 2: JOIN with silver products for category enrichment
# Per silver rule: NEVER combine LATERAL VIEW EXPLODE + JOIN in same FROM clause

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
        e.order_date,
        e.product_id,
        e.product_title,
        COALESCE(p.category, 'unknown') AS category,
        COALESCE(p.price, e.item_price) AS price,
        e.quantity,
        ROUND(COALESCE(e.item_total, e.item_price * e.quantity), 2) AS line_total,
        ROW_NUMBER() OVER (PARTITION BY e.cart_id, e.product_id ORDER BY e._ingestion_timestamp DESC) AS row_num
    FROM (
        SELECT
            c.id AS cart_id,
            c.userId AS user_id,
            CURRENT_DATE() AS order_date,
            item.id AS product_id,
            item.title AS product_title,
            item.price AS item_price,
            item.quantity AS quantity,
            item.total AS item_total,
            c._ingestion_timestamp
        FROM b_salesorders.carts c
        LATERAL VIEW EXPLODE(c.products) AS item
    ) e
    LEFT JOIN s_salesorders.products p ON e.product_id = p.product_id
)
WHERE row_num = 1
""")

print("s_salesorders.orders created")
spark.sql("SELECT * FROM s_salesorders.orders LIMIT 5").show(truncate=False)

# COMMAND ----------

# Cell 4: Silver Customers — Flatten and Deduplicate
spark.sql("""
CREATE OR REPLACE TABLE s_salesorders.customers AS
SELECT
    customer_id,
    first_name,
    last_name,
    email,
    username,
    phone,
    street,
    city,
    zipcode
FROM (
    SELECT
        id AS customer_id,
        firstName AS first_name,
        lastName AS last_name,
        LOWER(TRIM(email)) AS email,
        LOWER(TRIM(username)) AS username,
        phone,
        address.address AS street,
        address.city AS city,
        address.postalCode AS zipcode,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingestion_timestamp DESC) AS row_num
    FROM b_salesorders.users
    WHERE id IS NOT NULL
)
WHERE row_num = 1
""")

print("s_salesorders.customers created")
spark.sql("SELECT * FROM s_salesorders.customers LIMIT 5").show(truncate=False)

# COMMAND ----------

# Cell 5: Quarantine — Invalid records with null cart ID
spark.sql("""
CREATE OR REPLACE TABLE s_salesorders.orders_quarantine AS
SELECT
    id,
    userId,
    total,
    _ingestion_timestamp,
    _source,
    _batch_id,
    'Missing cart ID' AS quarantine_reason
FROM b_salesorders.carts
WHERE id IS NULL
""")

quarantine_count = spark.sql("SELECT COUNT(*) AS cnt FROM s_salesorders.orders_quarantine").collect()[0]["cnt"]
print(f"s_salesorders.orders_quarantine: {quarantine_count} records quarantined")

# COMMAND ----------

# Cell 6: Silver Summary
print("=" * 60)
print("SILVER CLEANSING COMPLETE")
print("=" * 60)
products_count = spark.sql("SELECT COUNT(*) AS cnt FROM s_salesorders.products").collect()[0]["cnt"]
orders_count = spark.sql("SELECT COUNT(*) AS cnt FROM s_salesorders.orders").collect()[0]["cnt"]
customers_count = spark.sql("SELECT COUNT(*) AS cnt FROM s_salesorders.customers").collect()[0]["cnt"]
print(f"  s_salesorders.products   — {products_count} rows")
print(f"  s_salesorders.orders     — {orders_count} rows")
print(f"  s_salesorders.customers  — {customers_count} rows")
print(f"  s_salesorders.orders_quarantine — {quarantine_count} rows")
