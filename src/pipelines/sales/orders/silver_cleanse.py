# Databricks notebook source

# COMMAND ----------
# Silver Cleansing: Sales Orders
# Cleanses, deduplicates, and transforms bronze data
# Writes to s_salesorders schema as Delta tables

# COMMAND ----------
SCHEMA_NAME = "s_salesorders"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")

# COMMAND ----------
# ── Silver Products ──
# Cleanse, rename columns, deduplicate

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

print("✓ Created s_salesorders.products")
spark.sql("SELECT * FROM s_salesorders.products LIMIT 5").show(truncate=False)

# COMMAND ----------
# ── Silver Orders ──
# Explode cart items, join with products for category, deduplicate
# NOTE: EXPLODE wrapped in subquery first, then JOIN (per silver rules)

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
            p.category,
            COALESCE(e.item_price, p.price) AS price,
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
        LEFT JOIN s_salesorders.products p
            ON e.product_id = p.product_id
    )
    WHERE row_num = 1
""")

print("✓ Created s_salesorders.orders")
spark.sql("SELECT * FROM s_salesorders.orders LIMIT 5").show(truncate=False)

# COMMAND ----------
# ── Silver Customers ──
# Flatten address, rename columns, deduplicate

spark.sql("""
    CREATE OR REPLACE TABLE s_salesorders.customers AS
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        username,
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
            address.address AS street,
            address.city AS city,
            address.postalCode AS zipcode,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingestion_timestamp DESC) AS row_num
        FROM b_salesorders.users
        WHERE id IS NOT NULL
    )
    WHERE row_num = 1
""")

print("✓ Created s_salesorders.customers")
spark.sql("SELECT * FROM s_salesorders.customers LIMIT 5").show(truncate=False)

# COMMAND ----------
# ── Quarantine: Invalid Cart Records ──
# Route records with null cart IDs to quarantine table

spark.sql("""
    CREATE OR REPLACE TABLE s_salesorders.orders_quarantine AS
    SELECT
        id AS cart_id,
        userId AS user_id,
        total,
        _ingestion_timestamp,
        _source,
        'Missing cart ID' AS quarantine_reason
    FROM b_salesorders.carts
    WHERE id IS NULL
""")

quarantine_count = spark.sql("SELECT COUNT(*) AS cnt FROM s_salesorders.orders_quarantine").collect()[0]["cnt"]
print(f"✓ Created s_salesorders.orders_quarantine ({quarantine_count} records quarantined)")

# COMMAND ----------
print("✓ Silver cleansing complete")
