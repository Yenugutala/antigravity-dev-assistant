# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer: Sales Orders — Cleansing & Transformation
# MAGIC Reads from `b_salesorders`, applies cleansing rules, writes to `s_salesorders`.

# COMMAND ----------

# Create silver schema
spark.sql("CREATE SCHEMA IF NOT EXISTS s_salesorders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Products — Cleanse & Deduplicate

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE TABLE s_salesorders.products AS
    SELECT
        product_id,
        title,
        price,
        category,
        rating_score,
        _ingestion_timestamp
    FROM (
        SELECT
            id AS product_id,
            TRIM(title) AS title,
            CAST(price AS DOUBLE) AS price,
            LOWER(TRIM(category)) AS category,
            CAST(rating AS DOUBLE) AS rating_score,
            _ingestion_timestamp,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingestion_timestamp DESC) AS row_num
        FROM b_salesorders.products
        WHERE id IS NOT NULL AND price >= 0
    )
    WHERE row_num = 1
""")

print("✅ s_salesorders.products created")

# COMMAND ----------

display(spark.sql("SELECT * FROM s_salesorders.products LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orders — Explode Cart Items, Enrich with Products, Deduplicate

# COMMAND ----------

# Quarantine: carts with null IDs
spark.sql("""
    CREATE OR REPLACE TABLE s_salesorders.orders_quarantine AS
    SELECT
        id AS cart_id,
        userId AS user_id,
        _ingestion_timestamp,
        'Missing cart ID' AS quarantine_reason
    FROM b_salesorders.carts
    WHERE id IS NULL
""")

print("✅ s_salesorders.orders_quarantine created")

# COMMAND ----------

# Explode cart items into individual order lines, then join with products
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
            COALESCE(p.price, e.item_price) AS price,
            e.quantity,
            ROUND(COALESCE(e.item_total, COALESCE(p.price, e.item_price) * e.quantity), 2) AS line_total,
            e._ingestion_timestamp,
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
            WHERE c.id IS NOT NULL
        ) e
        LEFT JOIN s_salesorders.products p
            ON e.product_id = p.product_id
    )
    WHERE row_num = 1
""")

print("✅ s_salesorders.orders created")

# COMMAND ----------

display(spark.sql("SELECT * FROM s_salesorders.orders LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers — Cleanse, Flatten Address, Deduplicate

# COMMAND ----------

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
        zipcode,
        _ingestion_timestamp
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
            _ingestion_timestamp,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingestion_timestamp DESC) AS row_num
        FROM b_salesorders.users
        WHERE id IS NOT NULL
    )
    WHERE row_num = 1
""")

print("✅ s_salesorders.customers created")

# COMMAND ----------

display(spark.sql("SELECT * FROM s_salesorders.customers LIMIT 5"))

# COMMAND ----------

print("🏁 Silver cleansing complete — all tables written to s_salesorders schema")
