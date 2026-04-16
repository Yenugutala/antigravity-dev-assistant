# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer: Sales Orders — Cleansing & Transformation
# MAGIC Reads from `b_salesorders`, creates cleansed tables in `s_salesorders` schema.

# COMMAND ----------

SILVER_SCHEMA = "s_salesorders"
BRONZE_SCHEMA = "b_salesorders"

# COMMAND ----------

# Create Silver schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")

# COMMAND ----------

# --- Silver Products ---
# Source: b_salesorders.products
# Transformations: rename id→product_id, TRIM title, LOWER category, CAST price/rating
# Filter: id IS NOT NULL AND price >= 0
# Dedup: ROW_NUMBER by product_id, ordered by _ingestion_timestamp DESC

print("Processing silver products...")

spark.sql(f"""
    CREATE OR REPLACE TABLE {SILVER_SCHEMA}.products AS
    SELECT product_id, title, price, category, rating_score, brand, description
    FROM (
        SELECT
            id AS product_id,
            TRIM(title) AS title,
            CAST(price AS DOUBLE) AS price,
            LOWER(TRIM(category)) AS category,
            CAST(rating AS DOUBLE) AS rating_score,
            brand,
            description,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingestion_timestamp DESC) AS rn
        FROM {BRONZE_SCHEMA}.products
        WHERE id IS NOT NULL AND price >= 0
    )
    WHERE rn = 1
""")

print(f"Wrote products to {SILVER_SCHEMA}.products")
display(spark.table(f"{SILVER_SCHEMA}.products"))

# COMMAND ----------

# --- Silver Orders ---
# Source: b_salesorders.carts (EXPLODE products array)
# Join with s_salesorders.products for category enrichment
# CRITICAL: LATERAL VIEW EXPLODE must be in a subquery before LEFT JOIN

print("Processing silver orders...")

spark.sql(f"""
    CREATE OR REPLACE TABLE {SILVER_SCHEMA}.orders AS
    SELECT
        e.cart_id,
        e.user_id,
        e.order_date,
        e.product_id,
        e.product_title,
        COALESCE(p.category, 'unknown') AS category,
        e.price,
        e.quantity,
        e.line_total
    FROM (
        SELECT
            c.id AS cart_id,
            c.userId AS user_id,
            CURRENT_DATE() AS order_date,
            item.id AS product_id,
            item.title AS product_title,
            CAST(item.price AS DOUBLE) AS price,
            item.quantity AS quantity,
            ROUND(CAST(item.total AS DOUBLE), 2) AS line_total,
            c._ingestion_timestamp AS ingestion_ts,
            ROW_NUMBER() OVER (
                PARTITION BY c.id, item.id
                ORDER BY c._ingestion_timestamp DESC
            ) AS rn
        FROM {BRONZE_SCHEMA}.carts c
        LATERAL VIEW EXPLODE(c.products) AS item
    ) e
    LEFT JOIN {SILVER_SCHEMA}.products p ON e.product_id = p.product_id
    WHERE e.rn = 1
""")

print(f"Wrote orders to {SILVER_SCHEMA}.orders")
display(spark.table(f"{SILVER_SCHEMA}.orders"))

# COMMAND ----------

# --- Silver Customers ---
# Source: b_salesorders.users
# Transformations: rename fields, LOWER email/username, flatten address struct
# Filter: id IS NOT NULL
# Dedup: ROW_NUMBER by customer_id

print("Processing silver customers...")

spark.sql(f"""
    CREATE OR REPLACE TABLE {SILVER_SCHEMA}.customers AS
    SELECT customer_id, first_name, last_name, email, phone, username, street, city, zipcode
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
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingestion_timestamp DESC) AS rn
        FROM {BRONZE_SCHEMA}.users
        WHERE id IS NOT NULL
    )
    WHERE rn = 1
""")

print(f"Wrote customers to {SILVER_SCHEMA}.customers")
display(spark.table(f"{SILVER_SCHEMA}.customers"))

# COMMAND ----------

# --- Quarantine: Invalid Carts (NULL cart ID) ---

print("Processing quarantine records...")

spark.sql(f"""
    CREATE OR REPLACE TABLE {SILVER_SCHEMA}.orders_quarantine AS
    SELECT
        id AS cart_id,
        userId AS user_id,
        'missing_cart_id' AS quarantine_reason,
        CURRENT_TIMESTAMP() AS quarantine_timestamp
    FROM {BRONZE_SCHEMA}.carts
    WHERE id IS NULL
""")

print(f"Wrote quarantine to {SILVER_SCHEMA}.orders_quarantine")
display(spark.table(f"{SILVER_SCHEMA}.orders_quarantine"))

# COMMAND ----------

# --- Summary ---

print("=" * 60)
print("SILVER CLEANSING COMPLETE")
print("=" * 60)
print(f"Schema: {SILVER_SCHEMA}")
for table in ["products", "orders", "customers", "orders_quarantine"]:
    count = spark.table(f"{SILVER_SCHEMA}.{table}").count()
    print(f"  {SILVER_SCHEMA}.{table}: {count} rows")
print("=" * 60)
