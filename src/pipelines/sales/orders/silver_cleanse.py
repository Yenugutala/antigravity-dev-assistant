# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer: Sales Orders — Cleansing & Transformation
# MAGIC Reads from `b_salesorders`, cleanses and transforms into `s_salesorders` schema.

# COMMAND ----------

SILVER_SCHEMA = "s_salesorders"
BRONZE_SCHEMA = "b_salesorders"

# COMMAND ----------

# Create Silver schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")

# COMMAND ----------

# --- Silver Products ---
# Source: b_salesorders.products
# Transformations: rename id→product_id, TRIM title, CAST price, LOWER category, rename rating→rating_score
# Filter: id IS NOT NULL AND price >= 0
# Dedup: partition by product_id, order by _ingestion_timestamp DESC

print("Processing silver products...")

spark.sql(f"""
    CREATE OR REPLACE TABLE {SILVER_SCHEMA}.products AS
    SELECT
        product_id,
        title,
        price,
        category,
        rating_score,
        brand,
        description
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
display(spark.table(f"{SILVER_SCHEMA}.products").limit(5))

# COMMAND ----------

# --- Silver Orders ---
# Source: b_salesorders.carts
# Transformations: LATERAL VIEW EXPLODE products, LEFT JOIN with s_salesorders.products for category
# Dedup: partition by cart_id, product_id, order by _ingestion_timestamp DESC

print("Processing silver orders...")

spark.sql(f"""
    CREATE OR REPLACE TABLE {SILVER_SCHEMA}.orders AS
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
            c.id AS cart_id,
            c.userId AS user_id,
            CURRENT_DATE() AS order_date,
            item.id AS product_id,
            item.title AS product_title,
            p.category AS category,
            CAST(item.price AS DOUBLE) AS price,
            item.quantity AS quantity,
            ROUND(CAST(item.total AS DOUBLE), 2) AS line_total,
            ROW_NUMBER() OVER (PARTITION BY c.id, item.id ORDER BY c._ingestion_timestamp DESC) AS rn
        FROM {BRONZE_SCHEMA}.carts c
        LATERAL VIEW EXPLODE(c.products) AS item
        LEFT JOIN {SILVER_SCHEMA}.products p ON item.id = p.product_id
    )
    WHERE rn = 1
""")

print(f"Wrote orders to {SILVER_SCHEMA}.orders")
display(spark.table(f"{SILVER_SCHEMA}.orders").limit(5))

# COMMAND ----------

# --- Silver Customers ---
# Source: b_salesorders.users
# Transformations: rename fields, flatten address struct, LOWER email/username
# Filter: id IS NOT NULL
# Dedup: partition by customer_id, order by _ingestion_timestamp DESC

print("Processing silver customers...")

spark.sql(f"""
    CREATE OR REPLACE TABLE {SILVER_SCHEMA}.customers AS
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
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingestion_timestamp DESC) AS rn
        FROM {BRONZE_SCHEMA}.users
        WHERE id IS NOT NULL
    )
    WHERE rn = 1
""")

print(f"Wrote customers to {SILVER_SCHEMA}.customers")
display(spark.table(f"{SILVER_SCHEMA}.customers").limit(5))

# COMMAND ----------

# --- Quarantine: Invalid Orders ---
# Records with NULL cart ID are routed here

print("Processing quarantine records...")

spark.sql(f"""
    CREATE OR REPLACE TABLE {SILVER_SCHEMA}.orders_quarantine AS
    SELECT
        c.id AS cart_id,
        c.userId AS user_id,
        item.id AS product_id,
        item.title AS product_title,
        item.price AS price,
        item.quantity AS quantity,
        item.total AS line_total,
        'Missing cart ID' AS quarantine_reason,
        CURRENT_TIMESTAMP() AS quarantine_timestamp
    FROM {BRONZE_SCHEMA}.carts c
    LATERAL VIEW EXPLODE(c.products) AS item
    WHERE c.id IS NULL
""")

quarantine_count = spark.table(f"{SILVER_SCHEMA}.orders_quarantine").count()
print(f"Quarantined {quarantine_count} records to {SILVER_SCHEMA}.orders_quarantine")

# COMMAND ----------

# --- Summary ---

print("=" * 60)
print("SILVER TRANSFORMATION COMPLETE")
print("=" * 60)
print(f"Schema: {SILVER_SCHEMA}")
print(f"Tables created:")
for table in ["products", "orders", "customers", "orders_quarantine"]:
    count = spark.table(f"{SILVER_SCHEMA}.{table}").count()
    print(f"  {SILVER_SCHEMA}.{table}: {count} rows")
print("=" * 60)
