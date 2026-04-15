# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer — Sales Orders Cleansing & Conformance
# MAGIC **Input**: b_salesorders.products, b_salesorders.carts, b_salesorders.users
# MAGIC **Output**: s_salesorders.products, s_salesorders.orders, s_salesorders.customers
# MAGIC **Pattern**: Spark SQL (PySQL) — cleanse, flatten, deduplicate, quarantine

# COMMAND ----------

from datetime import datetime
spark.sql("CREATE SCHEMA IF NOT EXISTS s_salesorders")
print("Schema s_salesorders ready")
print(f"Silver Cleansing Started: {datetime.now()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Cleanse Products

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE TABLE s_salesorders.products AS
    SELECT
        id AS product_id,
        TRIM(title) AS title,
        CAST(price AS DOUBLE) AS price,
        TRIM(LOWER(category)) AS category,
        description,
        CAST(rating AS DOUBLE) AS rating_score,
        _ingestion_timestamp
    FROM b_salesorders.products
    WHERE id IS NOT NULL
      AND price >= 0
""")

products_count = spark.sql("SELECT COUNT(*) as cnt FROM s_salesorders.products").collect()[0]["cnt"]
print(f"✓ s_salesorders.products: {products_count} rows")

# COMMAND ----------

display(spark.sql("SELECT product_id, title, price, category, rating_score FROM s_salesorders.products LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Cleanse & Flatten Orders (Explode cart items, join with products)

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE TABLE s_salesorders.orders AS
    WITH exploded_carts AS (
        SELECT
            c.id AS cart_id,
            c.userId AS user_id,
            item.id AS product_id,
            item.quantity AS quantity,
            item.price AS item_price,
            item.total AS item_total,
            c._ingestion_timestamp
        FROM b_salesorders.carts c
        LATERAL VIEW EXPLODE(c.products) AS item
        WHERE c.id IS NOT NULL
    ),
    enriched AS (
        SELECT
            ec.cart_id,
            ec.user_id,
            CURRENT_DATE() AS order_date,
            ec.product_id,
            p.title AS product_title,
            p.category,
            COALESCE(ec.item_price, p.price) AS price,
            ec.quantity,
            ROUND(COALESCE(ec.item_total, ec.item_price * ec.quantity, p.price * ec.quantity), 2) AS line_total,
            ec._ingestion_timestamp
        FROM exploded_carts ec
        LEFT JOIN s_salesorders.products p ON ec.product_id = p.product_id
    ),
    ranked AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY cart_id, product_id
                ORDER BY _ingestion_timestamp DESC
            ) AS rn
        FROM enriched
    )
    SELECT
        cart_id, user_id, order_date, product_id, product_title,
        category, price, quantity, line_total, _ingestion_timestamp
    FROM ranked
    WHERE rn = 1
""")

orders_count = spark.sql("SELECT COUNT(*) as cnt FROM s_salesorders.orders").collect()[0]["cnt"]
print(f"✓ s_salesorders.orders: {orders_count} rows")

# COMMAND ----------

display(spark.sql("""
    SELECT cart_id, user_id, order_date, product_title, category, price, quantity, line_total
    FROM s_salesorders.orders
    ORDER BY cart_id, product_id
    LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Cleanse Customers (Flatten address struct)

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE TABLE s_salesorders.customers AS
    WITH ranked AS (
        SELECT
            id AS customer_id,
            LOWER(TRIM(email)) AS email,
            LOWER(TRIM(username)) AS username,
            firstName AS first_name,
            lastName AS last_name,
            phone,
            address.city AS city,
            address.address AS street,
            address.postalCode AS zipcode,
            _ingestion_timestamp,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingestion_timestamp DESC) AS rn
        FROM b_salesorders.users
        WHERE id IS NOT NULL
    )
    SELECT
        customer_id, email, username, first_name, last_name,
        phone, city, street, zipcode, _ingestion_timestamp
    FROM ranked
    WHERE rn = 1
""")

customers_count = spark.sql("SELECT COUNT(*) as cnt FROM s_salesorders.customers").collect()[0]["cnt"]
print(f"✓ s_salesorders.customers: {customers_count} rows")

# COMMAND ----------

display(spark.sql("SELECT customer_id, email, first_name, last_name, city FROM s_salesorders.customers LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Quarantine Invalid Records

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE TABLE s_salesorders.orders_quarantine AS
    SELECT *, 'Missing cart ID' AS quarantine_reason
    FROM b_salesorders.carts
    WHERE id IS NULL
""")

quarantine_count = spark.sql("SELECT COUNT(*) as cnt FROM s_salesorders.orders_quarantine").collect()[0]["cnt"]
print(f"✓ s_salesorders.orders_quarantine: {quarantine_count} rows quarantined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Summary

# COMMAND ----------

print("=" * 50)
print("SILVER CLEANSING COMPLETE")
print("=" * 50)
for table in ["products", "orders", "customers", "orders_quarantine"]:
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM s_salesorders.{table}").collect()[0]["cnt"]
    print(f"  s_salesorders.{table}: {count} rows")
