# Databricks notebook source
# COMMAND ----------
# CREATE SCHEMA IF NOT EXISTS
spark.sql("CREATE SCHEMA IF NOT EXISTS s_salesorders")

# COMMAND ----------
# LINT & STANDARDS: UPPERCASE SQL keywords, snake_case columns.

# COMMAND ----------
# PROCESS PRODUCTS TABLE
spark.sql("""
CREATE OR REPLACE TABLE s_salesorders.products AS
WITH raw_products AS (
  SELECT 
    id,
    title,
    price,
    category,
    rating,
    brand,
    description,
    _ingestion_timestamp,
    _source,
    _batch_id,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingestion_timestamp DESC) AS row_num
  FROM b_salesorders.products
  WHERE id IS NOT NULL AND price >= 0
)
SELECT 
  id AS product_id,
  TRIM(title) AS title,
  CAST(price AS DOUBLE) AS price,
  LOWER(TRIM(category)) AS category,
  CAST(rating AS DOUBLE) AS rating_score,
  brand,
  description,
  _ingestion_timestamp,
  _source,
  _batch_id
FROM raw_products
WHERE row_num = 1
""")

# COMMAND ----------
# PROCESS CUSTOMERS TABLE
spark.sql("""
CREATE OR REPLACE TABLE s_salesorders.customers AS
WITH raw_users AS (
  SELECT 
    id,
    firstName,
    lastName,
    email,
    username,
    address,
    _ingestion_timestamp,
    _source,
    _batch_id,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingestion_timestamp DESC) AS row_num
  FROM b_salesorders.users
  WHERE id IS NOT NULL
)
SELECT 
  id AS customer_id,
  LOWER(TRIM(email)) AS email,
  LOWER(TRIM(username)) AS username,
  firstName AS first_name,
  lastName AS last_name,
  address.city AS city,
  address.address AS street,
  address.postalCode AS zipcode,
  _ingestion_timestamp,
  _source,
  _batch_id
FROM raw_users
WHERE row_num = 1
""")

# COMMAND ----------
# PROCESS ORDERS TABLE (WITH EXPLOSION SAFETY AND QUARANTINE ROUTING)

# Step 1: Create Quarantine Table if missing cart ID
spark.sql("""
CREATE TABLE IF NOT EXISTS s_salesorders.orders_quarantine (
  cart_id INT,
  user_id INT,
  product_id INT,
  product_title STRING,
  price DOUBLE,
  quantity INT,
  line_total DOUBLE,
  quarantine_reason STRING,
  _ingestion_timestamp STRING,
  _source STRING,
  _batch_id STRING
) USING DELTA
""")

# Step 2: Route invalid cart records (null IDs) to Quarantine
spark.sql("""
INSERT INTO s_salesorders.orders_quarantine
SELECT 
  id AS cart_id,
  userId AS user_id,
  CAST(NULL AS INT) AS product_id,
  CAST(NULL AS STRING) AS product_title,
  CAST(NULL AS DOUBLE) AS price,
  CAST(NULL AS INT) AS quantity,
  CAST(NULL AS DOUBLE) AS line_total,
  'Missing cart ID' AS quarantine_reason,
  _ingestion_timestamp,
  _source,
  _batch_id
FROM b_salesorders.carts
WHERE id IS NULL
""")

# Step 3: Explode and Enrich Valid Orders
spark.sql("""
CREATE OR REPLACE TABLE s_salesorders.orders AS
WITH exploded_carts AS (
  SELECT 
    id AS cart_id,
    userId AS user_id,
    _ingestion_timestamp,
    _source,
    _batch_id,
    exploded_product.id AS product_id,
    exploded_product.title AS product_title,
    exploded_product.price AS product_price,
    exploded_product.quantity AS quantity,
    exploded_product.total AS product_total
  FROM b_salesorders.carts
  LATERAL VIEW EXPLODE(products) AS exploded_product
  WHERE id IS NOT NULL
),
enriched_orders AS (
  SELECT 
    ec.cart_id,
    ec.user_id,
    CURRENT_DATE() AS order_date,
    ec.product_id,
    ec.product_title,
    p.category,
    COALESCE(p.price, ec.product_price) AS price,
    ec.quantity,
    ROUND(COALESCE(ec.product_total, COALESCE(p.price, ec.product_price) * ec.quantity), 2) AS line_total,
    ec._ingestion_timestamp,
    ec._source,
    ec._batch_id,
    ROW_NUMBER() OVER (PARTITION BY ec.cart_id, ec.product_id ORDER BY ec._ingestion_timestamp DESC) AS row_num
  FROM exploded_carts ec
  LEFT JOIN s_salesorders.products p ON ec.product_id = p.product_id
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
FROM enriched_orders
WHERE row_num = 1
""")

# COMMAND ----------
# VERIFICATION QUERY
print("Cleansing batch completed successfully.")
spark.sql("SELECT count(*) FROM s_salesorders.orders").show()
