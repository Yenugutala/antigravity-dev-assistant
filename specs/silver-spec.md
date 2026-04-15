---
# SILVER SPECIFICATION: Sales Orders — Cleansing & Transformation
# Reference: docs/business-requirement.md

pipeline_name: "salesorders-silver"
domain: "sales"
entity: "orders"
layer: "silver"
owner: "data-engineering-team"
version: "1.0"

source_schema: "b_salesorders"
target_schema: "s_salesorders"

tables:
  products:
    source: "b_salesorders.products"
    target: "s_salesorders.products"
    transformations:
      - column: "id"
        rule: "Rename to product_id"
      - column: "title"
        rule: "TRIM whitespace"
      - column: "price"
        rule: "CAST to DOUBLE"
      - column: "category"
        rule: "LOWER and TRIM"
      - column: "rating"
        rule: "CAST to DOUBLE, rename to rating_score"
    filter: "id IS NOT NULL AND price >= 0"
    deduplication:
      key: "product_id"
      order_by: "_ingestion_timestamp DESC"
      strategy: "keep_latest"

  orders:
    source: "b_salesorders.carts"
    target: "s_salesorders.orders"
    transformations:
      - rule: "LATERAL VIEW EXPLODE products array into individual line items"
      - rule: "LEFT JOIN with s_salesorders.products to enrich with category and price"
      - rule: "Calculate line_total = ROUND(item.total OR price * quantity, 2)"
      - rule: "Set order_date = CURRENT_DATE()"
    output_columns:
      - "cart_id"
      - "user_id"
      - "order_date"
      - "product_id"
      - "product_title"
      - "category"
      - "price"
      - "quantity"
      - "line_total"
    deduplication:
      key: ["cart_id", "product_id"]
      order_by: "_ingestion_timestamp DESC"
      strategy: "keep_latest"

  customers:
    source: "b_salesorders.users"
    target: "s_salesorders.customers"
    transformations:
      - column: "id"
        rule: "Rename to customer_id"
      - column: "email"
        rule: "LOWER and TRIM"
      - column: "username"
        rule: "LOWER and TRIM"
      - column: "firstName"
        rule: "Rename to first_name"
      - column: "lastName"
        rule: "Rename to last_name"
      - column: "address.city"
        rule: "Flatten to city"
      - column: "address.address"
        rule: "Flatten to street"
      - column: "address.postalCode"
        rule: "Flatten to zipcode"
    filter: "id IS NOT NULL"
    deduplication:
      key: "customer_id"
      order_by: "_ingestion_timestamp DESC"
      strategy: "keep_latest"

quarantine:
  table: "s_salesorders.orders_quarantine"
  rules:
    - condition: "id IS NULL"
      reason: "Missing cart ID"
---

## Transformation Notes

- All Spark SQL (PySQL pattern) — `spark.sql("CREATE OR REPLACE TABLE ...")`
- Cart products are exploded using `LATERAL VIEW EXPLODE` to create one row per line item
- Products are joined to orders to bring in category and price information
- Deduplication uses `ROW_NUMBER() OVER (PARTITION BY key ORDER BY timestamp DESC)` pattern
- Invalid records (null IDs) are routed to a quarantine table
