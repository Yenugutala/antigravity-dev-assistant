---
# PIPELINE SPECIFICATION: Sales Orders Pipeline
# Source: FakeStore API (https://fakestoreapi.com)
# Medallion: Bronze (PySpark) → Silver (Spark SQL) → Gold (Spark SQL)

pipeline_name: "sales-orders"
domain: "sales"
entity: "orders"
owner: "data-engineering-team"
created_date: "2026-04-14"
version: "1.0"

source:
  system: "REST API"
  format: "json"
  path_or_endpoint: "https://fakestoreapi.com"
  auth_required: false
  schema_evolution: false
  endpoints:
    - url: "https://fakestoreapi.com/products"
      entity: "products"
    - url: "https://fakestoreapi.com/carts"
      entity: "carts"
    - url: "https://fakestoreapi.com/users"
      entity: "users"

volume:
  estimated_rows_per_day: 200
  processing_mode: "batch"
  refresh_frequency: "daily"

bronze:
  ingestion_method: "api_call"
  partition_columns: ["_ingestion_date"]
  raw_schema:
    products:
      - name: "id"
        type: "integer"
        nullable: false
      - name: "title"
        type: "string"
        nullable: false
      - name: "price"
        type: "double"
        nullable: false
      - name: "description"
        type: "string"
        nullable: true
      - name: "category"
        type: "string"
        nullable: false
      - name: "image"
        type: "string"
        nullable: true
      - name: "rating"
        type: "struct"
        nullable: true
        fields:
          - name: "rate"
            type: "double"
          - name: "count"
            type: "integer"
    carts:
      - name: "id"
        type: "integer"
        nullable: false
      - name: "userId"
        type: "integer"
        nullable: false
      - name: "date"
        type: "string"
        nullable: false
      - name: "products"
        type: "array"
        nullable: false
        element:
          - name: "productId"
            type: "integer"
          - name: "quantity"
            type: "integer"
    users:
      - name: "id"
        type: "integer"
        nullable: false
      - name: "email"
        type: "string"
        nullable: false
      - name: "username"
        type: "string"
        nullable: false
      - name: "name"
        type: "struct"
        nullable: true
        fields:
          - name: "firstname"
            type: "string"
          - name: "lastname"
            type: "string"
      - name: "phone"
        type: "string"
        nullable: true
      - name: "address"
        type: "struct"
        nullable: true

silver:
  deduplication:
    key_columns: ["id"]
    order_by: "_ingestion_timestamp"
    strategy: "keep_latest"
  cleansing_rules:
    - column: "title"
      rule: "TRIM whitespace"
    - column: "price"
      rule: "CAST to DOUBLE, reject if negative"
    - column: "category"
      rule: "LOWER and TRIM"
    - column: "email"
      rule: "LOWER and TRIM"
    - column: "username"
      rule: "LOWER and TRIM"
  quarantine_rules:
    - condition: "id IS NULL"
      reason: "Missing primary key"
    - condition: "price < 0"
      reason: "Negative price is invalid"

gold:
  aggregations:
    - name: "revenue_by_category"
      grain: "category"
      group_by: ["category"]
      metrics:
        - expression: "ROUND(SUM(price * quantity), 2)"
          alias: "total_revenue"
        - expression: "SUM(quantity)"
          alias: "total_items_sold"
        - expression: "COUNT(DISTINCT cart_id)"
          alias: "total_orders"
        - expression: "ROUND(AVG(price), 2)"
          alias: "avg_price"
    - name: "order_summary"
      grain: "daily"
      group_by: ["order_date"]
      metrics:
        - expression: "COUNT(DISTINCT cart_id)"
          alias: "total_orders"
        - expression: "COUNT(DISTINCT user_id)"
          alias: "unique_customers"
        - expression: "ROUND(SUM(price * quantity), 2)"
          alias: "total_revenue"

data_quality:
  bronze:
    - check: "row_count > 0"
    - check: "null_rate(id) == 0"
  silver:
    - check: "unique(id) for products"
    - check: "null_rate(price) == 0"
    - check: "price >= 0"
  gold:
    - check: "total_revenue >= 0"
    - check: "total_orders > 0"

consumers:
  - type: "databricks_sql"
    name: "Analytics Dashboard"
  - type: "power_bi"
    name: "Sales Report"
---

## Additional Notes

### Data Flow
```
FakeStore API → Bronze (raw JSON → Delta) → Silver (cleansed, flattened) → Gold (aggregated metrics)
```

### Business Context
This pipeline ingests product catalog, shopping cart (order), and customer data from the FakeStore API.
The Gold layer provides revenue analytics by product category and daily order summaries.

### Key Transformations
- **Bronze→Silver**: Flatten nested structs (rating, name, address), explode cart products array,
  join carts with products to get prices, deduplicate, quarantine invalid records
- **Silver→Gold**: Aggregate revenue by category, daily order summaries with customer counts
