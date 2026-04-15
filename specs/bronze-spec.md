---
# BRONZE SPECIFICATION: Sales Orders — Raw Data Ingestion
# Reference: docs/business-requirement.md

pipeline_name: "salesorders-bronze"
domain: "sales"
entity: "orders"
layer: "bronze"
owner: "data-engineering-team"
version: "1.0"

source:
  system: "REST API"
  format: "json"
  base_url: "https://dummyjson.com"
  auth_required: false
  endpoints:
    - url: "/products"
      entity: "products"
      wrapper_key: "products"
    - url: "/carts"
      entity: "carts"
      wrapper_key: "carts"
    - url: "/users"
      entity: "users"
      wrapper_key: "users"

target_schema: "b_salesorders"

tables:
  products:
    columns:
      - name: "id"
        type: "integer"
        nullable: false
      - name: "title"
        type: "string"
        nullable: false
      - name: "price"
        type: "double"
        nullable: false
      - name: "category"
        type: "string"
        nullable: false
      - name: "rating"
        type: "double"
        nullable: true
      - name: "brand"
        type: "string"
        nullable: true
      - name: "description"
        type: "string"
        nullable: true

  carts:
    columns:
      - name: "id"
        type: "integer"
        nullable: false
      - name: "userId"
        type: "integer"
        nullable: false
      - name: "totalProducts"
        type: "integer"
        nullable: true
      - name: "totalQuantity"
        type: "integer"
        nullable: true
      - name: "total"
        type: "double"
        nullable: true
      - name: "products"
        type: "array<struct<id: int, title: string, price: double, quantity: int, total: double>>"
        nullable: false

  users:
    columns:
      - name: "id"
        type: "integer"
        nullable: false
      - name: "firstName"
        type: "string"
        nullable: true
      - name: "lastName"
        type: "string"
        nullable: true
      - name: "email"
        type: "string"
        nullable: false
      - name: "phone"
        type: "string"
        nullable: true
      - name: "username"
        type: "string"
        nullable: false
      - name: "address"
        type: "struct<address: string, city: string, state: string, postalCode: string>"
        nullable: true

ingestion_method: "api_call"
processing_mode: "batch"
write_mode: "overwrite"
metadata_columns:
  - "_ingestion_timestamp"
  - "_source"
  - "_batch_id"
---

## Ingestion Notes

- API responses are wrapped in a key (e.g., `{"products": [...]}`); extract using `wrapper_key`
- Use explicit Spark `StructType` schemas — never rely on schema inference
- Embed fallback sample data so the pipeline works even if the API is unreachable
- Preprocess raw JSON to strip unnecessary fields and enforce consistent numeric types (`float` for prices/ratings, `int` for IDs/quantities)
- Each notebook cell creates one Delta table in the `b_salesorders` schema
