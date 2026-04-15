# Silver Specification: Sales Orders — Cleansing & Transformation

> Reference: [Business Requirement](../docs/business-requirement.md)

## Pipeline Info

| Field | Value |
|-------|-------|
| Pipeline Name | salesorders-silver |
| Domain | sales |
| Entity | orders |
| Layer | silver |
| Owner | data-engineering-team |
| Version | 1.0 |

## Source & Target

| Field | Value |
|-------|-------|
| Source Schema | `b_salesorders` |
| Target Schema | `s_salesorders` |

---

## Table: `s_salesorders.products`

**Source**: `b_salesorders.products`

### Transformations

| Column | Rule |
|--------|------|
| id | Rename to `product_id` |
| title | TRIM whitespace |
| price | CAST to DOUBLE |
| category | LOWER and TRIM |
| rating | CAST to DOUBLE, rename to `rating_score` |

**Filter**: `id IS NOT NULL AND price >= 0`

**Deduplication**: Partition by `product_id`, order by `_ingestion_timestamp DESC`, keep latest

---

## Table: `s_salesorders.orders`

**Source**: `b_salesorders.carts`

### Transformations

1. `LATERAL VIEW EXPLODE` products array into individual line items
2. `LEFT JOIN` with `s_salesorders.products` to enrich with category and price
3. Calculate `line_total = ROUND(item.total OR price * quantity, 2)`
4. Set `order_date = CURRENT_DATE()`

### Output Columns

| Column | Description |
|--------|-------------|
| cart_id | Cart identifier |
| user_id | Customer reference |
| order_date | Date of processing |
| product_id | Product identifier |
| product_title | Product name |
| category | Product category (from products join) |
| price | Unit price |
| quantity | Quantity ordered |
| line_total | Total for this line item |

**Deduplication**: Partition by `[cart_id, product_id]`, order by `_ingestion_timestamp DESC`, keep latest

---

## Table: `s_salesorders.customers`

**Source**: `b_salesorders.users`

### Transformations

| Column | Rule |
|--------|------|
| id | Rename to `customer_id` |
| email | LOWER and TRIM |
| username | LOWER and TRIM |
| firstName | Rename to `first_name` |
| lastName | Rename to `last_name` |
| address.city | Flatten to `city` |
| address.address | Flatten to `street` |
| address.postalCode | Flatten to `zipcode` |

**Filter**: `id IS NOT NULL`

**Deduplication**: Partition by `customer_id`, order by `_ingestion_timestamp DESC`, keep latest

---

## Quarantine

| Field | Value |
|-------|-------|
| Table | `s_salesorders.orders_quarantine` |
| Condition | `id IS NULL` |
| Reason | Missing cart ID |

## Transformation Notes

- All Spark SQL (PySQL pattern) — `spark.sql("CREATE OR REPLACE TABLE ...")`
- Cart products are exploded using `LATERAL VIEW EXPLODE` to create one row per line item
- Products are joined to orders to bring in category and price information
- Deduplication uses `ROW_NUMBER() OVER (PARTITION BY key ORDER BY timestamp DESC)` pattern
- Invalid records (null IDs) are routed to a quarantine table
