---
paths:
  - "src/**/silver*.py"
---

# Silver Layer Rules

## Spark SQL Syntax
- UPPERCASE all SQL keywords (SELECT, FROM, WHERE, JOIN, etc.)
- snake_case for all column names
- NEVER combine `LATERAL VIEW EXPLODE` + `JOIN` in the same FROM clause
- Always wrap EXPLODE in a subquery first, then JOIN the result:

```sql
-- CORRECT
SELECT s.*, p.product_name
FROM (
    SELECT order_id, EXPLODE(items) AS item
    FROM b_salesorders.orders
) s
LEFT JOIN b_salesorders.products p ON s.item.product_id = p.id

-- WRONG (causes PARSE_SYNTAX_ERROR)
SELECT *
FROM b_salesorders.orders
LATERAL VIEW EXPLODE(items) AS item
LEFT JOIN b_salesorders.products p ON item.product_id = p.id
```

## Deduplication
- Use `ROW_NUMBER() OVER (PARTITION BY <key> ORDER BY _ingestion_timestamp DESC)` for dedup
- Filter with `WHERE row_num = 1`

## Schema
- Each notebook MUST start with `CREATE SCHEMA IF NOT EXISTS s_<pipeline>`
- Write to `s_<pipeline>.<table>` (e.g., `s_salesorders.orders_cleaned`)

## File Format
- Must start with `# Databricks notebook source`
- Use `# COMMAND ----------` to separate notebook cells
- Use `spark.sql(""" ... """)` for SQL execution in Python
