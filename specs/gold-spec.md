# Gold Specification: Sales Orders — Business Aggregations

> Reference: [Business Requirement](../docs/business-requirement.md)

## Pipeline Info

| Field | Value |
|-------|-------|
| Pipeline Name | salesorders-gold |
| Domain | sales |
| Entity | orders |
| Layer | gold |
| Owner | data-engineering-team |
| Version | 1.0 |

## Source & Target

| Field | Value |
|-------|-------|
| Source Schema | `s_salesorders` |
| Target Schema | `g_salesorders` |

---

## Aggregation: `g_salesorders.revenue_by_category`

**Source**: `s_salesorders.orders`

**Description**: Revenue metrics grouped by product category

**Group By**: `category`

### Metrics

| Expression | Alias |
|------------|-------|
| `ROUND(SUM(line_total), 2)` | total_revenue |
| `SUM(quantity)` | total_items_sold |
| `COUNT(DISTINCT cart_id)` | total_orders |
| `ROUND(AVG(price), 2)` | avg_price |
| `COUNT(DISTINCT product_id)` | unique_products |

**Order By**: `total_revenue DESC`

---

## Aggregation: `g_salesorders.order_summary`

**Source**: `s_salesorders.orders`

**Description**: Daily order summary with customer and revenue metrics

**Group By**: `order_date`

### Metrics

| Expression | Alias |
|------------|-------|
| `COUNT(DISTINCT cart_id)` | total_orders |
| `COUNT(DISTINCT user_id)` | unique_customers |
| `ROUND(SUM(line_total), 2)` | total_revenue |
| `SUM(quantity)` | total_items |
| `ROUND(AVG(line_total), 2)` | avg_order_line_value |

**Order By**: `order_date`

---

## Aggregation Notes

- All Spark SQL (PySQL pattern) — `spark.sql("CREATE OR REPLACE TABLE ...")`
- Revenue by category enables leadership to identify top-performing product segments
- Daily order summary supports operational dashboards and trend analysis
- Both tables are fully re-created on each run (idempotent)
