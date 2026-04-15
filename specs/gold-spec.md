---
# GOLD SPECIFICATION: Sales Orders — Business Aggregations
# Reference: docs/business-requirement.md

pipeline_name: "salesorders-gold"
domain: "sales"
entity: "orders"
layer: "gold"
owner: "data-engineering-team"
version: "1.0"

source_schema: "s_salesorders"
target_schema: "g_salesorders"

aggregations:
  revenue_by_category:
    source: "s_salesorders.orders"
    target: "g_salesorders.revenue_by_category"
    description: "Revenue metrics grouped by product category"
    group_by:
      - "category"
    metrics:
      - expression: "ROUND(SUM(line_total), 2)"
        alias: "total_revenue"
      - expression: "SUM(quantity)"
        alias: "total_items_sold"
      - expression: "COUNT(DISTINCT cart_id)"
        alias: "total_orders"
      - expression: "ROUND(AVG(price), 2)"
        alias: "avg_price"
      - expression: "COUNT(DISTINCT product_id)"
        alias: "unique_products"
    order_by: "total_revenue DESC"

  order_summary:
    source: "s_salesorders.orders"
    target: "g_salesorders.order_summary"
    description: "Daily order summary with customer and revenue metrics"
    group_by:
      - "order_date"
    metrics:
      - expression: "COUNT(DISTINCT cart_id)"
        alias: "total_orders"
      - expression: "COUNT(DISTINCT user_id)"
        alias: "unique_customers"
      - expression: "ROUND(SUM(line_total), 2)"
        alias: "total_revenue"
      - expression: "SUM(quantity)"
        alias: "total_items"
      - expression: "ROUND(AVG(line_total), 2)"
        alias: "avg_order_line_value"
    order_by: "order_date"
---

## Aggregation Notes

- All Spark SQL (PySQL pattern) — `spark.sql("CREATE OR REPLACE TABLE ...")`
- Revenue by category enables leadership to identify top-performing product segments
- Daily order summary supports operational dashboards and trend analysis
- Both tables are fully re-created on each run (idempotent)
