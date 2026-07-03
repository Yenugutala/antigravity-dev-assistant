# High-Level Design (HLD): Sales Ingestion & Processing

This document describes the design and architecture for the Sales data pipeline running in Databricks using the Medallion Architecture.

## Architecture Diagram
```
DummyJSON API ──(Python)──> [Bronze: b_antigravity_sales] ──(PySQL)──> [Silver: s_antigravity_sales] ──(PySQL)──> [Gold: g_antigravity_sales]
```

## Schema Structures

### 1. Bronze (Raw Ingestion)
- **`b_antigravity_sales.products`**: Raw products ingestion table
- **`b_antigravity_sales.carts`**: Raw carts ingestion table
- **`b_antigravity_sales.users`**: Raw users ingestion table

### 2. Silver (Cleaned & Conformed)
- **`s_antigravity_sales.products`**: Trimmed categories and prices cast to DOUBLE
- **`s_antigravity_sales.orders`**: Exploded carts joined with products, line totals calculated
- **`s_antigravity_sales.customers`**: Flattened user addresses and trimmed details
- **`s_antigravity_sales.orders_quarantine`**: Quarantined invalid orders (missing cart ID)

### 3. Gold (Business Aggregations)
- **`g_antigravity_sales.revenue_by_category`**: Product category revenue metrics
- **`g_antigravity_sales.order_summary`**: Daily summary of order metrics
