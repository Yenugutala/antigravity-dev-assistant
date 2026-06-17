# High-Level Design: Sales Orders Pipeline

> Auto-generated from specs: bronze-spec.md, silver-spec.md, gold-spec.md

## Solution Overview

The Sales Orders pipeline ingests product, cart, and user data from the DummyJSON REST API, cleanses and transforms it through a Medallion Architecture (Bronze → Silver → Gold), and produces business-ready aggregation tables for revenue analysis and order tracking. The pipeline is fully idempotent, re-runnable, and designed for Databricks with Delta Lake.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   BRONZE LAYER  │     │   SILVER LAYER  │     │   GOLD LAYER    │
│   (PySpark)     │────▶│   (Spark SQL)   │────▶│   (Spark SQL)   │
│                 │     │                 │     │                 │
│ b_salesorders.  │     │ s_salesorders.  │     │ g_salesorders.  │
│  products       │     │  products       │     │  revenue_by_    │
│  carts          │     │  orders         │     │    category     │
│  users          │     │  customers      │     │  order_summary  │
│                 │     │  orders_        │     │                 │
│                 │     │    quarantine   │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
       ▲
       │
  DummyJSON API
  https://dummyjson.com
  /products, /carts, /users
```

## Data Flow

1. **Bronze**: Fetch raw JSON from DummyJSON API (`/products`, `/carts`, `/users`), apply explicit schemas, add metadata columns (`_ingestion_timestamp`, `_source`, `_batch_id`), write as Delta tables
2. **Silver**: Cleanse (trim, lower, cast), explode cart items into order line items, join with products for category enrichment, deduplicate, quarantine invalid records
3. **Gold**: Aggregate into `revenue_by_category` (revenue, items sold, orders by category) and `order_summary` (daily order metrics)

## Technology Stack

| Component | Technology |
|-----------|------------|
| Platform | Databricks (Azure) |
| Storage | Delta Lake on DBFS |
| Bronze Code | PySpark (Python) |
| Silver/Gold Code | Spark SQL via PySpark |
| Source API | DummyJSON (https://dummyjson.com) |
| Testing | pytest (local, no cluster) |

## Table Structure

### Bronze Schema: `b_salesorders`

| Table | Source | Description |
|-------|--------|-------------|
| `b_salesorders.products` | `/products` | Raw product catalog |
| `b_salesorders.carts` | `/carts` | Raw shopping carts with nested items |
| `b_salesorders.users` | `/users` | Raw user/customer data |

### Silver Schema: `s_salesorders`

| Table | Source | Description |
|-------|--------|-------------|
| `s_salesorders.products` | `b_salesorders.products` | Cleansed products with standardized names |
| `s_salesorders.orders` | `b_salesorders.carts` | Exploded cart items enriched with product info |
| `s_salesorders.customers` | `b_salesorders.users` | Cleansed customer data with flattened address |
| `s_salesorders.orders_quarantine` | `b_salesorders.carts` | Invalid records with null cart IDs |

### Gold Schema: `g_salesorders`

| Table | Source | Description |
|-------|--------|-------------|
| `g_salesorders.revenue_by_category` | `s_salesorders.orders` | Revenue metrics grouped by product category |
| `g_salesorders.order_summary` | `s_salesorders.orders` | Daily order summary with customer and revenue metrics |
