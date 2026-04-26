# High-Level Design: Sales Orders Pipeline

## Solution Overview

The Sales Orders pipeline ingests product, cart, and user data from the DummyJSON REST API and processes it through a three-layer Databricks Medallion Architecture (Bronze → Silver → Gold). Raw JSON data is landed into Bronze Delta tables with explicit schemas, cleansed and conformed in the Silver layer via Spark SQL, and aggregated into business-ready Gold tables for revenue and order analytics. The pipeline is fully idempotent, re-runnable, and includes embedded fallback data for demo reliability.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   BRONZE LAYER  │     │  SILVER LAYER   │     │   GOLD LAYER    │
│   (PySpark)     │────▶│  (Spark SQL)    │────▶│  (Spark SQL)    │
│                 │     │                 │     │                 │
│ Raw ingestion   │     │ Cleanse, dedup  │     │ Aggregations    │
│ from REST API   │     │ conform, enrich │     │ & metrics       │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

## Data Flow

```
DummyJSON API
  ├── /products ──▶ b_salesorders.products ──▶ s_salesorders.products ──┐
  ├── /carts ─────▶ b_salesorders.carts ────▶ s_salesorders.orders ────┼──▶ g_salesorders.revenue_by_category
  └── /users ─────▶ b_salesorders.users ────▶ s_salesorders.customers  │   g_salesorders.order_summary
                                              s_salesorders.orders_quarantine
```

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Platform | Databricks (Azure) |
| Storage | Delta Lake |
| Bronze Code | PySpark with explicit StructType schemas |
| Silver/Gold Code | Spark SQL (PySQL pattern) |
| Orchestration | Databricks Notebooks (`dbutils.notebook.run`) |
| Source API | DummyJSON (https://dummyjson.com) |
| Testing | pytest (local, no cluster needed) |

## Table Structure

### Bronze Schema: `b_salesorders`

| Table | Source | Write Mode | Description |
|-------|--------|-----------|-------------|
| `b_salesorders.products` | `/products` | overwrite | Raw product catalog |
| `b_salesorders.carts` | `/carts` | overwrite | Raw shopping carts with nested items |
| `b_salesorders.users` | `/users` | overwrite | Raw customer profiles |

### Silver Schema: `s_salesorders`

| Table | Source | Description |
|-------|--------|-------------|
| `s_salesorders.products` | `b_salesorders.products` | Cleansed products (trimmed, cast, deduped) |
| `s_salesorders.orders` | `b_salesorders.carts` + products join | Exploded cart items enriched with product details |
| `s_salesorders.customers` | `b_salesorders.users` | Flattened and cleansed customer profiles |
| `s_salesorders.orders_quarantine` | `b_salesorders.carts` | Invalid records (null cart_id) |

### Gold Schema: `g_salesorders`

| Table | Source | Description |
|-------|--------|-------------|
| `g_salesorders.revenue_by_category` | `s_salesorders.orders` | Revenue, items sold, orders by product category |
| `g_salesorders.order_summary` | `s_salesorders.orders` | Daily order count, customers, revenue, items |
