# High-Level Design: Sales Orders Analytics Pipeline

## Solution Overview

The Sales Orders Analytics Pipeline ingests e-commerce data (products, carts, customers) from the DummyJSON REST API, cleanses and transforms it through a Medallion Architecture (Bronze → Silver → Gold), and produces business-ready aggregations for revenue analytics and daily order tracking. All pipeline code runs on Databricks with Delta Lake storage.

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│   BRONZE LAYER  │     │   SILVER LAYER   │     │   GOLD LAYER     │
│   (PySpark)     │────▶│   (Spark SQL)    │────▶│   (Spark SQL)    │
│                 │     │                  │     │                  │
│ Raw ingestion   │     │ Cleanse, dedup,  │     │ Business         │
│ from REST API   │     │ transform, join  │     │ aggregations     │
└─────────────────┘     └──────────────────┘     └──────────────────┘
```

## Data Flow

```
DummyJSON API
  ├── /products ──▶ b_salesorders.products ──▶ s_salesorders.products ──┐
  ├── /carts    ──▶ b_salesorders.carts    ──▶ s_salesorders.orders   ──┼──▶ g_salesorders.revenue_by_category
  └── /users    ──▶ b_salesorders.users    ──▶ s_salesorders.customers│  └──▶ g_salesorders.order_summary
                                               s_salesorders.orders_quarantine
```

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Compute | Databricks (Serverless) |
| Storage | Delta Lake on DBFS |
| Bronze Code | PySpark + requests |
| Silver/Gold Code | Spark SQL (PySQL pattern) |
| Orchestration | Databricks Notebooks |
| Testing | pytest (local, no cluster) |

## Table Structure

### Bronze — `b_salesorders`

| Table | Source | Description |
|-------|--------|-------------|
| `b_salesorders.products` | `/products` API | Raw product catalog |
| `b_salesorders.carts` | `/carts` API | Raw shopping carts with nested items |
| `b_salesorders.users` | `/users` API | Raw customer profiles with nested address |

### Silver — `s_salesorders`

| Table | Source | Description |
|-------|--------|-------------|
| `s_salesorders.products` | Bronze products | Deduplicated, trimmed, normalized |
| `s_salesorders.orders` | Bronze carts | Exploded line items joined with product category |
| `s_salesorders.customers` | Bronze users | Flattened address, renamed columns |
| `s_salesorders.orders_quarantine` | Bronze carts | Records with null cart IDs |

### Gold — `g_salesorders`

| Table | Source | Description |
|-------|--------|-------------|
| `g_salesorders.revenue_by_category` | Silver orders | Revenue, items sold, orders by category |
| `g_salesorders.order_summary` | Silver orders | Daily order count, customers, revenue |

## Pipeline Properties

- **Idempotent**: All tables use CREATE OR REPLACE / overwrite mode
- **Resilient**: Embedded fallback data if API is unreachable
- **Auditable**: Metadata columns (_ingestion_timestamp, _source, _batch_id) on all bronze tables
