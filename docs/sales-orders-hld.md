# High-Level Design: Sales Orders Pipeline

## Solution Overview

The Sales Orders pipeline ingests e-commerce data from the DummyJSON REST API, cleanses and transforms it through a Medallion Architecture (Bronze → Silver → Gold), and produces business-ready aggregation tables for revenue analysis and operational dashboards. The pipeline is fully idempotent and includes embedded fallback data for demo reliability.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   BRONZE LAYER  │     │   SILVER LAYER  │     │   GOLD LAYER    │
│   (PySpark)     │────▶│   (Spark SQL)   │────▶│   (Spark SQL)   │
│                 │     │                 │     │                 │
│ Raw API ingest  │     │ Cleanse & join  │     │ Business aggs   │
│ + fallback data │     │ + dedup + QA    │     │ + metrics       │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

## Data Flow

```
DummyJSON API
  ├── /products ──▶ b_salesorders.products ──▶ s_salesorders.products ──┐
  ├── /carts    ──▶ b_salesorders.carts    ──▶ s_salesorders.orders   ──┼──▶ g_salesorders.revenue_by_category
  └── /users    ──▶ b_salesorders.users    ──▶ s_salesorders.customers │    g_salesorders.order_summary
                                               s_salesorders.orders_quarantine
```

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Platform | Databricks (Azure) |
| Storage | Delta Lake |
| Bronze Code | PySpark (explicit StructType schemas) |
| Silver Code | Spark SQL (PySQL pattern) |
| Gold Code | Spark SQL (PySQL pattern) |
| Orchestration | Databricks Notebooks |
| Source API | DummyJSON (https://dummyjson.com) |
| Testing | pytest (local, no cluster required) |

## Table Structure

### Bronze Schema: `b_salesorders`

| Table | Description | Source |
|-------|-------------|--------|
| `b_salesorders.products` | Raw product catalog | `/products` API |
| `b_salesorders.carts` | Raw shopping carts with nested products | `/carts` API |
| `b_salesorders.users` | Raw customer data | `/users` API |

### Silver Schema: `s_salesorders`

| Table | Description | Source |
|-------|-------------|--------|
| `s_salesorders.products` | Deduplicated, cleaned products | `b_salesorders.products` |
| `s_salesorders.orders` | Exploded cart line items enriched with product info | `b_salesorders.carts` + `s_salesorders.products` |
| `s_salesorders.customers` | Flattened, cleaned customer profiles | `b_salesorders.users` |
| `s_salesorders.orders_quarantine` | Invalid records (null cart ID) | `b_salesorders.carts` |

### Gold Schema: `g_salesorders`

| Table | Description | Source |
|-------|-------------|--------|
| `g_salesorders.revenue_by_category` | Revenue metrics by product category | `s_salesorders.orders` |
| `g_salesorders.order_summary` | Daily order summary with customer metrics | `s_salesorders.orders` |

## Pipeline Execution

1. **Bronze** — Fetch from DummyJSON API (fallback to embedded sample data), write raw Delta tables
2. **Silver** — Cleanse, explode, join, deduplicate, quarantine invalid records
3. **Gold** — Aggregate revenue by category and daily order summaries
4. **Orchestration** — `notebooks/run_pipeline.py` runs all three in sequence
