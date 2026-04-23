# High-Level Design: Sales Orders Pipeline

## Solution Overview

The Sales Orders pipeline ingests product, cart, and user data from the DummyJSON REST API, cleanses and transforms it through a Medallion Architecture (Bronze → Silver → Gold), and produces business-ready aggregation tables for revenue analysis and order reporting. All layers are idempotent and re-runnable on Databricks.

## Architecture

```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│   BRONZE (Raw)   │────▶│  SILVER (Clean)  │────▶│  GOLD (Business) │
│    PySpark       │     │   Spark SQL      │     │   Spark SQL      │
└──────────────────┘     └──────────────────┘     └──────────────────┘
```

**Bronze** — PySpark ingestion from DummyJSON API with explicit schemas and fallback data
**Silver** — Spark SQL cleansing, deduplication (ROW_NUMBER), LATERAL VIEW EXPLODE
**Gold** — Spark SQL business aggregations for reporting

## Data Flow

```
DummyJSON API
  ├── /products ──▶ b_salesorders.products ──▶ s_salesorders.products ──┐
  ├── /carts    ──▶ b_salesorders.carts    ──▶ s_salesorders.orders    ──┼──▶ g_salesorders.revenue_by_category
  └── /users    ──▶ b_salesorders.users    ──▶ s_salesorders.customers │    g_salesorders.order_summary
                                               s_salesorders.orders_quarantine
```

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Platform | Databricks (Azure) |
| Storage | Delta Lake |
| Bronze Code | PySpark |
| Silver/Gold Code | Spark SQL (PySQL) |
| API Source | DummyJSON (`https://dummyjson.com`) |
| Orchestration | Databricks Notebooks |
| Testing | pytest (local, no cluster) |

## Table Structure

### Bronze (`b_salesorders`)

| Table | Source | Description |
|-------|--------|-------------|
| products | /products | Raw product catalog |
| carts | /carts | Raw shopping carts with nested items |
| users | /users | Raw customer profiles |

### Silver (`s_salesorders`)

| Table | Source | Description |
|-------|--------|-------------|
| products | b_salesorders.products | Cleansed, deduped products |
| orders | b_salesorders.carts | Exploded cart line items joined with products |
| customers | b_salesorders.users | Flattened, cleansed customer records |
| orders_quarantine | b_salesorders.carts | Invalid records (null cart IDs) |

### Gold (`g_salesorders`)

| Table | Source | Description |
|-------|--------|-------------|
| revenue_by_category | s_salesorders.orders | Revenue metrics by product category |
| order_summary | s_salesorders.orders | Daily order summary with customer metrics |
