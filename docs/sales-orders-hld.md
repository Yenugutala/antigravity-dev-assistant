# High-Level Design: Sales Orders Pipeline

## Solution Overview

This pipeline ingests sales order data from the DummyJSON REST API and processes it through a Medallion Architecture (Bronze → Silver → Gold) on Databricks. The Bronze layer captures raw data, Silver cleanses and transforms it, and Gold produces business-ready aggregations for revenue analysis and order reporting.

## Architecture

```
REST API (DummyJSON)
        │
        ▼
┌─────────────────┐
│   BRONZE LAYER  │  PySpark — Raw ingestion
│  b_salesorders   │  Tables: products, carts, users
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   SILVER LAYER  │  Spark SQL — Cleanse & transform
│  s_salesorders   │  Tables: products, orders, customers, orders_quarantine
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    GOLD LAYER   │  Spark SQL — Business aggregations
│  g_salesorders   │  Tables: revenue_by_category, order_summary
└─────────────────┘
```

## Data Flow

1. **Bronze**: Fetch products, carts, users from `https://dummyjson.com` API. Fallback to embedded sample data if API is down. Write raw Delta tables with metadata columns.
2. **Silver**: Cleanse products (TRIM, LOWER, CAST). Explode cart items into order line items and join with products for category enrichment. Flatten user addresses into customers. Quarantine invalid records.
3. **Gold**: Aggregate orders by category for revenue metrics. Summarize orders by date for operational dashboards.

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Platform | Azure Databricks |
| Storage | Delta Lake |
| Bronze Code | PySpark |
| Silver/Gold Code | Spark SQL (PySQL pattern) |
| Testing | pytest |
| Version Control | GitHub |

## Table Structure

### Bronze — `b_salesorders`
| Table | Description |
|-------|------------|
| `b_salesorders.products` | Raw product catalog |
| `b_salesorders.carts` | Raw shopping carts with nested product arrays |
| `b_salesorders.users` | Raw user profiles with nested address |

### Silver — `s_salesorders`
| Table | Description |
|-------|------------|
| `s_salesorders.products` | Cleansed, deduplicated products |
| `s_salesorders.orders` | Exploded order line items enriched with category |
| `s_salesorders.customers` | Flattened customer profiles |
| `s_salesorders.orders_quarantine` | Invalid records (null cart IDs) |

### Gold — `g_salesorders`
| Table | Description |
|-------|------------|
| `g_salesorders.revenue_by_category` | Revenue, items sold, orders by category |
| `g_salesorders.order_summary` | Daily order counts, revenue, customer metrics |
