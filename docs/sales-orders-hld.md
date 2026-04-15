# High-Level Design: Sales Orders Pipeline

## Solution Overview

The Sales Orders pipeline ingests product, cart, and user data from the DummyJSON REST API, cleanses and transforms it through a Medallion Architecture (Bronze → Silver → Gold), and produces business-ready analytics tables for revenue reporting and order analysis. All processing runs on Azure Databricks using Delta Lake for reliable, idempotent table writes.

---

## Architecture

```
  REST API (DummyJSON)
        │
        ▼
  ┌─────────────────────────────┐
  │  BRONZE (PySpark)           │
  │  Schema: b_salesorders      │
  │  ─────────────────────────  │
  │  products, carts, users     │
  │  Raw ingestion + metadata   │
  └─────────────┬───────────────┘
                │
                ▼
  ┌─────────────────────────────┐
  │  SILVER (Spark SQL)         │
  │  Schema: s_salesorders      │
  │  ─────────────────────────  │
  │  products   — cleansed      │
  │  orders     — exploded      │
  │  customers  — flattened     │
  │  orders_quarantine          │
  └─────────────┬───────────────┘
                │
                ▼
  ┌─────────────────────────────┐
  │  GOLD (Spark SQL)           │
  │  Schema: g_salesorders      │
  │  ─────────────────────────  │
  │  revenue_by_category        │
  │  order_summary              │
  └─────────────────────────────┘
```

---

## Data Flow

| Source | Bronze Table | Silver Table | Gold Table |
|--------|-------------|--------------|------------|
| `/products` API | `b_salesorders.products` | `s_salesorders.products` | `g_salesorders.revenue_by_category` |
| `/carts` API | `b_salesorders.carts` | `s_salesorders.orders` | `g_salesorders.order_summary` |
| `/users` API | `b_salesorders.users` | `s_salesorders.customers` | — |

---

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Compute | Azure Databricks |
| Storage | Delta Lake |
| Bronze | PySpark + REST API |
| Silver | Spark SQL (PySQL) |
| Gold | Spark SQL (PySQL) |
| Orchestration | Databricks Notebooks |
| Testing | pytest |
| Version Control | GitHub |

---

## Table Structure

### Bronze (`b_salesorders`)

| Table | Key Columns | Metadata |
|-------|------------|----------|
| `products` | id, title, price, category, rating, brand | `_ingestion_timestamp`, `_source`, `_batch_id` |
| `carts` | id, userId, total, products (array) | `_ingestion_timestamp`, `_source`, `_batch_id` |
| `users` | id, firstName, lastName, email, address (struct) | `_ingestion_timestamp`, `_source`, `_batch_id` |

### Silver (`s_salesorders`)

| Table | Key Columns | Transformations |
|-------|------------|-----------------|
| `products` | product_id, title, price, category, rating_score | Rename, TRIM, LOWER, dedup |
| `orders` | cart_id, user_id, product_id, quantity, line_total | EXPLODE, JOIN, dedup |
| `customers` | customer_id, first_name, last_name, email, city | Flatten struct, dedup |
| `orders_quarantine` | — | Null cart IDs |

### Gold (`g_salesorders`)

| Table | Group By | Key Metrics |
|-------|---------|-------------|
| `revenue_by_category` | category | total_revenue, total_items_sold, total_orders, avg_price |
| `order_summary` | order_date | total_orders, unique_customers, total_revenue, total_items |
