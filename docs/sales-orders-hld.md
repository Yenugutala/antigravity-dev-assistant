# High-Level Design: Sales Orders Pipeline

## 1. Solution Overview
This pipeline ingests product catalog, shopping cart (orders), and customer data from the FakeStore REST API, processes it through a Databricks Medallion Architecture (Bronze → Silver → Gold), and produces business-ready analytics tables for revenue analysis by product category and daily order summaries.

## 2. Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│   FakeStore     │     │     BRONZE       │     │     SILVER       │     │      GOLD        │
│   REST API      │────▶│   (PySpark)      │────▶│   (Spark SQL)    │────▶│   (Spark SQL)    │
│                 │     │                  │     │                  │     │                  │
│ /products       │     │ products_bronze  │     │ products_silver  │     │ revenue_by_      │
│ /carts          │     │ carts_bronze     │     │ orders_silver    │     │   category       │
│ /users          │     │ users_bronze     │     │ customers_silver │     │ order_summary    │
└─────────────────┘     └──────────────────┘     └──────────────────┘     └──────────────────┘
                          Raw JSON → Delta         Cleansed, Flattened      Aggregated Metrics
                          + metadata columns       + deduplicated           + business KPIs
```

## 3. Technology Stack

| Component | Technology |
|---|---|
| Compute | Azure Databricks (14-day trial) |
| Storage | Delta Lake on DBFS |
| Bronze Layer | PySpark + requests library |
| Silver Layer | Spark SQL via spark.sql() |
| Gold Layer | Spark SQL via spark.sql() |
| Orchestration | Databricks Notebook chaining (%run) |
| Version Control | GitHub + Databricks Repos |
| CI/CD | GitHub Actions (lint + test) |

## 4. Data Flow

### Bronze Layer (Raw Ingestion)
- **Input**: 3 REST API endpoints (products, carts, users)
- **Process**: HTTP GET → JSON → Spark DataFrame → Add metadata columns
- **Output**: 3 Delta tables (`products_bronze`, `carts_bronze`, `users_bronze`)
- **Metadata**: `_ingestion_timestamp`, `_source`, `_batch_id`

### Silver Layer (Cleansing & Conformance)
- **Input**: 3 Bronze Delta tables
- **Process**:
  - Flatten nested structs (rating, name, address)
  - Explode cart products array into order line items
  - Join orders with products to get prices/categories
  - Deduplicate by primary key (keep latest)
  - Quarantine invalid records (null IDs, negative prices)
- **Output**: 3 Silver tables + 1 quarantine table

### Gold Layer (Business Aggregations)
- **Input**: Silver tables
- **Process**:
  - Revenue by category: SUM(price * quantity), COUNT orders, AVG price
  - Order summary: daily orders, unique customers, total revenue
- **Output**: 2 Gold tables (`revenue_by_category`, `order_summary`)

## 5. Table Structure

| Layer | Table Name | Description |
|---|---|---|
| Bronze | `default.products_bronze` | Raw product catalog from API |
| Bronze | `default.carts_bronze` | Raw shopping cart/order data |
| Bronze | `default.users_bronze` | Raw customer data |
| Silver | `default.products_silver` | Cleansed products with flattened rating |
| Silver | `default.orders_silver` | Flattened order line items with product details |
| Silver | `default.customers_silver` | Cleansed customers with flattened name/address |
| Silver | `default.orders_quarantine` | Invalid records rejected during cleansing |
| Gold | `default.revenue_by_category` | Revenue, order count, avg price per category |
| Gold | `default.order_summary` | Daily order count, unique customers, revenue |

## 6. Data Quality Strategy

| Layer | Check | Action |
|---|---|---|
| Bronze | Row count > 0 | Fail pipeline if API returns empty |
| Bronze | No null IDs | Log warning |
| Silver | Unique primary keys | Deduplicate (keep latest) |
| Silver | No negative prices | Quarantine record |
| Silver | No null required fields | Quarantine record |
| Gold | Revenue >= 0 | Log warning |
| Gold | Order count > 0 | Fail if no data |

## 7. Monitoring

- Pipeline execution time logged per stage (Bronze, Silver, Gold)
- Row counts logged per table after each write
- Quarantine table row count checked — alert if > 5% of input
- All outputs displayed in notebook for visual verification
