# High-Level Design: Sales Orders Pipeline

## 1. Solution Overview
This pipeline ingests product catalog, shopping cart (orders), and customer data from the DummyJSON REST API, processes it through a Databricks Medallion Architecture (Bronze → Silver → Gold), and produces business-ready analytics tables for revenue analysis by product category and daily order summaries.

## 2. Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│   DummyJSON     │     │     BRONZE       │     │     SILVER       │     │      GOLD        │
│   REST API      │────▶│   (PySpark)      │────▶│   (Spark SQL)    │────▶│   (Spark SQL)    │
│                 │     │                  │     │                  │     │                  │
│ /products       │     │ b_salesorders.   │     │ s_salesorders.   │     │ g_salesorders.   │
│ /carts          │     │   products       │     │   products       │     │   revenue_by_    │
│ /users          │     │   carts          │     │   orders         │     │     category     │
│                 │     │   users          │     │   customers      │     │   order_summary  │
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
- **Process**: HTTP GET → JSON → Clean & Type-Cast → Explicit Schema → Spark DataFrame → Delta
- **Output**: 3 Delta tables in `b_salesorders` schema
- **Metadata**: `_ingestion_timestamp`, `_source`, `_batch_id`
- **Fallback**: Embedded sample data if API is unreachable

### Silver Layer (Cleansing & Conformance)
- **Input**: 3 Bronze Delta tables from `b_salesorders`
- **Process**:
  - Cleanse products: TRIM, LOWER, CAST
  - Explode cart products array into order line items (LATERAL VIEW EXPLODE)
  - Join orders with products for category/price enrichment
  - Deduplicate by primary key (ROW_NUMBER, keep latest)
  - Quarantine invalid records (null IDs)
- **Output**: 3 Silver tables + 1 quarantine table in `s_salesorders` schema

### Gold Layer (Business Aggregations)
- **Input**: Silver tables from `s_salesorders`
- **Process**:
  - Revenue by category: SUM(line_total), COUNT orders, AVG price
  - Order summary: daily orders, unique customers, total revenue
- **Output**: 2 Gold tables in `g_salesorders` schema

## 5. Table Structure

| Layer | Schema | Table Name | Description |
|---|---|---|---|
| Bronze | `b_salesorders` | `products` | Raw product catalog from API |
| Bronze | `b_salesorders` | `carts` | Raw shopping cart/order data |
| Bronze | `b_salesorders` | `users` | Raw customer data |
| Silver | `s_salesorders` | `products` | Cleansed products with rating |
| Silver | `s_salesorders` | `orders` | Flattened order line items with product details |
| Silver | `s_salesorders` | `customers` | Cleansed customers with flattened address |
| Silver | `s_salesorders` | `orders_quarantine` | Invalid records rejected during cleansing |
| Gold | `g_salesorders` | `revenue_by_category` | Revenue, order count, avg price per category |
| Gold | `g_salesorders` | `order_summary` | Daily order count, unique customers, revenue |

## 6. Monitoring

- Pipeline execution time logged per stage (Bronze, Silver, Gold)
- Row counts logged per table after each write
- Quarantine table row count checked for anomalies
- All outputs displayed in notebook for visual verification
