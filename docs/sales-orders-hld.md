# High-Level Design: Sales Orders Medallion Pipeline

## Solution Overview
This pipeline ingests and processes raw sales orders data from a REST API source (`https://dummyjson.com`) and conforms it to a three-tier Medallion architecture (Bronze, Silver, and Gold) on the Databricks Lakehouse Platform. By standardizing ingestion, cleaning transactional logs, extracting flat structures, and computing key business performance aggregates, this solution delivers reliable data for analytical reports, metrics, and business intelligence dashboards.

## Architecture
The pipeline transitions data through three main processing layers:
1. **Bronze (Raw Ingestion)**: Ingests raw data using PySpark from the REST API endpoints (`/products`, `/carts`, `/users`) with explicit schemas. Data is written to `b_salesorders` Delta tables with ingestion metadata (`_ingestion_timestamp`, `_source`, `_batch_id`).
2. **Silver (Conformed & Cleansed)**: Cleans and transforms the raw bronze data using Spark SQL. It explodes order lines from cart arrays, enforces data types, deduplicates records to keep the latest batch, and quarantines invalid records in `s_salesorders` Delta tables.
3. **Gold (Business Aggregations)**: Aggregates silver data into target tables (`revenue_by_category` and `order_summary`) within the `g_salesorders` schema using Spark SQL. These tables are optimized for BI consumption.

## Data Flow Diagram
```
[ REST API /products, /carts, /users ]
                  │
                  ▼  (PySpark Ingestion with Explicit Schemas & Metadata)
   [ Bronze: b_salesorders.products / carts / users ]
                  │
                  ├──────────────────────────────┐
                  ▼ (Explode & Join)             ▼ (Deduplication / Flatten)
     [ Silver: s_salesorders.orders ]     [ Silver: s_salesorders.products / customers ]
                  │                                     │
                  │ (Invalid: Null Cart ID)             │
                  ├──► [ s_salesorders.orders_quarantine ]
                  │
                  ▼ (Business Aggregations)
   [ Gold: g_salesorders.revenue_by_category / order_summary ]
```

## Technology Stack
- **Compute**: Databricks (Serverless or Shared Compute clusters)
- **Storage**: Delta Lake (DBFS storage root)
- **Ingestion Language**: PySpark (Python 3)
- **Transformation Language**: Spark SQL (via PySpark SQL API)
- **Orchestration**: Databricks Workflows / Jobs (using `dbutils.notebook.run`)

## Table Structures

### Bronze Layer (b_salesorders)
- **`b_salesorders.products`**: Raw products dataset.
- **`b_salesorders.carts`**: Raw cart and transactional product array dataset.
- **`b_salesorders.users`**: Raw customer registration dataset.

### Silver Layer (s_salesorders)
- **`s_salesorders.products`**: Cleansed products with trimmed strings and lowercased categories.
- **`s_salesorders.orders`**: Exploded line-level order items enriched with product information.
- **`s_salesorders.customers`**: Flattened user address fields and conformed email/username keys.
- **`s_salesorders.orders_quarantine`**: Quarantined records with missing/null cart IDs.

### Gold Layer (g_salesorders)
- **`g_salesorders.revenue_by_category`**: Aggregated performance metrics by product category.
- **`g_salesorders.order_summary`**: Daily operational orders and customer trends.
