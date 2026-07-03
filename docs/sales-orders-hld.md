# High-Level Design (HLD): Sales Ingestion & Processing Pipeline

This High-Level Design (HLD) document outlines the architecture, data flow, schema catalogs, and engineering standards for the Sales medallion data pipeline running in Azure Databricks.

---

## 1. Document Overview

### 1.1 Objective
The purpose of this pipeline is to ingest sales records (products, carts, and users) from the DummyJSON REST API, clean and join the data in a medallion structure, and produce high-value business metrics for operational reporting and analytical consumption.

### 1.2 System Scope
The pipeline consumes raw API endpoints, normalizes nested arrays, flattens customer demographics, isolates invalid records into quarantine, and publishes daily summaries and category performance metrics.

---

## 2. Architecture Overview

The pipeline utilizes the **Databricks Medallion Architecture**, separating data processing into three distinct layers to ensure high reliability, transaction integrity (ACID), and historical auditability.

```mermaid
graph LR
    API["REST API\n(DummyJSON)"] ── Ingest ──> B["BRONZE\n(Raw Ingestion)"]
    B ── Cleanse/Join ──> S["SILVER\n(Conformed tables)"]
    S ── Aggregations ──> G["GOLD\n(Business Metrics)"]

    subgraph Bronze Layer ["b_antigravity_sales"]
        B1["products"]
        B2["carts"]
        B3["users"]
    end

    subgraph Silver Layer ["s_antigravity_sales"]
        S1["products"]
        S2["orders"]
        S3["customers"]
        S4["orders_quarantine"]
    end

    subgraph Gold Layer ["g_antigravity_sales"]
        G1["revenue_by_category"]
        G2["order_summary"]
    end

    style API fill:#FF9F43,stroke:#E08930,color:#fff
    style B fill:#74B9FF,stroke:#5A9FE0,color:#fff
    style S fill:#A29BFE,stroke:#8A83E0,color:#fff
    style G fill:#FDCB6E,stroke:#E0B35E,color:#333
```

*(Note: If the diagram above is not rendering, install the **Markdown Preview Mermaid Support** extension in your IDE, or view the text-based architecture below)*

```text
                  [ Source REST API: https://dummyjson.com ]
                                     │
                                     ▼ (Python Ingestion)
               [ BRONZE SCHEMA (Raw): b_antigravity_sales ]
               ├── products (raw JSON response)
               ├── carts (raw JSON response)
               └── users (raw JSON response)
                                     │
                                     ▼ (PySQL Cleansing, Explosion, Join)
            [ SILVER SCHEMA (Conformed): s_antigravity_sales ]
            ├── products (deduped, conformed fields)
            ├── customers (flattened demographic details)
            ├── orders (exploded line-items joined with products)
            └── orders_quarantine (isolated records with null keys)
                                     │
                                     ▼ (PySQL Business Aggregations)
             [ GOLD SCHEMA (Analytics): g_antigravity_sales ]
             ├── revenue_by_category (categoric revenue profiling)
             └── order_summary (daily operational metrics summary)
```

---

## 3. Pipeline Ingestion & Processing Stages

### 3.1 Bronze Layer (Raw Ingestion)
* **Technology**: PySpark (Python notebooks).
* **Ingestion Method**: REST API endpoints pulled via the Python `requests` library.
* **Schema Integrity**: Explicit Spark `StructType` schemas are enforced on DataFrame creation to block malformed payloads and prevent schema inference latency.
* **Resiliency (API Fallback)**: High-availability fallback datasets are embedded inside the ingestion scripts. If the source REST API is unreachable, the pipeline automatically loads fallback sample data to guarantee uninterrupted execution.
* **Audit Metadata**: Every table includes metadata audit columns:
  * `_ingestion_timestamp`: ISO 8601 UTC timestamp of ingestion.
  * `_source`: Data source flag (`dummyjson_api` or `fallback_sample`).
  * `_batch_id`: A unique UUID4 string generated for each run.

### 3.2 Silver Layer (Cleansing & Conformance)
* **Technology**: PySQL (Spark SQL queries executed inside Python cells).
* **Conformity Standards**:
  * Strings are trimmed of leading/trailing whitespace.
  * Product category strings are conformed to lowercase (e.g. `"Beauty "` $\rightarrow$ `"beauty"`).
  * Numeric data types are explicitly cast to `DOUBLE` (prices, ratings) and `INTEGER` (IDs, quantities).
* **Array Explosion Safety**: Nested product arrays inside the carts table are exploded. To prevent parser syntax errors (`PARSE_SYNTAX_ERROR`), the `EXPLODE` statement is wrapped inside a CTE subquery before performing any `LEFT JOIN` on dimensions.
* **Deduplication**: Rows are deduplicated using window partitions `ROW_NUMBER() OVER (PARTITION BY <primary_key> ORDER BY _ingestion_timestamp DESC)` to ensure only the latest batch updates are loaded.
* **Quarantine Routing**: Records failing critical integrity checks (specifically where the primary key `id` is null) are routed to a dedicated `orders_quarantine` table with an explicit string field (`quarantine_reason`) describing the failure.

### 3.3 Gold Layer (Business Aggregations)
* **Technology**: PySQL (Spark SQL).
* **Objective**: Aggregate the cleaned transaction ledger into metrics suitable for dashboards (Power BI, Tableau) and analytical reporting.
* **Key Aggregations**:
  * **Revenue by Category**: Aggregates total sales revenue, unique products sold, and distinct order counts grouped by category.
  * **Daily Order Summary**: Aggregates daily customer counts, order counts, items sold, and average ticket values.
* **Idempotency**: Tables are re-created dynamically using `CREATE OR REPLACE TABLE` statements on each run to guarantee idempotence.

---

## 4. Database Schema Specifications

### 4.1 Bronze Schema (`b_antigravity_sales`)

#### Table: `b_antigravity_sales.products`
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | INTEGER | No | Unique product ID (Primary Key) |
| `title` | STRING | No | Name of the product |
| `price` | DOUBLE | No | Unit price |
| `category` | STRING | No | Product segment category |
| `rating` | DOUBLE | Yes | Average consumer rating |
| `brand` | STRING | Yes | Brand identifier |
| `description` | STRING | Yes | Text description |
| `_ingestion_timestamp` | STRING | No | Ingestion timestamp |
| `_source` | STRING | No | Ingestion source flag |
| `_batch_id` | STRING | No | Unique ingestion run ID |

---

### 4.2 Silver Schema (`s_antigravity_sales`)

#### Table: `s_antigravity_sales.products`
*Deduplicated by `product_id` keeping the latest ingestion.*
| Column | Type | Nullable | Transformation |
|--------|------|----------|----------------|
| `product_id` | INTEGER | No | Renamed from `id` |
| `title` | STRING | No | Trimmed whitespace |
| `price` | DOUBLE | No | Cast to Double |
| `category` | STRING | No | Trimmed and lowercased |
| `rating_score` | DOUBLE | Yes | Renamed from `rating` |
| `brand` | STRING | Yes | Standardized string |
| `description` | STRING | Yes | Standardized string |
| `_ingestion_timestamp` | STRING | No | Metadata |

#### Table: `s_antigravity_sales.orders`
*Exploded transaction line items.*
| Column | Type | Nullable | Description / Formula |
|--------|------|----------|----------------------|
| `cart_id` | INTEGER | No | Unique cart/order identifier |
| `user_id` | INTEGER | No | Associated customer ID |
| `order_date` | DATE | No | Calculated processing date (`CURRENT_DATE()`) |
| `product_id` | INTEGER | No | Enriched product ID |
| `product_title` | STRING | No | Enriched product title |
| `category` | STRING | Yes | Enriched product category |
| `price` | DOUBLE | No | Unit price of the item |
| `quantity` | INTEGER | No | Total quantity ordered |
| `line_total` | DOUBLE | No | Total line cost (`ROUND(price * quantity, 2)`) |
| `_ingestion_timestamp` | STRING | No | Metadata |

---

### 4.3 Gold Schema (`g_antigravity_sales`)

#### Table: `g_antigravity_sales.revenue_by_category`
*Aggregated by `category`.*
| Column | Type | Description / Formula |
|--------|------|----------------------|
| `category` | STRING | Product category |
| `total_revenue` | DOUBLE | `ROUND(SUM(line_total), 2)` |
| `total_items_sold` | LONG | `SUM(quantity)` |
| `total_orders` | LONG | `COUNT(DISTINCT cart_id)` |
| `avg_price` | DOUBLE | `ROUND(AVG(price), 2)` |
| `unique_products` | LONG | `COUNT(DISTINCT product_id)` |

#### Table: `g_antigravity_sales.order_summary`
*Aggregated daily by `order_date`.*
| Column | Type | Description / Formula |
|--------|------|----------------------|
| `order_date` | DATE | Operational day |
| `total_orders` | LONG | `COUNT(DISTINCT cart_id)` |
| `unique_customers` | LONG | `COUNT(DISTINCT user_id)` |
| `total_revenue` | DOUBLE | `ROUND(SUM(line_total), 2)` |
| `total_items` | LONG | `SUM(quantity)` |
| `avg_order_line_value` | DOUBLE | `ROUND(AVG(line_total), 2)` |
