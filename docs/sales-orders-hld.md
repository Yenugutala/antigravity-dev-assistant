# High-Level Design: Sales Orders Medallion Pipeline

## 1. Solution Overview
The Sales Orders Medallion Pipeline is designed to ingest raw product catalog, shopping cart, and customer user profile data from the REST API source and process it through Bronze (Raw), Silver (Cleaned and Conformed), and Gold (Business Aggregations) layers. This robust architecture enables automated, daily refreshed business reporting on daily sales trends and product category revenue performance.

## 2. Architecture & Data Flow
The data flow goes sequentially through the three layers of the Medallion Architecture:

```text
                  [ Source REST API: https://dummyjson.com ]
                                     │
                                     ▼ (Python Ingestion)
               [ BRONZE SCHEMA (Raw): b_salesorders ]
               ├── products (raw JSON response)
               ├── carts (raw JSON response)
               └── users (raw JSON response)
                                     │
                                     ▼ (PySQL Cleansing, Explosion, Join)
            [ SILVER SCHEMA (Conformed): s_salesorders ]
            ├── products (deduped, conformed fields)
            ├── customers (flattened demographic details)
            ├── orders (exploded line-items joined with products)
            └── orders_quarantine (isolated records with null keys)
                                     │
                                     ▼ (PySQL Business Aggregations)
             [ GOLD SCHEMA (Analytics): g_salesorders ]
             ├── revenue_by_category (categoric revenue profiling)
             └── order_summary (daily operational metrics summary)
```

## 3. Technology Stack
- **Compute & Orchestration**: Databricks Runtime, Spark Structured Streaming (batch mode)
- **Storage Format**: Delta Lake (Delta tables)
- **Ingestion Language**: Python (PySpark API)
- **Cleansing / Transformation Language**: Spark SQL (via PySQL wrapper)

## 4. Database Schema Specifications

### 4.1 Bronze Schema (`b_salesorders`)

#### Table: `b_salesorders.products`
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

#### Table: `b_salesorders.carts`
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | INTEGER | No | Unique cart/order identifier |
| `userId` | INTEGER | No | Associated customer ID |
| `totalProducts` | INTEGER | Yes | Total products count |
| `totalQuantity` | INTEGER | Yes | Total items quantity |
| `total` | DOUBLE | Yes | Cart price total |
| `products` | ARRAY | No | Array of product structs |
| `_ingestion_timestamp` | STRING | No | Ingestion timestamp |
| `_source` | STRING | No | Ingestion source flag |
| `_batch_id` | STRING | No | Unique ingestion run ID |

#### Table: `b_salesorders.users`
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | INTEGER | No | Unique customer ID |
| `firstName` | STRING | Yes | Customer first name |
| `lastName` | STRING | Yes | Customer last name |
| `email` | STRING | No | Email address |
| `phone` | STRING | Yes | Phone number |
| `username` | STRING | No | Username |
| `address` | STRUCT | Yes | Address struct (address, city, state, postalCode) |
| `_ingestion_timestamp` | STRING | No | Ingestion timestamp |
| `_source` | STRING | No | Ingestion source flag |
| `_batch_id` | STRING | No | Unique ingestion run ID |

---

### 4.2 Silver Schema (`s_salesorders`)

#### Table: `s_salesorders.products`
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

#### Table: `s_salesorders.orders`
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

#### Table: `s_salesorders.customers`
*Deduplicated by `customer_id` keeping the latest ingestion.*
| Column | Type | Nullable | Transformation |
|--------|------|----------|----------------|
| `customer_id` | INTEGER | No | Renamed from `id` |
| `email` | STRING | No | Trimmed and lowercased |
| `username` | STRING | No | Trimmed and lowercased |
| `first_name` | STRING | Yes | Renamed from `firstName` |
| `last_name` | STRING | Yes | Renamed from `lastName` |
| `city` | STRING | Yes | Flattened address.city |
| `street` | STRING | Yes | Flattened address.address |
| `zipcode` | STRING | Yes | Flattened address.postalCode |
| `_ingestion_timestamp` | STRING | No | Metadata |

#### Table: `s_salesorders.orders_quarantine`
*Quarantine table for invalid orders where `id IS NULL`.*
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `cart_id` | INTEGER | Yes | Extracted cart ID |
| `user_id` | INTEGER | Yes | Extracted customer ID |
| `_ingestion_timestamp` | STRING | No | Metadata |
| `quarantine_reason` | STRING | No | Hardcoded description of failure |

---

### 4.3 Gold Schema (`g_salesorders`)

#### Table: `g_salesorders.revenue_by_category`
*Aggregated by `category`.*
| Column | Type | Description / Formula |
|--------|------|----------------------|
| `category` | STRING | Product category |
| `total_revenue` | DOUBLE | `ROUND(SUM(line_total), 2)` |
| `total_items_sold` | LONG | `SUM(quantity)` |
| `total_orders` | LONG | `COUNT(DISTINCT cart_id)` |
| `avg_price` | DOUBLE | `ROUND(AVG(price), 2)` |
| `unique_products` | LONG | `COUNT(DISTINCT product_id)` |

#### Table: `g_salesorders.order_summary`
*Aggregated daily by `order_date`.*
| Column | Type | Description / Formula |
|--------|------|----------------------|
| `order_date` | DATE | Operational day |
| `total_orders` | LONG | `COUNT(DISTINCT cart_id)` |
| `unique_customers` | LONG | `COUNT(DISTINCT user_id)` |
| `total_revenue` | DOUBLE | `ROUND(SUM(line_total), 2)` |
| `total_items` | LONG | `SUM(quantity)` |
| `avg_order_line_value` | DOUBLE | `ROUND(AVG(line_total), 2)` |
