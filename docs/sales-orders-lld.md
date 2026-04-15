# Low-Level Design: Sales Orders Pipeline

## 1. Bronze Layer — Schema Definitions

### `default.products_bronze`
| Column | Type | Nullable | Source |
|---|---|---|---|
| id | INT | No | API: /products |
| title | STRING | No | API: /products |
| price | DOUBLE | No | API: /products |
| description | STRING | Yes | API: /products |
| category | STRING | No | API: /products |
| image | STRING | Yes | API: /products |
| rating | STRUCT<rate: DOUBLE, count: INT> | Yes | API: /products |
| _ingestion_timestamp | TIMESTAMP | No | System generated |
| _source | STRING | No | Literal "fakestoreapi" |
| _batch_id | STRING | No | UUID per run |

### `default.carts_bronze`
| Column | Type | Nullable | Source |
|---|---|---|---|
| id | INT | No | API: /carts |
| userId | INT | No | API: /carts |
| date | STRING | No | API: /carts |
| products | ARRAY<STRUCT<productId: INT, quantity: INT>> | No | API: /carts |
| _ingestion_timestamp | TIMESTAMP | No | System generated |
| _source | STRING | No | Literal "fakestoreapi" |
| _batch_id | STRING | No | UUID per run |

### `default.users_bronze`
| Column | Type | Nullable | Source |
|---|---|---|---|
| id | INT | No | API: /users |
| email | STRING | No | API: /users |
| username | STRING | No | API: /users |
| name | STRUCT<firstname: STRING, lastname: STRING> | Yes | API: /users |
| phone | STRING | Yes | API: /users |
| address | STRUCT<city: STRING, street: STRING, number: INT, zipcode: STRING, geolocation: STRUCT<lat: STRING, long: STRING>> | Yes | API: /users |
| _ingestion_timestamp | TIMESTAMP | No | System generated |
| _source | STRING | No | Literal "fakestoreapi" |
| _batch_id | STRING | No | UUID per run |

---

## 2. Silver Layer — Transformation Logic

### `default.products_silver` (from products_bronze)
```sql
CREATE OR REPLACE TABLE default.products_silver AS
SELECT
    id AS product_id,
    TRIM(title) AS title,
    CAST(price AS DOUBLE) AS price,
    TRIM(LOWER(category)) AS category,
    description,
    image,
    rating.rate AS rating_score,
    rating.count AS rating_count,
    _ingestion_timestamp
FROM default.products_bronze
WHERE id IS NOT NULL AND price >= 0
```

### `default.orders_silver` (from carts_bronze + products_silver)
```sql
-- Step 1: Explode cart products array
CREATE OR REPLACE TABLE default.orders_silver AS
WITH exploded_carts AS (
    SELECT
        c.id AS cart_id,
        c.userId AS user_id,
        CAST(c.date AS DATE) AS order_date,
        item.productId AS product_id,
        item.quantity AS quantity,
        c._ingestion_timestamp
    FROM default.carts_bronze c
    LATERAL VIEW EXPLODE(c.products) AS item
    WHERE c.id IS NOT NULL
),
-- Step 2: Join with products to get price and category
enriched AS (
    SELECT
        ec.cart_id,
        ec.user_id,
        ec.order_date,
        ec.product_id,
        p.title AS product_title,
        p.category,
        p.price,
        ec.quantity,
        ROUND(p.price * ec.quantity, 2) AS line_total,
        ec._ingestion_timestamp
    FROM exploded_carts ec
    JOIN default.products_silver p ON ec.product_id = p.product_id
),
-- Step 3: Deduplicate
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY cart_id, product_id
            ORDER BY _ingestion_timestamp DESC
        ) AS rn
    FROM enriched
)
SELECT * EXCEPT(rn) FROM ranked WHERE rn = 1
```

### `default.customers_silver` (from users_bronze)
```sql
CREATE OR REPLACE TABLE default.customers_silver AS
WITH ranked AS (
    SELECT
        id AS customer_id,
        LOWER(TRIM(email)) AS email,
        LOWER(TRIM(username)) AS username,
        name.firstname AS first_name,
        name.lastname AS last_name,
        phone,
        address.city AS city,
        address.street AS street,
        address.zipcode AS zipcode,
        _ingestion_timestamp,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingestion_timestamp DESC) AS rn
    FROM default.users_bronze
    WHERE id IS NOT NULL
)
SELECT * EXCEPT(rn) FROM ranked WHERE rn = 1
```

### `default.orders_quarantine`
```sql
-- Records rejected during Silver processing
CREATE OR REPLACE TABLE default.orders_quarantine AS
SELECT *, 'Missing primary key' AS quarantine_reason
FROM default.carts_bronze WHERE id IS NULL
UNION ALL
SELECT *, 'Missing user ID' AS quarantine_reason
FROM default.carts_bronze WHERE userId IS NULL
```

---

## 3. Gold Layer — Aggregation Logic

### `default.revenue_by_category`
```sql
CREATE OR REPLACE TABLE default.revenue_by_category AS
SELECT
    category,
    ROUND(SUM(line_total), 2) AS total_revenue,
    SUM(quantity) AS total_items_sold,
    COUNT(DISTINCT cart_id) AS total_orders,
    ROUND(AVG(price), 2) AS avg_price,
    COUNT(DISTINCT product_id) AS unique_products
FROM default.orders_silver
GROUP BY category
ORDER BY total_revenue DESC
```

### `default.order_summary`
```sql
CREATE OR REPLACE TABLE default.order_summary AS
SELECT
    order_date,
    COUNT(DISTINCT cart_id) AS total_orders,
    COUNT(DISTINCT user_id) AS unique_customers,
    ROUND(SUM(line_total), 2) AS total_revenue,
    SUM(quantity) AS total_items,
    ROUND(AVG(line_total), 2) AS avg_order_line_value
FROM default.orders_silver
GROUP BY order_date
ORDER BY order_date
```

---

## 4. Data Quality Checks

| Layer | Table | Check | SQL |
|---|---|---|---|
| Bronze | products_bronze | Row count > 0 | `SELECT COUNT(*) FROM default.products_bronze` |
| Bronze | products_bronze | No null IDs | `SELECT COUNT(*) FROM default.products_bronze WHERE id IS NULL` |
| Silver | products_silver | Unique product_id | `SELECT product_id, COUNT(*) FROM default.products_silver GROUP BY product_id HAVING COUNT(*) > 1` |
| Silver | orders_silver | No negative prices | `SELECT COUNT(*) FROM default.orders_silver WHERE price < 0` |
| Gold | revenue_by_category | Revenue >= 0 | `SELECT * FROM default.revenue_by_category WHERE total_revenue < 0` |
| Gold | order_summary | Orders > 0 | `SELECT * FROM default.order_summary WHERE total_orders = 0` |

---

## 5. Error Handling

| Error | Action |
|---|---|
| API returns HTTP error | Raise exception, log error, fail pipeline |
| API returns empty data | Raise exception with message "No data returned" |
| Null primary key | Move to quarantine table, continue pipeline |
| Negative price | Move to quarantine table, continue pipeline |
| Schema mismatch | Log warning, attempt to proceed with available columns |
| Delta table write fails | Retry once, then fail pipeline |

---

## 6. Configuration Parameters

```yaml
# From config.yml
database: "default"
source_api: "https://fakestoreapi.com"
tables:
  bronze: products_bronze, carts_bronze, users_bronze
  silver: products_silver, orders_silver, customers_silver
  gold: revenue_by_category, order_summary
```
