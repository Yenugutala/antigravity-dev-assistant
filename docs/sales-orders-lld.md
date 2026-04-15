# Low-Level Design: Sales Orders Pipeline

## 1. Bronze Layer — Schema Definitions

### `b_salesorders.products`
| Column | Type | Nullable | Source |
|---|---|---|---|
| id | INT | No | API: /products |
| title | STRING | No | API: /products |
| price | DOUBLE | No | API: /products |
| category | STRING | No | API: /products |
| rating | DOUBLE | Yes | API: /products |
| brand | STRING | Yes | API: /products |
| description | STRING | Yes | API: /products |
| _ingestion_timestamp | TIMESTAMP | No | System generated |
| _source | STRING | No | Literal "dummyjson" |
| _batch_id | STRING | No | UUID per run |

### `b_salesorders.carts`
| Column | Type | Nullable | Source |
|---|---|---|---|
| id | INT | No | API: /carts |
| userId | INT | No | API: /carts |
| totalProducts | INT | Yes | API: /carts |
| totalQuantity | INT | Yes | API: /carts |
| total | DOUBLE | Yes | API: /carts |
| products | ARRAY<STRUCT<id: INT, title: STRING, price: DOUBLE, quantity: INT, total: DOUBLE>> | No | API: /carts |
| _ingestion_timestamp | TIMESTAMP | No | System generated |
| _source | STRING | No | Literal "dummyjson" |
| _batch_id | STRING | No | UUID per run |

### `b_salesorders.users`
| Column | Type | Nullable | Source |
|---|---|---|---|
| id | INT | No | API: /users |
| firstName | STRING | Yes | API: /users |
| lastName | STRING | Yes | API: /users |
| email | STRING | No | API: /users |
| phone | STRING | Yes | API: /users |
| username | STRING | No | API: /users |
| address | STRUCT<address: STRING, city: STRING, state: STRING, postalCode: STRING> | Yes | API: /users |
| _ingestion_timestamp | TIMESTAMP | No | System generated |
| _source | STRING | No | Literal "dummyjson" |
| _batch_id | STRING | No | UUID per run |

---

## 2. Silver Layer — Transformation Logic

### `s_salesorders.products` (from b_salesorders.products)
```sql
CREATE OR REPLACE TABLE s_salesorders.products AS
SELECT
    id AS product_id,
    TRIM(title) AS title,
    CAST(price AS DOUBLE) AS price,
    TRIM(LOWER(category)) AS category,
    description,
    CAST(rating AS DOUBLE) AS rating_score,
    _ingestion_timestamp
FROM b_salesorders.products
WHERE id IS NOT NULL AND price >= 0
```

### `s_salesorders.orders` (from b_salesorders.carts + s_salesorders.products)
```sql
CREATE OR REPLACE TABLE s_salesorders.orders AS
WITH exploded_carts AS (
    SELECT
        c.id AS cart_id,
        c.userId AS user_id,
        item.id AS product_id,
        item.quantity AS quantity,
        item.price AS item_price,
        item.total AS item_total,
        c._ingestion_timestamp
    FROM b_salesorders.carts c
    LATERAL VIEW EXPLODE(c.products) AS item
    WHERE c.id IS NOT NULL
),
enriched AS (
    SELECT
        ec.cart_id,
        ec.user_id,
        CURRENT_DATE() AS order_date,
        ec.product_id,
        p.title AS product_title,
        p.category,
        COALESCE(ec.item_price, p.price) AS price,
        ec.quantity,
        ROUND(COALESCE(ec.item_total, ec.item_price * ec.quantity, p.price * ec.quantity), 2) AS line_total,
        ec._ingestion_timestamp
    FROM exploded_carts ec
    LEFT JOIN s_salesorders.products p ON ec.product_id = p.product_id
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY cart_id, product_id
            ORDER BY _ingestion_timestamp DESC
        ) AS rn
    FROM enriched
)
SELECT
    cart_id, user_id, order_date, product_id, product_title,
    category, price, quantity, line_total, _ingestion_timestamp
FROM ranked
WHERE rn = 1
```

### `s_salesorders.customers` (from b_salesorders.users)
```sql
CREATE OR REPLACE TABLE s_salesorders.customers AS
WITH ranked AS (
    SELECT
        id AS customer_id,
        LOWER(TRIM(email)) AS email,
        LOWER(TRIM(username)) AS username,
        firstName AS first_name,
        lastName AS last_name,
        phone,
        address.city AS city,
        address.address AS street,
        address.postalCode AS zipcode,
        _ingestion_timestamp,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingestion_timestamp DESC) AS rn
    FROM b_salesorders.users
    WHERE id IS NOT NULL
)
SELECT
    customer_id, email, username, first_name, last_name,
    phone, city, street, zipcode, _ingestion_timestamp
FROM ranked
WHERE rn = 1
```

### `s_salesorders.orders_quarantine`
```sql
CREATE OR REPLACE TABLE s_salesorders.orders_quarantine AS
SELECT *, 'Missing cart ID' AS quarantine_reason
FROM b_salesorders.carts WHERE id IS NULL
```

---

## 3. Gold Layer — Aggregation Logic

### `g_salesorders.revenue_by_category`
```sql
CREATE OR REPLACE TABLE g_salesorders.revenue_by_category AS
SELECT
    category,
    ROUND(SUM(line_total), 2) AS total_revenue,
    SUM(quantity) AS total_items_sold,
    COUNT(DISTINCT cart_id) AS total_orders,
    ROUND(AVG(price), 2) AS avg_price,
    COUNT(DISTINCT product_id) AS unique_products
FROM s_salesorders.orders
GROUP BY category
ORDER BY total_revenue DESC
```

### `g_salesorders.order_summary`
```sql
CREATE OR REPLACE TABLE g_salesorders.order_summary AS
SELECT
    order_date,
    COUNT(DISTINCT cart_id) AS total_orders,
    COUNT(DISTINCT user_id) AS unique_customers,
    ROUND(SUM(line_total), 2) AS total_revenue,
    SUM(quantity) AS total_items,
    ROUND(AVG(line_total), 2) AS avg_order_line_value
FROM s_salesorders.orders
GROUP BY order_date
ORDER BY order_date
```

---

## 4. Error Handling

| Error | Action |
|---|---|
| API returns HTTP error | Fall back to embedded sample data |
| API returns empty data | Fall back to embedded sample data |
| Null primary key | Move to quarantine table, continue pipeline |
| Schema type mismatch | Prevented by explicit StructType schemas |
| Delta table write fails | Retry once, then fail pipeline |

---

## 5. Configuration Parameters

```yaml
# From config.yml
schemas:
  bronze: "b_salesorders"
  silver: "s_salesorders"
  gold: "g_salesorders"
source_api: "https://dummyjson.com"
tables:
  bronze: b_salesorders.products, b_salesorders.carts, b_salesorders.users
  silver: s_salesorders.products, s_salesorders.orders, s_salesorders.customers
  gold: g_salesorders.revenue_by_category, g_salesorders.order_summary
```
