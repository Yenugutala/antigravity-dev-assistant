# Bronze Specification: Sales Orders — Raw Data Ingestion

> Reference: [Business Requirement](../docs/business-requirement.md)

## Pipeline Info

| Field | Value |
|-------|-------|
| Pipeline Name | salesorders-bronze |
| Domain | sales |
| Entity | orders |
| Layer | bronze |
| Owner | data-engineering-team |
| Version | 1.0 |

## Source

| Field | Value |
|-------|-------|
| System | REST API |
| Format | JSON |
| Base URL | `https://dummyjson.com` |
| Auth Required | No |

### API Endpoints

| Endpoint | Entity | Wrapper Key |
|----------|--------|-------------|
| `/products` | products | `products` |
| `/carts` | carts | `carts` |
| `/users` | users | `users` |

## Target Schema: `b_salesorders`

### Table: `b_salesorders.products`

| Column | Type | Nullable |
|--------|------|----------|
| id | INTEGER | No |
| title | STRING | No |
| price | DOUBLE | No |
| category | STRING | No |
| rating | DOUBLE | Yes |
| brand | STRING | Yes |
| description | STRING | Yes |

### Table: `b_salesorders.carts`

| Column | Type | Nullable |
|--------|------|----------|
| id | INTEGER | No |
| userId | INTEGER | No |
| totalProducts | INTEGER | Yes |
| totalQuantity | INTEGER | Yes |
| total | DOUBLE | Yes |
| products | ARRAY\<STRUCT\<id: INT, title: STRING, price: DOUBLE, quantity: INT, total: DOUBLE\>\> | No |

### Table: `b_salesorders.users`

| Column | Type | Nullable |
|--------|------|----------|
| id | INTEGER | No |
| firstName | STRING | Yes |
| lastName | STRING | Yes |
| email | STRING | No |
| phone | STRING | Yes |
| username | STRING | No |
| address | STRUCT\<address: STRING, city: STRING, state: STRING, postalCode: STRING\> | Yes |

## Ingestion Config

| Field | Value |
|-------|-------|
| Ingestion Method | api_call |
| Processing Mode | batch |
| Write Mode | overwrite |

### Metadata Columns (added automatically)

| Column | Description |
|--------|-------------|
| `_ingestion_timestamp` | Timestamp when record was ingested |
| `_source` | Source system identifier |
| `_batch_id` | Unique batch run ID |

## Ingestion Notes

- API responses are wrapped in a key (e.g., `{"products": [...]}`); extract using `wrapper_key`
- Use explicit Spark `StructType` schemas — never rely on schema inference
- Embed fallback sample data so the pipeline works even if the API is unreachable
- Preprocess raw JSON to strip unnecessary fields and enforce consistent numeric types (`float` for prices/ratings, `int` for IDs/quantities)
- Each notebook cell creates one Delta table in the `b_salesorders` schema
