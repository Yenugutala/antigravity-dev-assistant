# Pipeline Specification Template

> Copy this file, fill in all fields, and run: `/build-pipeline specs/<your-spec>.md`

## Pipeline Info

| Field | Value |
|-------|-------|
| Pipeline Name | `<domain>-<entity>` |
| Domain | `<business_domain>` (e.g., sales, finance, marketing) |
| Entity | `<entity_name>` (e.g., orders, invoices, campaigns) |
| Owner | `<team_or_person>` |
| Created Date | YYYY-MM-DD |
| Version | 1.0 |

## Source

| Field | Value |
|-------|-------|
| System | `<source_system>` (API, CSV, Parquet, JDBC, Salesforce, SAP) |
| Format | `<data_format>` (json, csv, parquet, delta) |
| Path or Endpoint | `<url_or_path>` (API URL or DBFS/ADLS path) |
| Auth Required | false (set true if API key or credentials needed) |
| Schema Evolution | false (set true to allow new columns automatically) |

## Volume

| Field | Value |
|-------|-------|
| Estimated Rows/Day | 0 |
| Processing Mode | batch (batch, streaming, micro_batch) |
| Refresh Frequency | daily (real-time, hourly, daily, weekly) |

---

## Bronze Layer

| Field | Value |
|-------|-------|
| Ingestion Method | batch_read (auto_loader, batch_read, streaming, api_call) |
| Partition Columns | `_ingestion_date` |

### Raw Schema

| Column | Type | Nullable |
|--------|------|----------|
| `<column_name>` | `<spark_type>` (string, integer, long, double, boolean, timestamp, date) | true |

---

## Silver Layer

### Deduplication

| Field | Value |
|-------|-------|
| Key Columns | `<primary_key>` |
| Order By | `<timestamp_column>` |
| Strategy | keep_latest (keep_latest, keep_first) |

### Cleansing Rules

| Column | Rule |
|--------|------|
| `<column>` | `<description>` (e.g., "TRIM and UPPER", "CAST to double", "COALESCE with 0") |

### Quarantine Rules

| Condition | Reason |
|-----------|--------|
| `<sql_where_clause>` (e.g., "id IS NULL") | `<why_quarantined>` |

---

## Gold Layer

### Aggregation: `<metric_name>` (e.g., daily_revenue, monthly_orders)

| Field | Value |
|-------|-------|
| Grain | `<time_grain>` (daily, weekly, monthly) |
| Group By | `<col1>`, `<col2>` |

#### Metrics

| Expression | Alias |
|------------|-------|
| `SUM(<col>)` | `<metric_alias>` |
| `COUNT(DISTINCT <col>)` | `<count_alias>` |

---

## Additional Notes

<!-- Add any extra context, business rules, or diagrams here -->
