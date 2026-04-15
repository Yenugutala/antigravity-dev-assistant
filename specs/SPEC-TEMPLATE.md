---
# PIPELINE SPECIFICATION TEMPLATE
# Copy this file and fill in all fields for your pipeline.
# Then run: /build-pipeline specs/<your-spec>.md

pipeline_name: "<domain>-<entity>"
domain: "<business_domain>"           # e.g., sales, finance, marketing
entity: "<entity_name>"               # e.g., orders, invoices, campaigns
owner: "<team_or_person>"
created_date: "YYYY-MM-DD"
version: "1.0"

source:
  system: "<source_system>"           # API, CSV, Parquet, JDBC, Salesforce, SAP
  format: "<data_format>"             # json, csv, parquet, delta
  path_or_endpoint: "<url_or_path>"   # API URL or DBFS/ADLS path
  auth_required: false                # true if API key or credentials needed
  schema_evolution: false             # true to allow new columns automatically

volume:
  estimated_rows_per_day: 0
  processing_mode: "batch"            # batch, streaming, micro_batch
  refresh_frequency: "daily"          # real-time, hourly, daily, weekly

bronze:
  ingestion_method: "batch_read"      # auto_loader, batch_read, streaming, api_call
  partition_columns: ["_ingestion_date"]
  raw_schema:                         # Define expected columns from source
    - name: "<column_name>"
      type: "<spark_type>"            # string, integer, long, double, boolean, timestamp, date
      nullable: true

silver:
  deduplication:
    key_columns: ["<primary_key>"]
    order_by: "<timestamp_column>"
    strategy: "keep_latest"           # keep_latest, keep_first
  cleansing_rules:
    - column: "<column>"
      rule: "<description>"           # e.g., "TRIM and UPPER", "CAST to double", "COALESCE with 0"
  quarantine_rules:
    - condition: "<sql_where_clause>" # e.g., "id IS NULL"
      reason: "<why_quarantined>"

gold:
  aggregations:
    - name: "<metric_name>"           # e.g., daily_revenue, monthly_orders
      grain: "<time_grain>"           # daily, weekly, monthly
      group_by: ["<col1>", "<col2>"]
      metrics:
        - expression: "SUM(<col>)"
          alias: "<metric_alias>"
        - expression: "COUNT(DISTINCT <col>)"
          alias: "<count_alias>"

data_quality:
  bronze:
    - check: "row_count > 0"
  silver:
    - check: "unique(<key_col>)"
    - check: "null_rate(<required_col>) < 0.01"
  gold:
    - check: "<metric> >= 0"

consumers:
  - type: "databricks_sql"            # power_bi, synapse, ml_model, api, databricks_sql
    name: "<consumer_name>"
---

## Additional Notes
<!-- Add any extra context, business rules, or diagrams here -->
