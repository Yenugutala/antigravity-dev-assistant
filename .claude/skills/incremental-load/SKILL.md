---
name: incremental-load
description: >
  Generate Change Data Capture (CDC) and incremental load patterns for Delta Lake
  using MERGE, watermark columns, Auto Loader, and SCD implementations.
---

# Incremental Load

## Purpose
Convert full-refresh pipelines to incremental load patterns using Delta Lake MERGE,
watermark-based reads, and Databricks Auto Loader. Supports Slowly Changing Dimension
(SCD) Type 1 and Type 2 implementations with backfill capabilities.

## Usage
```
/incremental-load specs/incremental-spec.md
```

## Capabilities
- Generate Delta Lake MERGE (upsert) statements for incremental updates
- Configure Auto Loader ingestion with automatic schema evolution
- Implement watermark-based incremental reads from source systems
- Create SCD Type 1 (overwrite) and Type 2 (history tracking) patterns
- Produce backfill scripts for historical data loading
