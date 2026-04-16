---
name: schema-evolution
description: >
  Manage Delta Lake schema changes, column additions, type migrations,
  and backward-compatible schema evolution for production pipelines.
---

# Schema Evolution

## Purpose
Analyze existing Delta Lake table schemas and generate migration scripts for
schema changes. Supports adding columns, renaming fields, type casting,
and backward-compatible evolution with zero downtime.

## Usage
```
/schema-evolution specs/schema-change.md
```

## Capabilities
- Generate ALTER TABLE migration scripts for Delta Lake
- Add new columns with default values and proper data types
- Handle type widening (INT → BIGINT, FLOAT → DOUBLE)
- Create rollback scripts for safe schema changes
- Validate schema compatibility before applying migrations
