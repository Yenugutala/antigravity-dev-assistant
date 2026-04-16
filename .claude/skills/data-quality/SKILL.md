---
name: data-quality
description: >
  Generate data quality validation rules, null checks, range validation,
  referential integrity checks, and anomaly detection for Delta Lake tables.
---

# Data Quality Checks

## Purpose
Read table schemas and business rules to auto-generate data quality validation notebooks.
Supports null checks, range validation, uniqueness constraints, referential integrity,
and statistical anomaly detection across Bronze, Silver, and Gold layers.

## Usage
```
/data-quality specs/dq-rules.md
```

## Capabilities
- Generate NULL and NOT NULL validation checks for all columns
- Create range validation rules (e.g., price > 0, quantity >= 1)
- Validate referential integrity across tables (foreign key checks)
- Detect statistical anomalies (z-score, IQR-based outlier detection)
- Produce DQ summary dashboard with pass/fail metrics per table
