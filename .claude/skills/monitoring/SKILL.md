---
name: monitoring
description: >
  Generate pipeline monitoring notebooks with health checks, SLA tracking,
  failure alerting, data freshness validation, and operational dashboards.
---

# Pipeline Monitoring

## Purpose
Generate production monitoring notebooks that track pipeline health, data freshness,
and execution metrics. Produces SLA monitoring with configurable alerting thresholds,
row count validation, and Databricks SQL dashboard definitions.

## Usage
```
/monitoring specs/monitoring-spec.md
```

## Capabilities
- Generate pipeline health check notebooks with pass/fail status
- Create SLA monitoring with configurable alerting thresholds
- Validate data freshness against expected ingestion schedules
- Track execution time trends and detect performance anomalies
- Produce Databricks SQL dashboard definitions for operational visibility
