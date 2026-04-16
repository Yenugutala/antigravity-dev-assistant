---
name: performance-tuning
description: >
  Analyze and optimize Spark SQL queries, Delta Lake table layouts,
  partitioning strategies, and pipeline execution performance.
---

# Performance Tuning

## Purpose
Analyze existing Spark SQL queries and Delta Lake tables to identify performance
bottlenecks. Generates optimization recommendations including partition pruning,
Z-ordering, caching strategies, and query rewrites.

## Usage
```
/performance-tuning specs/perf-spec.md
```

## Capabilities
- Analyze Spark SQL query plans and identify bottlenecks
- Recommend partitioning and Z-ordering strategies for Delta tables
- Generate OPTIMIZE and VACUUM maintenance scripts
- Suggest broadcast join hints and AQE configuration tuning
- Create benchmark notebooks for before/after performance comparison
