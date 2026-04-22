---
name: cost-optimization
description: >
  Analyze Databricks cluster configurations, storage usage, and job scheduling
  to generate cost reduction recommendations and cleanup scripts.
---

# Cost Optimization

## Purpose
Analyze Databricks workspace configurations to identify cost reduction opportunities.
Generates cluster right-sizing recommendations, storage optimization scripts,
and job scheduling strategies to minimize compute and storage costs.

## Usage
```
/cost-optimization specs/cost-spec.md
```

## Capabilities
- Generate cluster right-sizing recommendations based on workload analysis
- Create storage cost analysis with OPTIMIZE and VACUUM scheduling
- Recommend job scheduling strategies to avoid peak pricing windows
- Configure spot instance policies for non-critical pipeline jobs
- Produce unused table and orphaned file cleanup scripts
