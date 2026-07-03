---
name: cost-optimization
description: >
  Optimize Databricks compute costs (cluster sizing, auto-scaling, Delta cache, TTL, cluster policies).
---

# Databricks Cost Optimization Skill

## Purpose
Optimize query patterns and compute structures to minimize Databricks Unit (DBU) consumption and cloud infrastructure costs.

## Usage
Triggered when the user asks for "cost optimization", "reduce cost", "DBU optimization", or "cluster sizing recommendations".

## Guidelines
1. **Cluster Sizing**: Recommend single-node clusters for small/trial workloads, and multi-node auto-scaling clusters with appropriate worker minimums/maximums for large workloads.
2. **Photon Engine**: Advise when to enable Photon (complex SQL/joins on large datasets) vs. when to disable it (simple queries or small datasets where overhead outweighs benefit).
3. **Storage/Cache**: Promote use of Delta caching for repeated query tables.
4. **Lifecycle**: Suggest auto-termination timeouts (TTL) of 10-20 minutes for interactive development clusters, and using Job clusters (which are 3-4x cheaper than interactive clusters) for scheduled workflows.
