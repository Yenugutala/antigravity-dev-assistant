---
name: data-lineage
description: >
  Generate end-to-end data lineage documentation showing source-to-gold flow,
  column-level lineage tracking, dependency graphs, and impact analysis.
---

# Data Lineage

## Purpose
Read pipeline specs and generated code to produce end-to-end data lineage documentation.
Traces column-level transformations from source through Bronze → Silver → Gold layers,
identifies upstream/downstream dependencies, and generates visual lineage diagrams.

## Usage
```
/data-lineage specs/bronze-spec.md specs/silver-spec.md specs/gold-spec.md
```

## Capabilities
- Trace column-level lineage across Bronze, Silver, and Gold layers
- Generate dependency graphs showing table-to-table relationships
- Produce impact analysis reports for proposed schema changes
- Create lineage visualizations in Mermaid and DOT format
- Map cross-pipeline dependencies for multi-domain data platforms
