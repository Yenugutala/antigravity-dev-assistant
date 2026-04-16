---
name: data-catalog
description: >
  Auto-generate data dictionary, column-level documentation, lineage diagrams,
  and metadata catalogs for Databricks Delta Lake tables.
---

# Data Catalog

## Purpose
Scan existing Delta Lake schemas and auto-generate comprehensive data catalog
documentation including data dictionaries, column descriptions, data lineage,
and table relationship diagrams.

## Usage
```
/data-catalog specs/catalog-spec.md
```

## Capabilities
- Generate data dictionary with column names, types, and descriptions
- Create data lineage diagrams (source → Bronze → Silver → Gold)
- Document table relationships and foreign key mappings
- Produce Markdown documentation for stakeholder review
- Export catalog metadata in JSON format for integration with Unity Catalog
