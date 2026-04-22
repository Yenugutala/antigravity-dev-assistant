---
name: data-migration
description: >
  Generate migration scripts for schema changes, cross-environment data promotion,
  platform migrations, and rollback strategies for Delta Lake tables.
---

# Data Migration

## Purpose
Generate migration scripts for schema changes, cross-environment data promotion,
and platform migrations. Produces backward-compatible ALTER TABLE scripts,
data reconciliation notebooks, and rollback plans for safe production deployments.

## Usage
```
/data-migration specs/migration-spec.md
```

## Capabilities
- Generate schema migration scripts (ALTER TABLE, column adds/renames/drops)
- Create cross-environment promotion workflows (dev → staging → prod)
- Produce data backfill and reconciliation validation notebooks
- Generate rollback scripts for failed migration recovery
- Validate migration success with row-count and checksum verification
