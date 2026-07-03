---
name: data-governance
description: >
  Scaffold Unity Catalog configurations, schemas, column masking, and table access control lists (ACLs).
---

# Unity Catalog Data Governance Skill

## Purpose
Enforce secure access controls, column-level masking, and row-level filtering on Delta tables using Databricks Unity Catalog.

## Usage
Triggered when the user asks for "Unity Catalog", "column masking", "row filtering", "access control", "grants", or "data governance".

## Guidelines
1. **Unity Catalog Catalogues & Schemas**: Scaffold three-level namespaces: `<catalog>.<schema>.<table>` (e.g. `prod_catalog.s_antigravity_sales.orders`).
2. **Column Masking**: Create SQL functions returning masked values (e.g. `CASE WHEN IS_ACCOUNT_GROUP_MEMBER('admin') THEN email ELSE 'MASKED' END`) and link them using `ALTER TABLE ... SET MASK`.
3. **Row-level Filters**: Set up security filters using SQL logic and link them using `ALTER TABLE ... SET ROW FILTER`.
4. **Access Control (Grants)**: Write clean Unity Catalog SQL access grants: `GRANT SELECT ON TABLE ... TO <group_name>`.
