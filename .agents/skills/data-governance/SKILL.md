---
name: data-governance
description: >
  Scaffold Unity Catalog configurations, schemas, column masking, and table access control lists (ACLs).
---

# Unity Catalog Data Governance Skill

## Purpose
Enforces enterprise security standards, data masking rules, row-level filters, and database access control permissions (ACLs) directly inside Databricks Unity Catalog.

## Usage
Triggered when the user asks for "Unity Catalog configurations", "column-level masking", "row-level filtering", "data governance setup", "database grants", or "secure schema design".

---

## Technical Scaffolding SQL Patterns

### 1. Catalog & Schema Isolation (3-Level Namespaces)
Set up a clean organizational namespace hierarchy separating Dev, Staging, and Prod environments.

```sql
-- Establish the catalog structure
CREATE CATALOG IF NOT EXISTS prod_catalog;
USE CATALOG prod_catalog;

-- Establish schemas for the medallion layers
CREATE SCHEMA IF NOT EXISTS b_sales_ingest
  COMMENT 'Bronze raw ingestion layer for sales data';

CREATE SCHEMA IF NOT EXISTS s_sales_clean
  COMMENT 'Silver cleansed and deduplicated layer for sales data';

CREATE SCHEMA IF NOT EXISTS g_sales_aggregate
  COMMENT 'Gold business metrics layer for sales data';
```

### 2. Row-Level Security Filters
Create logical policies to limit table rows shown to users depending on their active group permissions or location context.

```sql
-- Create a row-level filter function inside a dedicated security schema
CREATE SCHEMA IF NOT EXISTS security_policies;

CREATE OR REPLACE FUNCTION security_policies.regional_order_filter(region_name STRING)
  RETURN IS_ACCOUNT_GROUP_MEMBER('global_admin') OR
         (IS_ACCOUNT_GROUP_MEMBER('regional_analysts') AND region_name = 'USA');

-- Bind the row filter logic to the target table
ALTER TABLE s_sales_clean.orders
  SET ROW FILTER security_policies.regional_order_filter ON (region);
```

### 3. Column-Level Data Masking (PII Protection)
Anonymize sensitive fields (e.g. Email addresses, phone numbers) for non-administrative roles.

```sql
-- Create a column masking function for email strings
CREATE OR REPLACE FUNCTION security_policies.mask_pii_email(email STRING)
  RETURN CASE
    WHEN IS_ACCOUNT_GROUP_MEMBER('hr_admin') OR IS_ACCOUNT_GROUP_MEMBER('compliance_security') THEN email
    ELSE 'MASKED_PII_EMAIL@***.com'
  END;

-- Apply the masking function to a column in the silver table
ALTER TABLE s_sales_clean.customers
  ALTER COLUMN email SET MASK security_policies.mask_pii_email;
```

### 4. Access Control Lists (ACL Grants)
Scaffold basic SQL access control grants on catalogs, schemas, and individual tables.

```sql
-- Grant read-only access to silver schema tables to the data analysts group
GRANT USAGE ON CATALOG prod_catalog TO `data_analysts`;
GRANT USAGE ON SCHEMA s_sales_clean TO `data_analysts`;
GRANT SELECT ON ALL TABLES IN SCHEMA s_sales_clean TO `data_analysts`;

-- Grant full schema administration permissions to the data engineers group
GRANT ALL PRIVILEGES ON SCHEMA b_sales_ingest TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA s_sales_clean TO `data_engineers`;
```

---

## Best Practices & Common Pitfalls
* **DO** use account-level groups (`IS_ACCOUNT_GROUP_MEMBER`) in security functions instead of workspace-local groups or raw usernames.
* **DO** verify performance impacts of row filtering. Keep filtering functions lightweight and avoid indexing non-indexed files.
* **DON'T** apply masking rules directly on raw Bronze tables if downstream Silver cleansing scripts need access to the unmasked values for joins or matching.
* **DON'T** manage grants manually in notebooks for production; orchestrate them via Terraform or Databricks Asset Bundles (DAB).
