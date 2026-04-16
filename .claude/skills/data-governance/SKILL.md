---
name: data-governance
description: >
  Generate PII masking rules, role-based access control policies, audit logging,
  and compliance reporting for Databricks data platforms.
---

# Data Governance

## Purpose
Read governance specifications and generate PII detection, data masking notebooks,
RBAC policy scripts, and audit logging pipelines. Ensures compliance with
GDPR, CCPA, and enterprise data governance standards.

## Usage
```
/data-governance specs/governance-spec.md
```

## Capabilities
- Detect and classify PII columns (email, phone, SSN, address)
- Generate dynamic data masking rules using Delta Lake column masks
- Create RBAC policy scripts for Unity Catalog permissions
- Build audit logging pipelines to track data access patterns
- Produce compliance reports for GDPR and CCPA requirements
