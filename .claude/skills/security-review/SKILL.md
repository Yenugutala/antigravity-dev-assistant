---
name: security-review
description: >
  Scan pipeline code for security vulnerabilities including hardcoded secrets,
  SQL injection risks, access control gaps, and unsafe data handling patterns.
---

# Security Review

## Purpose
Scan data pipeline code and configurations for security vulnerabilities. Detects
hardcoded secrets, SQL injection risks, overly permissive access controls, and
sensitive data exposure in logs or outputs. Aligned with OWASP data pipeline guidelines.

## Usage
```
/security-review src/pipelines/
```

## Capabilities
- Detect hardcoded secrets, credentials, and API tokens in pipeline code
- Identify SQL injection and code injection vulnerabilities
- Analyze access control policies for permission gaps
- Flag sensitive data exposure in logs, error messages, and outputs
- Generate OWASP-aligned security checklists for data pipeline review
