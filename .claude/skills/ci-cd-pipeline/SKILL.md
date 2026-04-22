---
name: ci-cd-pipeline
description: >
  Generate CI/CD configurations for automated pipeline deployment using
  GitHub Actions, Azure DevOps, and Databricks Asset Bundles.
---

# CI/CD Pipeline

## Purpose
Generate CI/CD workflow configurations for automated linting, testing, and deployment
of data pipelines to Databricks. Supports GitHub Actions and Azure DevOps with
Databricks Asset Bundle (DAB) deployment and environment promotion strategies.

## Usage
```
/ci-cd-pipeline specs/cicd-spec.md
```

## Capabilities
- Generate GitHub Actions workflows for lint, test, and deploy stages
- Generate Azure DevOps pipeline YAML with multi-stage approvals
- Configure automated test execution in CI with mocked Spark sessions
- Produce Databricks Asset Bundle (DAB) deployment configurations
- Support environment promotion workflows (dev → staging → prod)
