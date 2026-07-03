---
name: ci-cd-pipeline
description: >
  Generate CI/CD configurations for automated pipeline deployment using
  GitHub Actions, Azure DevOps, and Databricks Asset Bundles (DAB).
---

# CI/CD Pipeline Generator for Databricks

## Purpose
Help the developer build robust CI/CD pipelines to lint, test, package, and deploy their PySpark/Spark SQL medallion pipelines to Databricks environments (Dev, Staging, Prod).

## Usage
Triggered when the user asks for "CI/CD", "GitHub Actions", "Azure DevOps", or "Databricks Asset Bundles (DAB)" setup.

## Guidelines
1. **GitHub Actions**: Create `.github/workflows/deploy.yml` with tasks for Python setup, running local `pytest` tests, linting with `ruff`, and deploying via Databricks CLI.
2. **Azure DevOps**: Create `azure-pipelines.yml` with multi-stage environments and approval gates.
3. **Databricks Asset Bundles (DAB)**: Create `databricks.yml` specifying targets, cluster configurations, task paths, and job schedules.
4. **Environment Promotion**: Enforce strict separation of variables (e.g., schemas, workspace URLs) across Dev, Staging, and Prod.
