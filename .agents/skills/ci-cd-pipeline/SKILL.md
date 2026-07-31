---
name: ci-cd-pipeline
description: >
  Generate CI/CD configurations for automated pipeline deployment using
  GitHub Actions, Azure DevOps, and Databricks Asset Bundles (DAB).
---

# Databricks CI/CD Pipeline & Asset Bundles (DAB) Skill

## Purpose
Establishes robust, production-ready continuous integration and continuous deployment (CI/CD) pipelines to lint, test, package, and deploy medallion data pipelines into Databricks environments. This ensures consistent workspace deployments using Databricks Asset Bundles (DAB), GitHub Actions, or Azure DevOps pipelines.

## Usage
Triggered when the user asks for "CI/CD setup", "GitHub Actions", "Azure DevOps pipelines", or "Databricks Asset Bundles (DAB)" configurations.

---

## Technical Scaffolding Templates

### 1. Databricks Asset Bundle Config (`databricks.yml`)
Place this configuration in the repository root to define environments, builds, and target jobs.

```yaml
bundle:
  name: ai-pipeline-accelerator

artifacts:
  default:
    type: whl
    path: .

targets:
  dev:
    workspace:
      host: https://adb-dev.azuredatabricks.net
      root_path: /Shared/.bundle/dev/${bundle.name}
    resources:
      jobs:
        run_medallion_pipeline:
          name: "[Dev] Medallion Pipeline Orchestrator"
          tasks:
            - task_key: run_pipeline
              notebook_task:
                notebook_path: ./notebooks/run_pipeline.py
              new_cluster:
                spark_version: 13.3.x-scala2.12
                node_type_id: Standard_D4s_v5
                num_workers: 1

  prod:
    workspace:
      host: https://adb-prod.azuredatabricks.net
      root_path: /Shared/.bundle/prod/${bundle.name}
    resources:
      jobs:
        run_medallion_pipeline:
          name: "[Prod] Medallion Pipeline Orchestrator"
          tasks:
            - task_key: run_pipeline
              notebook_task:
                notebook_path: ./notebooks/run_pipeline.py
              new_cluster:
                spark_version: 13.3.x-scala2.12
                node_type_id: Standard_D8s_v5
                autoscale:
                  min_workers: 2
                  max_workers: 8
```

### 2. GitHub Actions Deployment Workflow (`.github/workflows/deploy.yml`)
Automate validation, testing, and bundle deployment to target environments.

```yaml
name: Deploy Databricks Pipeline

on:
  push:
    branches:
      - develop
      - main
  pull_request:
    branches:
      - main

jobs:
  validate-and-test:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout Code
        uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-python: '3.10'
          cache: 'pip'

      - name: Install Dependencies
        run: |
          pip install ruff pytest pyspark
          if [ -f requirements.txt ]; then pip install -r requirements.txt; fi

      - name: Lint and Format Check
        run: ruff check .

      - name: Run Local Unit Tests
        run: pytest tests/ -v

  deploy-dev:
    needs: validate-and-test
    if: github.ref == 'refs/heads/develop'
    runs-on: ubuntu-latest
    env:
      DATABRICKS_HOST: ${{ secrets.DEV_DATABRICKS_HOST }}
      DATABRICKS_TOKEN: ${{ secrets.DEV_DATABRICKS_TOKEN }}
    steps:
      - name: Checkout Code
        uses: actions/checkout@v3

      - name: Install Databricks CLI
        run: |
          curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

      - name: Deploy Asset Bundle to Dev
        run: |
          databricks bundle deploy --target dev

  deploy-prod:
    needs: validate-and-test
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    env:
      DATABRICKS_HOST: ${{ secrets.PROD_DATABRICKS_HOST }}
      DATABRICKS_TOKEN: ${{ secrets.PROD_DATABRICKS_TOKEN }}
    steps:
      - name: Checkout Code
        uses: actions/checkout@v3

      - name: Install Databricks CLI
        run: |
          curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

      - name: Deploy Asset Bundle to Prod
        run: |
          databricks bundle deploy --target prod
```

### 3. Azure DevOps Pipeline Config (`azure-pipelines.yml`)
Implement multi-stage CI/CD deployments inside Azure DevOps.

```yaml
trigger:
  - develop
  - main

variables:
  - name: pythonVersion
    value: '3.10'

stages:
- stage: CI_Build_and_Test
  jobs:
  - job: RunTests
    pool:
      vmImage: 'ubuntu-latest'
    steps:
    - task: UsePythonVersion@0
      inputs:
        versionSpec: $(pythonVersion)
    - script: |
        pip install ruff pytest pyspark
        ruff check .
        pytest tests/ -v
      displayName: 'Lint and Execute Unit Tests'

- stage: Deploy_Dev
  dependsOn: CI_Build_and_Test
  condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/develop'))
  jobs:
  - deployment: DeployDevDAB
    pool:
      vmImage: 'ubuntu-latest'
    environment: 'databricks-dev'
    strategy:
      runOnce:
        deploy:
          steps:
          - checkout: self
          - script: |
              curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
            displayName: 'Install Databricks CLI'
          - script: |
              databricks bundle deploy --target dev
            displayName: 'Deploy Bundle to Dev Workspace'
            env:
              DATABRICKS_HOST: $(DEV_DATABRICKS_HOST)
              DATABRICKS_TOKEN: $(DEV_DATABRICKS_TOKEN)
```

---

## Best Practices & Common Pitfalls
* **DO** store environment workspace tokens and host URLs securely in repository Secrets (GitHub Secrets / DevOps Variables).
* **DO** use Databricks Asset Bundles (DAB) instead of legacy legacy workspace import APIs to manage jobs, notebooks, and configurations dynamically.
* **DON'T** hardcode personal access tokens (PAT) in configuration files or check them into git history.
* **DON'T** trigger direct deployments to staging or production without a preceding successful build and run of unit tests.
