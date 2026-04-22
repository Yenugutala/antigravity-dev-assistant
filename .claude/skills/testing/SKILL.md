---
name: testing
description: >
  Auto-generate unit tests, integration tests, and data validation tests
  from pipeline specs. Produces pytest suites runnable locally without a Databricks cluster.
---

# Testing

## Purpose
Read pipeline specifications and generated code to auto-generate comprehensive test
suites. Produces pytest-based unit tests, integration tests, and data validation tests
that run locally without requiring a Databricks cluster.

## Usage
```
/testing specs/bronze-spec.md specs/silver-spec.md specs/gold-spec.md
```

## Capabilities
- Generate unit tests from spec schemas with expected column types and counts
- Create integration tests for end-to-end Bronze → Silver → Gold flows
- Produce mock Spark session and DataFrame stubs for local execution
- Generate data validation test cases (null checks, type checks, range validation)
- Report test coverage gaps and suggest additional test scenarios
