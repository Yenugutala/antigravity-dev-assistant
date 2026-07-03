---
name: schema-evolution
description: >
  Implement schema evolution strategies (mergeSchema, schema validation, schema drift handling).
---

# Delta Lake Schema Evolution Skill

## Purpose
Gracefully handle structural changes in incoming datasets (e.g., new columns, renamed fields) without breaking downstream pipelines.

## Usage
Triggered when the user asks for "schema evolution", "schema drift", "mergeSchema", or "handle new columns".

## Guidelines
1. **Delta Evolution**: Configure write options with `.option("mergeSchema", "true")` for auto-ingesting new columns during append/overwrite writes.
2. **Schema Drift Handling**: Scaffold staging-to-target schema validation scripts that automatically alert or log when unexpected structural changes occur.
3. **Explicit Type Casting**: Enforce strict numeric and string type casting to prevent schema merge compilation errors (e.g., trying to merge `DoubleType` into `IntegerType`).
