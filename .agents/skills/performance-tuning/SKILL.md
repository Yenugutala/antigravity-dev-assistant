---
name: performance-tuning
description: >
  Scaffold optimization commands and query patterns (OPTIMIZE, Z-ORDER, partitioning, AQE, broadcast joins).
---

# Databricks Performance Tuning Skill

## Purpose
Speed up PySpark and Spark SQL execution times by aligning table structures and query patterns with Delta Engine optimization engines.

## Usage
Triggered when the user asks for "performance tuning", "optimize query", "Z-order", "partitioning", or "slow pipeline".

## Guidelines
1. **Delta Optimization**: Suggest executing `OPTIMIZE <table> ZORDER BY (<columns>)` on fields frequently used in JOIN or WHERE clauses.
2. **Table Partitioning**: Recommend partitioning only for tables larger than 1TB, on high-cardinality columns (like `date`). Advise *against* partitioning small tables.
3. **Adaptive Query Execution (AQE)**: Ensure AQE configuration properties are enabled.
4. **Join Optimization**: Guide use of Broadcast Joins for small dimension tables (e.g., joining orders with a small categories mapping table).
