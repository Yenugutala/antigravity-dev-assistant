---
paths:
  - "src/**/gold*.py"
---

# Gold Layer Rules

## Purpose
- Business-level aggregations and metrics
- Reads from silver tables, writes to gold tables
- Optimized for reporting and analytics consumption

## Spark SQL Syntax
- UPPERCASE all SQL keywords
- snake_case for all column names
- Use meaningful aliases for aggregated columns (e.g., `total_revenue`, `avg_order_value`)

## Aggregation Patterns
- Always GROUP BY explicit columns (never use positional references)
- Use COALESCE for nullable aggregation results
- Include record counts alongside aggregated metrics

## Schema
- Each notebook MUST start with `CREATE SCHEMA IF NOT EXISTS g_<pipeline>`
- Write to `g_<pipeline>.<table>` (e.g., `g_salesorders.revenue_by_category`)

## File Format
- Must start with `# Databricks notebook source`
- Use `# COMMAND ----------` to separate notebook cells
- Use `spark.sql(""" ... """)` for SQL execution in Python
