---
name: data-quality
description: >
  Scaffold data validation checks, expectations rules, and test profiling for Delta tables.
---

# Data Quality & Validation Skill

## Purpose
Ensure data entering the Medallion pipeline is validated, conforming to structural expectations, range rules, and integrity checks.

## Usage
Triggered when the user asks for "data quality", "expectations", "validation checks", "data profiling", or "Delta Live Tables (DLT) expectations".

## Guidelines
1. **Delta Live Tables Expectations**: Support `EXPECT`, `EXPECT OR FAIL`, and `EXPECT OR DROP` constraints for raw data filtering.
2. **Schema & Check Constraints**: Scaffold `ALTER TABLE ... ADD CONSTRAINT` commands for basic database-level validations.
3. **Data Profiling**: Provide helper scripts to profile min/max values, null counts, and distribution metrics.
4. **Quarantine Logic**: Standardize routing of failed records to quarantine tables (e.g., `orders_quarantine`) with a `quarantine_reason` explaining why the record failed checks.
