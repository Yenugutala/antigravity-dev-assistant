---
name: incremental-load
description: >
  Scaffold Auto Loader ingestion patterns and Delta Lake merge (upsert) configurations.
---

# Delta Lake Incremental Load & Upsert Skill

## Purpose
Optimize pipeline data processing by only loading and merging new or modified records on each run.

## Usage
Triggered when the user asks for "incremental load", "Auto Loader", "upsert", "MERGE INTO", or "incremental ingest".

## Guidelines
1. **Auto Loader (`readStream`)**: Use `spark.readStream.format("cloudFiles")` with file schemas and checkpoint locations for continuous or scheduled file ingestion.
2. **Merge / Upsert (MERGE INTO)**: Scaffold `MERGE INTO <target> USING <source> ON <keys>` Spark SQL queries to update existing records and insert new ones.
3. **Idempotent Appends**: Guide when to use append write modes with deduplication partitioning to guarantee idempotency.
