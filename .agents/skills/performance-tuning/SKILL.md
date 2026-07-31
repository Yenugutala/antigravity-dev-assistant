---
name: performance-tuning
description: >
  Scaffold optimization commands and query patterns (OPTIMIZE, Z-ORDER, partitioning, AQE, broadcast joins).
---

# Databricks Performance Tuning Skill

## Purpose
Accelerates query execution, improves data retrieval throughput, and optimizes resource utilization across PySpark and Spark SQL pipelines on Delta Lake.

## Usage
Triggered when the user asks for "performance tuning", "optimize query runtimes", "broadcast joins", "table partitioning strategy", "Z-order execution", or "tuning Spark configurations".

---

## Technical Scaffolding Code Patterns

### 1. Delta Table Optimization & Z-Ordering
Execute data skipping optimizations on high-cardinality columns used in `WHERE` filters or `JOIN` clauses.

```sql
-- Optimize layout of the orders table by co-locating data based on query search fields
OPTIMIZE s_sales.orders
ZORDER BY (customer_id, order_date);

-- Automate file compaction periodically inside your processing pipeline
-- Note: Run OPTIMIZE on a schedule (e.g. daily/weekly), not necessarily after every micro-batch
```

### 2. Broadcast Joins in PySpark
Optimize joins between a massive table (e.g., transactions) and a small lookup table (e.g., store codes) by caching the small table on all worker nodes, eliminating shuffle operations.

```python
from pyspark.sql.functions import broadcast

# Load a small lookup dimension table (< 100MB)
df_stores = spark.table("s_sales.stores")

# Load a massive transactional fact table
df_orders = spark.table("s_sales.orders")

# Perform broadcast join to prevent expensive shuffling of 'orders' data
df_joined = df_orders.join(
    broadcast(df_stores),
    on="store_id",
    how="left"
)

# Display plan to verify broadcast exchange operation is present
df_joined.explain(True)
```

### 3. Tuning Table Partitioning
Partition large tables (typically > 1TB in size) on low-cardinality, high-filtering columns like date or region. Avoid partitioning small tables.

```sql
-- DDL definition showing partitioning syntax
CREATE TABLE IF NOT EXISTS s_sales.orders (
  order_id STRING,
  customer_id STRING,
  line_total DOUBLE,
  order_date DATE,
  region STRING
)
USING DELTA
PARTITIONED BY (region);
```

### 4. Spark Performance Optimization Configurations
Inject these configurations into your SparkSession initialization scripts to configure query planning.

```python
# Enable Adaptive Query Execution (AQE) for dynamic optimization plan adjustments
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Enable Dynamic Partition Pruning (DPP) to skip irrelevant partition directories in joins
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")

# Configure target file size for writes to 32MB to 128MB (reduces latency in queries)
spark.conf.set("spark.databricks.delta.targetFileSize", "134217728") # 128MB
```

---

## Best Practices & Common Pitfalls

### Table Partitioning Guidelines
* **DO NOT** partition tables smaller than 1TB. Over-partitioning leads to the "small file problem", introducing significant metadata lookup overhead and degrading query performance.
* **DO** choose partition columns carefully. Good candidates have cardinality between 10 and 1000 (e.g., `region`, `year_month`, `country`). Never partition by high-cardinality columns like `order_id` or `timestamp`.

### The Tuning Checklist
* **DO** combine OPTIMIZE with Z-ORDER. Calling OPTIMIZE on its own only performs bin-packing.
* **DO** apply broadcast hints only for tables smaller than 100MB to avoid worker node memory exhaustion (Out of Memory - OOM errors).
* **DON'T** Z-Order on columns that are never used in query filters.
* **DON'T** run OPTIMIZE during active write transactions if you want to avoid write-conflict retries.
