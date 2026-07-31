---
name: incremental-load
description: >
  Scaffold Auto Loader ingestion patterns and Delta Lake merge (upsert) configurations.
---

# Delta Lake Incremental Load & Upsert Skill

## Purpose
Enables efficient incremental ingestion and transformation of datasets in Databricks. Rather than reprocessing entire datasets on every schedule, this skill implements file ingestion via Auto Loader (`cloudFiles`) and state merges (`MERGE INTO`), significantly reducing compute times.

## Usage
Triggered when the user asks for "incremental load", "Auto Loader setup", "MERGE INTO queries", "write data stream", "upserting files", or "state recovery config".

---

## Technical Scaffolding Code Patterns

### 1. Ingestion using Databricks Auto Loader (`readStream`)
Leverage the `cloudFiles` format to detect new files arriving in storage and read them stream-style.

```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# 1. Define schema explicitly to avoid overhead
schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("line_total", DoubleType(), True),
    StructField("order_date", StringType(), True)
])

# 2. Configure Auto Loader stream reader
df_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/mnt/datalake/metadata/orders_schema_dir")
    .schema(schema)
    .load("/mnt/datalake/raw/orders/")
)

# 3. Write stream to Bronze table
query = (df_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/datalake/checkpoints/bronze_orders")
    .trigger(availableNow=True)
    .toTable("b_sales.orders")
)
```

### 2. Delta Lake Upsert via Spark SQL (`MERGE INTO`)
Merge incremental data changes (inserts, updates, deletes) from a staging table into a final Silver/Gold target table.

```sql
-- Ensure source data is deduplicated before executing merge
WITH deduplicated_updates AS (
  SELECT 
    order_id,
    customer_id,
    line_total,
    order_date,
    _ingestion_timestamp,
    ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY _ingestion_timestamp DESC) as r_num
  FROM s_sales.orders_staging
)
MERGE INTO s_sales.orders AS target
USING (SELECT * FROM deduplicated_updates WHERE r_num = 1) AS source
ON target.order_id = source.order_id

-- If matched, update values
WHEN MATCHED THEN
  UPDATE SET 
    target.customer_id = source.customer_id,
    target.line_total = source.line_total,
    target.order_date = source.order_date,
    target._ingestion_timestamp = source._ingestion_timestamp

-- If not matched, insert values
WHEN NOT MATCHED THEN
  INSERT (order_id, customer_id, line_total, order_date, _ingestion_timestamp)
  VALUES (source.order_id, source.customer_id, source.line_total, source.order_date, source._ingestion_timestamp);
```

### 3. Stream Merging into Delta Target using `foreachBatch`
Perform upsert operations inside a Structured Streaming pipeline using PySpark.

```python
def upsert_to_delta(micro_batch_df, batch_id):
    # Register microbatch as temporary view
    micro_batch_df.createOrReplaceTempView("batch_updates")
    
    # Run merge statement
    micro_batch_df.sparkSession.sql("""
        MERGE INTO s_sales.orders AS target
        USING (
          SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY _ingestion_timestamp DESC) as rn 
            FROM batch_updates
          ) WHERE rn = 1
        ) AS source
        ON target.order_id = source.order_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

# Configure streaming write using foreachBatch
stream_query = (df_stream.writeStream
    .foreachBatch(upsert_to_delta)
    .option("checkpointLocation", "/mnt/datalake/checkpoints/silver_orders_merge")
    .trigger(availableNow=True)
    .start()
)
```

---

## Best Practices & Common Pitfalls
* **DO** always isolate checkpoint folders across streams; sharing checkpoint directories will lead to stream failures and recovery state corruption.
* **DO** deduplicate micro-batches inside the merge query (as demonstrated in the `foreachBatch` example) to prevent "Multiple source rows matched the same target row" errors.
* **DON'T** run un-triggered streaming queries in a batch orchestrator. Use `.trigger(availableNow=True)` (or `once=True` in older runtimes) to process new files and terminate.
* **DON'T** forget to partition large target tables (over 1TB) by date or high-level key if the merge join keys align, to avoid full-table scans.
