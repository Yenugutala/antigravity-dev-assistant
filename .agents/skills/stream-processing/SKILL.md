---
name: stream-processing
description: >
  Scaffold Spark Structured Streaming configurations, checkpoints, trigger offsets, and watermarks.
---

# Spark Structured Streaming Skill

## Purpose
Builds scalable, near-real-time event-driven data processing pipelines using Spark Structured Streaming on Delta Lake.

## Usage
Triggered when the user asks for "Structured Streaming", "streaming configurations", "checkpointing directories", "watermarking queries", "availableNow triggers", or "event-time windowing".

---

## Technical Scaffolding Code Patterns

### 1. Basic Streaming Read & Write Skeleton
Configure a Structured Streaming pipeline to ingest data from a source directory and write to a Delta table.

```python
# 1. Define stream reader
streaming_df = (spark.readStream
    .format("delta")
    .load("/mnt/datalake/bronze/orders")
)

# 2. Apply transformations
transformed_df = streaming_df.filter(streaming_df.line_total > 0)

# 3. Configure stream writer with checkpoints and triggers
query = (transformed_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/datalake/checkpoints/silver_orders")
    .trigger(availableNow=True) # Runs micro-batches on all available files, then terminates
    .toTable("s_sales.orders")
)
```

### 2. Watermarking and Event-Time Window Aggregations
Compute rolling metrics over time windows while applying watermarks to manage state storage limits by discarding late-arriving data.

```python
from pyspark.sql.functions import col, window, count, sum

# Load event-based streams (must contain a timestamp column)
events_df = (spark.readStream
    .format("delta")
    .load("/mnt/datalake/bronze/event_logs")
)

# Apply 10-minute watermarking and group by 5-minute rolling windows
windowed_aggregations = (events_df
    .withColumn("event_time", col("event_timestamp").cast("timestamp"))
    .withWatermark("event_time", "10 minutes") # Keep 10 minutes of state for late-arriving records
    .groupBy(
        window(col("event_time"), "5 minutes", "5 minutes"),
        col("event_type")
    )
    .agg(
        count("*").alias("event_count"),
        sum("payload_bytes").alias("total_bytes")
    )
)

# Write results to Gold aggregated table
agg_query = (windowed_aggregations.writeStream
    .format("delta")
    .outputMode("complete") # Complete mode is required for state aggregations without a primary key merge
    .option("checkpointLocation", "/mnt/datalake/checkpoints/gold_event_metrics")
    .trigger(processingTime="1 minute") # Triggers execution every minute
    .toTable("g_sales.event_aggregations")
)
```

### 3. Stream-Static Joins
Enrich streaming transaction records in real-time by joining them with a static catalog mapping table.

```python
# 1. Read dynamic transaction events stream
stream_df = spark.readStream.format("delta").load("/mnt/datalake/bronze/transactions")

# 2. Read static customer profiles table
static_customers_df = spark.table("s_crm.customers")

# 3. Perform join (Spark automatically handles joining streaming records with static references)
enriched_stream = stream_df.join(
    static_customers_df,
    on="customer_id",
    how="inner"
)

# 4. Write enriched stream to target table
enrich_query = (enriched_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/datalake/checkpoints/silver_transactions")
    .trigger(availableNow=True)
    .toTable("s_sales.transactions_enriched")
)
```

---

## Best Practices & Common Pitfalls

### State & Watermark Management
* **DO** always specify watermarks using `withWatermark()` when executing state aggregations (such as `groupBy` or `join` operations) in streaming pipelines. Without watermarks, Spark maintains state tables in memory indefinitely, eventually causing Out of Memory (OOM) failures.
* **DO** ensure the watermark column matches the event timestamp.

### The Streaming Checklist
* **DO** choose the trigger policy matching business latency requirements:
  - `trigger(availableNow=True)`: Best for batch-like cost structures, executing all new files in parallel before shutting down.
  - `trigger(processingTime='1 minute')`: Best for continuously running pipelines with low latency.
* **DON'T** share checkpoint directories across different streams; this will result in metadata directory conflicts and cause streaming queries to crash.
* **DON'T** modify PySpark schema structures on active streaming sources without planning directory-level migration or resetting checkpoints.
