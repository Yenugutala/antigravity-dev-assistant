---
name: stream-processing
description: >
  Generate Spark Structured Streaming pipelines from specifications,
  including Kafka ingestion, watermarking, windowed aggregations, and checkpointing.
---

# Stream Processing

## Purpose
Read streaming pipeline specifications and generate production-ready Spark Structured
Streaming notebooks. Supports Kafka, Event Hubs, and Auto Loader sources with
watermarking, windowed aggregations, and exactly-once checkpointing.

## Usage
```
/stream-processing specs/streaming-spec.md
```

## Capabilities
- Generate Structured Streaming pipelines with readStream/writeStream
- Configure Kafka and Azure Event Hubs source connectors
- Implement watermarking and late data handling
- Create windowed aggregations (tumbling, sliding, session windows)
- Set up Delta Lake streaming sinks with checkpointing
