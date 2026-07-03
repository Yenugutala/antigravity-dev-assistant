---
name: stream-processing
description: >
  Scaffold Spark Structured Streaming configurations, checkpoints, trigger offsets, and watermarks.
---

# Spark Structured Streaming Skill

## Purpose
Build near-real-time event driven data processing pipelines using Spark Structured Streaming.

## Usage
Triggered when the user asks for "streaming", "Structured Streaming", "checkpoint", "trigger once", or "watermarking".

## Guidelines
1. **Streaming Checkpoints**: Always specify checkpoint paths on DBFS/cloud storage using `.option("checkpointLocation", <path>)` to enable stream recovery.
2. **Trigger Policies**: Support `trigger(availableNow=True)` (or legacy `once=True`) for batch-like cost structures while running a streaming pipeline.
3. **Watermarking & Windowing**: Scaffold `withWatermark()` declarations for managing state table size when performing aggregations on event-time windows.
