---
name: cost-optimization
description: >
  Optimize Databricks compute costs (cluster sizing, auto-scaling, Delta cache, TTL, cluster policies).
---

# Databricks Cost Optimization Skill

## Purpose
Enforce and recommend structural configurations, query patterns, and cluster management strategies to minimize Databricks Unit (DBU) usage and cloud compute infrastructure costs without sacrificing performance.

## Usage
Triggered when the user asks for "cost optimization", "reduce DBU costs", "cluster size recommendations", "how to decrease compute bills", or "configure cluster policies".

---

## Technical Scaffolding Configurations

### 1. Enforcing Cluster Policies (JSON Template)
Use this Cluster Policy definition in Databricks to restrict developers to cost-effective VM types, enforce auto-termination time limits (TTL), and restrict maximum worker node configurations.

```json
{
  "autotermination_minutes": {
    "type": "fixed",
    "value": 15,
    "hidden": false
  },
  "spark_version": {
    "type": "regex",
    "pattern": "13\\..*|14\\..*",
    "defaultValue": "13.3.x-scala2.12"
  },
  "node_type_id": {
    "type": "allowlist",
    "values": [
      "Standard_D4s_v5",
      "Standard_D8s_v5"
    ],
    "defaultValue": "Standard_D4s_v5"
  },
  "driver_node_type_id": {
    "type": "fixed",
    "value": "Standard_D4s_v5",
    "hidden": false
  },
  "autoscale.max_workers": {
    "type": "range",
    "maxValue": 4,
    "defaultValue": 2
  },
  "autoscale.min_workers": {
    "type": "fixed",
    "value": 1
  }
}
```

### 2. Cost-Optimizing Spark Configurations
Configure these parameters inside your SparkSession settings (via Cluster configuration or notebook initialization) to maximize Delta caching benefits and prevent unnecessary file reads.

```python
# Configure Spark to utilize local SSD storage as Delta cache
spark.conf.set("spark.databricks.io.cache.enabled", "true")

# Enforce automatic file size management to avoid the "small file problem" (reduces read costs)
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# Set retention limits for Delta table history (avoids paying for obsolete historical files)
# Run SQL command to purge older records:
# ALTER TABLE s_sales.orders SET TBLPROPERTIES (delta.logRetentionDuration = "interval 30 days")
```

### 3. Cost-Efficient Jobs Cluster Configuration
Always deploy production jobs on dedicated **Job Clusters** (which are 3x to 4x cheaper per DBU than Interactive/All-Purpose compute). Below is a snippet representing a cost-efficient Job cluster setup in a Databricks Job task:

```json
{
  "new_cluster": {
    "spark_version": "13.3.x-scala2.12",
    "azure_attributes": {
      "first_on_demand": 1,
      "availability": "SPOT_WITH_FALLBACK_AZURE"
    },
    "node_type_id": "Standard_D4s_v5",
    "autoscale": {
      "min_workers": 1,
      "max_workers": 4
    },
    "spark_conf": {
      "spark.databricks.io.cache.enabled": "true"
    }
  }
}
```

---

## Best Practices & Common Pitfalls

### Cluster Sizing Recommendations
* **Single-Node Clusters**: Use single-node clusters (`num_workers = 0` or configuration setting `spark.databricks.cluster.profile singleNode`) for Dev/Sandbox testing, exploratory data analysis, and lightweight API pipelines.
* **Auto-scaling Workers**: Avoid setting wide gaps in worker limits (e.g. 1 to 50 workers). Scale conservatively (e.g. 2 to 8 workers) and pair with appropriate auto-termination thresholds.

### The DBU Cost Checklist
* **DO** use Spot Instances with a fallback mechanism for worker nodes in development and non-critical batch processing.
* **DO** enable Photon Engine only when processing massive datasets containing complex joins and groupings; disable Photon for simple, lightweight ingestion pipelines to save DBUs.
* **DO** configure auto-termination (TTL) for interactive development clusters to trigger within 10 to 20 minutes of inactivity.
* **DON'T** run production workloads on shared All-Purpose Interactive clusters.
* **DON'T** let historical table state build up indefinitely without configuring appropriate retention policies (e.g., `VACUUM` tables regularly to release stale file storage).
