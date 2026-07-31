---
name: data-quality
description: >
  Scaffold data validation checks, expectations rules, and test profiling for Delta tables.
---

# Data Quality & Validation Skill

## Purpose
Ensures that data ingested into and transformed by medallion pipelines is validated for completeness, correctness, and adherence to business rules, routing non-compliant records to quarantined tables.

## Usage
Triggered when the user asks for "data quality checks", "Delta check constraints", "DLT expectations", "quarantine logic", "validate schema rules", or "data profiling".

---

## Technical Scaffolding Code Patterns

### 1. Delta Live Tables (DLT) Expectations
Scaffold validation rules within Delta Live Tables utilizing `EXPECT`, `EXPECT OR DROP`, or `EXPECT OR FAIL`.

```python
import dlt
from pyspark.sql.functions import col

@dlt.table(
  name="orders_cleaned",
  comment="Cleansed orders data with data quality checks."
)
@dlt.expect("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("positive_amount", "line_total > 0")
@dlt.expect_or_fail("recent_date", "order_date >= '2020-01-01'")
def orders_cleaned():
    return dlt.read("orders_raw")
```

### 2. Standard Table Check Constraints
For normal Spark SQL / Delta Lake tables, enforce database-level structural validations.

```sql
-- Enforce non-null ID fields
ALTER TABLE s_sales.orders ADD CONSTRAINT order_id_not_null CHECK (order_id IS NOT NULL);

-- Enforce price boundaries
ALTER TABLE s_sales.orders ADD CONSTRAINT valid_line_total CHECK (line_total >= 0);

-- Enforce specific categories
ALTER TABLE s_sales.orders ADD CONSTRAINT valid_status CHECK (order_status IN ('COMPLETED', 'CANCELLED', 'PENDING_PAYMENT'));
```

### 3. Pipeline Quarantine Routing (PySpark)
Implement routing logic in traditional PySpark ingestion code to isolate dirty data without aborting the pipeline.

```python
from pyspark.sql.functions import col, when, lit, concat_ws, array

# Define validation rules
rules = {
    "missing_id": col("order_id").isNull(),
    "negative_total": col("line_total") < 0,
    "invalid_status": ~col("order_status").isin(['COMPLETED', 'CANCELLED', 'PENDING_PAYMENT'])
}

# Add boolean validation evaluation flags
df_flags = df
for rule_name, condition in rules.items():
    df_flags = df_flags.withColumn(f"_err_{rule_name}", condition)

# Determine record compliance status
failed_conditions = []
for rule_name in rules.keys():
    failed_conditions.append(when(col(f"_err_{rule_name}"), lit(rule_name)))

df_evaluated = df_flags.withColumn(
    "quarantine_reasons", 
    concat_ws(",", array(*failed_conditions))
)

# Route to Clean and Quarantine targets
df_clean = df_evaluated.filter(col("quarantine_reasons") == "").drop(*[f"_err_{r}" for r in rules.keys()]).drop("quarantine_reasons")
df_quarantine = df_evaluated.filter(col("quarantine_reasons") != "")

# Write to destinations
df_clean.write.format("delta").mode("append").saveAsTable("s_sales.orders")
df_quarantine.write.format("delta").mode("append").saveAsTable("s_sales.orders_quarantine")
```

### 4. Data Profiling Helper Script
Review table distributions, null records, and boundary checks.

```sql
SELECT 
  count(*) as total_records,
  sum(case when order_id is null then 1 else 0 end) as null_order_ids,
  sum(case when line_total < 0 then 1 else 0 end) as negative_amount_records,
  avg(line_total) as average_order_amount,
  min(order_date) as oldest_order_date
FROM s_sales.orders;
```

---

## Best Practices & Common Pitfalls
* **DO** use `expect_or_drop` for minor data anomalies (e.g. malformed emails) and reserve `expect_or_fail` for fatal structural anomalies (e.g. completely null primary keys).
* **DO** record the failure reason (quarantine reasons column) when writing records to quarantine tables, to assist with debugging.
* **DON'T** apply check constraints to raw Bronze tables; keep Bronze raw and unmodified so it acts as an exact historical representation of source data.
* **DON'T** let check constraints fail silently in background batch processes; monitor quarantine ratios over time.
