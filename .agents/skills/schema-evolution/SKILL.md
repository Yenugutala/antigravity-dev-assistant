---
name: schema-evolution
description: >
  Implement schema evolution strategies (mergeSchema, schema validation, schema drift handling).
---

# Delta Lake Schema Evolution Skill

## Purpose
Ensures that medallion data pipelines handle changes in source data structures (such as new columns, altered fields, or changing data types) without failing execution or corrupting downstream target tables.

## Usage
Triggered when the user asks for "schema evolution", "schema drift management", "mergeSchema option", "overwriteSchema configurations", "handling new columns", or "validating table schema changes".

---

## Technical Scaffolding Code Patterns

### 1. Enabling Delta Schema Evolution on Write
Use the `mergeSchema` write option in PySpark to automatically append new columns arriving in source data into the target Delta table structure.

```python
# Ingest new columns automatically without schema-related pipeline crashes
(df_new_data.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable("s_sales.orders")
)
```

### 2. Overwriting Table Schema & Structure
When schema layouts change completely (e.g. Columns are dropped or drastically re-typed) and history does not need to be preserved, use `overwriteSchema`.

```python
# Force-overwrite table metadata and column definitions with new layout
(df_modified_structure.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("s_sales.orders")
)
```

### 3. Automated Schema Drift Check (Alerting Script)
Implement a programmatic validation utility to check for schema drift before initiating writes.

```python
def check_schema_drift(incoming_df, target_table_name):
    # Fetch existing schema from target table catalog
    try:
        target_schema = spark.table(target_table_name).schema
        incoming_schema = incoming_df.schema
        
        # Compare column sets
        target_cols = set([(field.name.lower(), field.dataType) for field in target_schema])
        incoming_cols = set([(field.name.lower(), field.dataType) for field in incoming_schema])
        
        new_columns = incoming_cols - target_cols
        missing_columns = target_cols - incoming_cols
        
        if new_columns:
            print(f"[ALERT] Schema Drift Detected! New columns present: {new_columns}")
            # Insert custom alerting framework call here (e.g., Slack webhooks, alerts log)
            return True
        return False
    except Exception as e:
        print(f"[INFO] Target table {target_table_name} does not exist yet. No drift possible.")
        return False

# Run validation checks inside ingestion code
check_schema_drift(df_ingest, "s_sales.orders")
```

### 4. Explicit Type Casting (Compilation Safety)
Prevent data type mismatch compilation errors (e.g. attempting to write `double` values into an `integer` column) by casting fields explicitly.

```python
from pyspark.sql.types import DoubleType, IntegerType, StringType

# Explicitly align column data types to prevent mergeSchema failures
df_cast = df_raw.select(
    col("order_id").cast(StringType()),
    col("customer_id").cast(StringType()),
    col("quantity").cast(IntegerType()),
    col("price").cast(DoubleType())
)
```

---

## Best Practices & Common Pitfalls
* **DO** use `mergeSchema` only for additive column updates. It cannot handle destructive changes like column deletions or modifications to data types.
* **DO** perform explicit type casting on numeric columns during raw ingestion; Delta Lake will refuse to merge `double` data types into fields defined as `int` (throwing a `CANNOT_MERGE_TYPE` exception).
* **DON'T** enable `mergeSchema` as a global default on all tables without setting up schema drift alerting; tracking changes is necessary to keep analytical dashboards aligned.
* **DON'T** use `overwriteSchema` in production pipelines unless you intend to wipe existing records and re-initialize the table.
