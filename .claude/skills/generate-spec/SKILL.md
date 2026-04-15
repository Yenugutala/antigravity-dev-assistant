---
name: generate-spec
description: >
  Interactive specification generator that asks structured questions about a data pipeline
  and generates a complete spec file. Invoke with "create spec", "new pipeline spec",
  "generate specification", or "define pipeline".
---

# Generate Pipeline Specification — Interactive Q&A

## Purpose
Guide the user through creating a pipeline specification by asking structured questions.
Generate a complete spec file at `specs/<domain>-<entity>-spec.md`.

## Process

### Ask These Questions IN ORDER (wait for each answer):

**1. Domain & Entity**
"What business domain is this pipeline for? (e.g., sales, finance, marketing, hr)"
"What entity/dataset? (e.g., orders, invoices, customers, employees)"

**2. Data Source**
"Where does the data come from?"
Options: REST API, CSV files in DBFS, Parquet files, JDBC database, other
"What is the URL or file path?"

**3. Source Schema**
"What columns does the source data have? List them with types."
"Which column is the primary key?"

**4. Volume & Frequency**
"How many rows per day approximately?"
"How often should this run? (daily, hourly, real-time)"

**5. Silver Cleansing Rules**
"What cleansing rules should apply?"
Examples: trim whitespace, cast types, handle nulls, deduplicate
"Should invalid records be quarantined? What makes a record invalid?"

**6. Gold Aggregations**
"What business metrics do you need?"
Examples: total revenue, order counts, averages by category
"What dimensions to group by? (category, date, region)"

**7. Data Quality**
"What data quality checks are critical?"
Examples: no null primary keys, positive prices, referential integrity

**8. Consumers**
"Who will use the Gold layer data?"
Options: Power BI, Databricks SQL, ML models, APIs

### After All Questions Answered:

1. Read the template at `specs/SPEC-TEMPLATE.md`
2. Fill in all fields with the user's answers
3. Save to `specs/<domain>-<entity>-spec.md`
4. Show the generated spec to the user
5. Ask: "Would you like to modify anything, or shall I run /build-pipeline to generate the code?"
