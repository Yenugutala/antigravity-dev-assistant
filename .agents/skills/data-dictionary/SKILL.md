---
name: data-dictionary
description: >
  Generate AI-readable and human-readable metadata, column descriptions, and data catalogs to support Text-to-SQL and search.
---

# Data Dictionary & Metadata Skill (AI-Ready Cataloging)

## Purpose
Produce rich, documented schema catalogs and data dictionaries containing business context, data types, distinct values, and rules. AI agents (such as code generators and Text-to-SQL engines) read this catalog to query tables accurately without syntax errors.

## Usage
Triggered when the user asks for "data dictionary", "metadata comments", "data catalog", "Text-to-SQL context", or "column descriptions".

## Guidelines
1. **Unity Catalog Comments**: Generate SQL `COMMENT ON TABLE` and `COMMENT ON COLUMN` scripts containing rich description text (e.g. `COMMENT ON COLUMN s_antigravity_sales.orders.line_total IS 'The calculated total price for this line item, rounded to 2 decimal places.'`).
2. **Markdown Data Dictionaries**: Generate markdown-formatted dictionaries describing table schemas, primary keys, foreign keys, constraints, and sample distributions.
3. **AI Context Injection**: Format metadata with tags or descriptions specifically structured to guide Text-to-SQL LLMs (e.g. mapping synonyms like "revenue" to column name `line_total`).
