---
name: data-dictionary
description: >
  Generate AI-readable and human-readable metadata, column descriptions, and data catalogs to support Text-to-SQL and search.
---

# Data Dictionary & Metadata Skill (AI-Ready Cataloging)

## Purpose
Establishes a standardized framework for documenting database assets in Databricks Unity Catalog. Clear metadata, comments, and schemas enable human catalog searchability and empower AI agents (such as text-to-SQL semantic searchers) to accurately navigate data tables and columns.

## Usage
Triggered when the user asks for "data dictionary", "column descriptions", "cataloging schemas", "metadata setup", or "Text-to-SQL context definition".

---

## Technical Scaffolding Configurations

### 1. Unity Catalog SQL Comments (DDL)
Standardize comments directly on tables and columns using SQL syntax within your medallion pipeline deployment scripts.

```sql
-- Apply comment at the table level
COMMENT ON TABLE s_sales.orders IS 'Cleaned sales transaction records representing successfully completed orders.';

-- Apply comments at the column level
COMMENT ON COLUMN s_sales.orders.order_id IS 'Unique identifier (UUID) generated at source checkout representing a single transaction transaction.';
COMMENT ON COLUMN s_sales.orders.customer_id IS 'Foreign key reference mapping back to s_crm.customers.customer_id.';
COMMENT ON COLUMN s_sales.orders.line_total IS 'Calculated order total representing price multiplied by quantity, rounded to 2 decimal places.';
COMMENT ON COLUMN s_sales.orders.order_status IS 'State of the order. Possible values: COMPLETED, CANCELLED, PENDING_PAYMENT.';
```

### 2. AI Semantic Context Mapping (JSON Template)
Use this schema structure to define column alias mappings and business synonyms for AI search agents and Text-to-SQL LLM prompts.

```json
{
  "table_name": "s_sales.orders",
  "business_description": "Contains order details and financials.",
  "columns": [
    {
      "name": "line_total",
      "data_type": "DOUBLE",
      "synonyms": ["revenue", "sales amount", "total price", "turnover"],
      "description": "Calculated value of order total. Formula: (price * quantity) - discount."
    },
    {
      "name": "order_status",
      "data_type": "STRING",
      "synonyms": ["state of order", "progress code"],
      "description": "Lifecycle status of the transaction.",
      "accepted_values": ["COMPLETED", "CANCELLED", "PENDING_PAYMENT"]
    }
  ]
}
```

### 3. Markdown Data Dictionary Format
Standardize documentation files generated alongside code artifacts.

```markdown
# Data Dictionary: Sales Orders (`s_sales.orders`)

Contains transactional data representing completed checkout flows.

## Primary & Foreign Keys
* **Primary Key**: `order_id`
* **Foreign Keys**:
  * `customer_id` references `s_crm.customers.customer_id`

## Schema Structure
| Column Name | Data Type | Nullable | Description | Business Synonyms |
| :--- | :--- | :--- | :--- | :--- |
| `order_id` | STRING | No | Unique UUID transaction ID. | Order Code, Txn ID |
| `customer_id` | STRING | No | References the buyer profile. | Buyer ID, Client ID |
| `line_total` | DOUBLE | Yes | Total purchase cost in USD. | Sales, Revenue |
| `order_status` | STRING | Yes | State of the transaction. | Status, State |
```

---

## Best Practices & Common Pitfalls
* **DO** write comments for both tables and individual columns during table creation (DDL) or table alteration (DML).
* **DO** map ambiguous column names (e.g. `amt_gross`) to common business terms (e.g. `gross revenue`) in the AI context.
* **DON'T** let comments fall out of sync with code changes; updates to table schemas must trigger modifications in database comments.
* **DON'T** rely on implicit schemas for Text-to-SQL prompts without explicitly declaring data type assertions and acceptable values.
