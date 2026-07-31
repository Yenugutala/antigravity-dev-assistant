---
name: ontology
description: >
  Map data entities to semantic models, taxonomies, and knowledge graphs to enable AI semantic reasoning.
---

# Enterprise Data Ontology & Semantic Mapping Skill

## Purpose
Establishes a semantic bridge between physical Databricks Delta Lake schemas and conceptual business ontologies, taxonomies, and knowledge graphs. This enables semantic reasoning for advanced AI agents, metadata search engines, and enterprise catalog systems.

## Usage
Triggered when the user asks for "semantic modeling", "RDF triples", "JSON-LD context", "graph schemas", "knowledge graph mapping", "Neo4j Cypher scripts", or "ontology structures".

---

## Technical Scaffolding Code Patterns

### 1. JSON-LD Context Mapping (Metadata Cataloging)
Generate structured documents describing table entities, semantic types, and relationship associations to feed graph engines and LLM agents.

```json
{
  "@context": {
    "schema": "https://schema.org/",
    "enterprise": "https://ontology.enterprise.com/sales#",
    "Order": "enterprise:Order",
    "Customer": "enterprise:Customer",
    "orderId": {
      "@id": "enterprise:orderId",
      "@type": "schema:Text"
    },
    "hasCustomer": {
      "@id": "enterprise:hasCustomer",
      "@type": "@id"
    },
    "totalValue": {
      "@id": "enterprise:totalValue",
      "@type": "schema:Float"
    }
  },
  "@type": "Order",
  "orderId": "txn_89410",
  "hasCustomer": "https://data.enterprise.com/customer/cust_4430",
  "totalValue": 149.99
}
```

### 2. PySpark Semantic Triple Extraction (RDF-Like Structure)
Format flat Delta table records into RDF-like semantic triples: Subject-Predicate-Object (SPO) format for ingest into graph databases.

```python
from pyspark.sql.functions import col, lit, concat

# Map s_sales.orders table to a RDF Triple DataFrame
triples_df = (spark.table("s_sales.orders")
    .select(
        concat(lit("https://data.enterprise.com/order/"), col("order_id")).alias("subject"),
        lit("https://ontology.enterprise.com/sales#hasCustomer").alias("predicate"),
        concat(lit("https://data.enterprise.com/customer/"), col("customer_id")).alias("object")
    )
)

# Display extracted triples
triples_df.show(5, truncate=False)

# Write to storage for downstream graph systems
triples_df.write.format("delta").mode("append").saveAsTable("g_sales.order_ontology_triples")
```

### 3. Graph Database Scaffolding (Neo4j Cypher Template)
Scaffold Cypher statements to import, link, and query medallion data entities as nodes and edges in a graph database.

```cypher
// 1. Create constraints to ensure entity integrity
CREATE CONSTRAINT FOR (o:Order) REQUIRE o.orderId IS UNIQUE;
CREATE CONSTRAINT FOR (c:Customer) REQUIRE c.customerId IS UNIQUE;

// 2. Load Order nodes from Delta table export
LOAD CSV WITH HEADERS FROM 'file:///mnt/datalake/ontology/orders_export.csv' AS row
MERGE (o:Order {orderId: row.order_id})
SET o.lineTotal = toFloat(row.line_total),
    o.orderDate = row.order_date;

// 3. Load Customer nodes
LOAD CSV WITH HEADERS FROM 'file:///mnt/datalake/ontology/customers_export.csv' AS row
MERGE (c:Customer {customerId: row.customer_id})
SET c.name = row.customer_name;

// 4. Create semantic relationship edges
LOAD CSV WITH HEADERS FROM 'file:///mnt/datalake/ontology/orders_export.csv' AS row
MATCH (o:Order {orderId: row.order_id})
MATCH (c:Customer {customerId: row.customer_id})
MERGE (o)-[:HAS_CUSTOMER]->(c);
```

---

## Best Practices & Common Pitfalls
* **DO** utilize established standard taxonomies (such as schema.org, W3C RDF, or FOAF) for base metadata tags to guarantee interoperability.
* **DO** automate the extraction of triples using scheduled batch scripts that read from clean Silver tables.
* **DON'T** load massive raw datasets directly into graph databases; cleanse, aggregate, and deduplicate entities in Databricks first.
* **DON'T** hardcode URI paths in extraction code — define namespaces in a global properties configuration.
