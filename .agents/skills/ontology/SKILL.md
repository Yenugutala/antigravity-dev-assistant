---
name: ontology
description: >
  Map data entities to semantic models, taxonomies, and knowledge graphs to enable AI semantic reasoning.
---

# Enterprise Data Ontology Skill (AI Semantic Mapping)

## Purpose
Bridge raw data structures and databases (e.g. Delta Lake tables) with enterprise semantic models and ontologies, allowing AI agents to understand the business relationships between entity schemas.

## Usage
Triggered when the user asks for "ontology", "semantic model", "knowledge graph", "JSON-LD", "RDF", or "semantic relationships".

## Guidelines
1. **Semantic Mapping**: Scaffold relationships between tables (e.g. `s_antigravity_sales.orders` is related to `s_antigravity_sales.customers` via a `has_customer` relationship).
2. **JSON-LD & RDF Generation**: Generate structured metadata in JSON-LD formats representing table entities for consumption by LLMs or graph databases.
3. **Graph Schemas**: Design property graph schemas (e.g., node and edge definitions) for representing medallion databases in Neo4j or Amazon Neptune.
