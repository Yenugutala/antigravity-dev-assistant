---
name: documentation
description: >
  Auto-generate operational runbooks, SLA documents, technical architecture guides,
  and on-call playbooks from pipeline specs and code.
---

# Documentation

## Purpose
Auto-generate operational documentation from pipeline specs and generated code.
Produces runbooks, SLA documents, architecture diagrams, and on-call playbooks
in markdown format ready for Confluence, GitHub wiki, or internal knowledge bases.

## Usage
```
/documentation specs/bronze-spec.md specs/silver-spec.md specs/gold-spec.md
```

## Capabilities
- Generate operational runbooks (start, stop, restart, troubleshoot pipelines)
- Create SLA and data contract documentation with freshness guarantees
- Produce technical architecture diagrams in Mermaid format
- Generate pipeline dependency and scheduling documentation
- Create on-call playbooks for common failure scenarios and resolution steps
