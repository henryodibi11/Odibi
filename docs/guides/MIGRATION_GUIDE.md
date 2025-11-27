# Odibi V3 Migration Guide

## 1. Privacy Inheritance (Safety Upgrade)

**Change:** PII status now inherits from upstream nodes. If a column is marked as `pii: true` in a source node, it will remain PII in all downstream nodes unless explicitly declassified.

**Impact:**
*   **Existing Pipelines:** Pipelines that relied on implicit declassification (i.e., assuming PII status is lost after one node) may now trigger anonymization in downstream nodes if privacy is configured.
*   **New Behavior:** Safer by default. You cannot accidentally expose PII by forgetting to re-tag it.

**Action Required:**
If you have columns that are no longer PII (e.g., you hashed them or dropped the sensitive part), you must now explicitly **declassify** them in the node configuration if you want to stop tracking them as PII.

```yaml
- name: downstream_node
  privacy:
    method: "hash"
    declassify:
      - "hashed_email"  # Stop tracking this as PII
```

## 2. Spark Write Modes

**Change:** The Spark engine now supports `upsert` and `append_once` modes, bringing it to parity with the Pandas engine.

**Usage:**
These modes require `keys` to be defined in the `write.options` (or `params` if using a transformer that passes them). They are supported only for **Delta Lake** format.

```yaml
- name: merge_users
  write:
    connection: "silver"
    format: "delta"
    table: "users"
    mode: "upsert"
    options:
      keys: ["user_id"]
```

## 3. Context API

**Change:** The `NodeExecutionContext` (available in custom transformers as `ctx`) has been updated.
*   Added `ctx.schema`: Returns a dictionary of column types.
*   Added `ctx.pii_metadata`: Dictionary of active PII columns.
