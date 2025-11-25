# Odibi Data Patterns

This directory contains documentation for common data pipeline patterns used in Odibi. Each pattern solves a specific problem and includes step-by-step examples.

## Patterns

### [1. Append-Only Raw Layer](./append_only_raw.md)
**Problem:** How do I safely ingest data without losing audit trails?

**Pattern:** All data from sources is appended to the Raw layer without modification. Raw is immutable and append-only.

**When to use:** Always, for all source ingestion. Raw is your safety net.

---

### [2. High Water Mark (Incremental Load)](./hwm_pattern_guide.md)
**Problem:** My source table has millions of rows. How do I efficiently read only new/changed data?

**Pattern:** Track the maximum timestamp processed, then filter the source query to only rows newer than that mark. On first run, load full history; on subsequent runs, load only new/changed data.

**When to use:** When your source has timestamps (created_at, updated_at) and you want incremental reads. Essential for daily incremental loads, CDC pipelines, and cost-efficient large-table processing.

**See also:** [Complete HWM Pattern Guide](./hwm_pattern_guide.md) - comprehensive guide with setup, examples, troubleshooting, and migration path.

---

### [3. Merge/Upsert (Silver Layer)](./merge_upsert.md)
**Problem:** How do I deduplicate and keep the latest version of each record?

**Pattern:** Use Delta Lake's MERGE operation (or Merge Transformer) to upsert records by key, with audit columns tracking created/updated timestamps.

**When to use:** Refining Raw → Silver. Always use for stateful transformations.

---

### [4. Windowed Reprocess (Gold Layer Aggregates)](./windowed_reprocess.md)
**Problem:** Late-arriving data can break my aggregates. How do I fix them without double-counting?

**Pattern:** Instead of patching aggregates with updates, recalculate the entire time window and overwrite that partition.

**When to use:** Building Gold-layer aggregates (KPIs, star schemas). Ensures idempotency and correctness.

---

## Design Principles

These patterns are built on the **Odibi Architecture Manifesto**:

1. **Robots Remember, Humans Forget** → Use checkpoint bookkeeping, not manual state tracking
2. **Raw is Sacred** → Append-only, immutable history. Never destroy original data.
3. **Rebuild the Bucket, Don't Patch the Hole** → Reprocess entire time windows, don't patch aggregates
4. **SQL is for Humans, ADLS is for Robots** → ADLS stores everything; SQL serves BI
5. **No Duplication** → Test against production data; don't duplicate datasets

---

## Quick Reference

| Pattern | Input | Output | Write Mode | Idempotent? |
|---------|-------|--------|------------|-----------|
| Append-Only Raw | Source | Raw | `append` | Yes (duplicates OK) |
| High Water Mark | Source + Timestamp | Raw | `append` | Yes (filtered by timestamp) |
| Merge/Upsert | Raw (micro-batch) | Silver | `merge` | Yes (by key) |
| Windowed Reprocess | Silver (window) | Gold | `overwrite` (partition) | Yes (recalculated) |

---

## Further Reading

- [Odibi Architecture Manifesto](../design/02_architecture_manifesto.md)
- [Merge Transformer Spec](../design/03_merge_transformer_spec.md)
- Databricks: "Incremental Processing" documentation
- Book: *Fundamentals of Data Engineering* by Joe Reis & Matt Housley
