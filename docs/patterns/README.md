# Odibi Data Patterns

This directory contains documentation for common data pipeline patterns used in Odibi. Each pattern solves a specific problem and includes step-by-step examples.

## Patterns

### [1. Append-Only Raw Layer](./append_only_raw.md)
**Problem:** How do I safely ingest data without losing audit trails?

**Pattern:** All data from sources is appended to the Raw layer without modification. Raw is immutable and append-only.

**When to use:** Always, for all source ingestion. Raw is your safety net.

---

### [2. High Water Mark (Smart Read)](./smart_read.md)
**Problem:** My source table has millions of rows. How do I efficiently read only new/changed data?

**Pattern:** Use the `incremental` configuration (Smart Read) to automatically filter the source query. Odibi manages the state for you: First Run = Full Load, Subsequent Runs = Incremental Load.

**When to use:** When your source has timestamps (created_at, updated_at) and you want incremental reads. Essential for daily incremental loads.

**See also:** [Manual HWM Guide](./hwm_pattern_guide.md) - understanding the underlying SQL pattern.

---

### [3. Merge/Upsert (Silver Layer)](./merge_upsert.md)
**Problem:** How do I deduplicate and keep the latest version of each record?

**Pattern:** Use Delta Lake's MERGE operation (or Merge Transformer) to upsert records by key, with audit columns tracking created/updated timestamps.

**When to use:** Refining Raw → Silver. Always use for stateful transformations.

---

### [4. SCD Type 2 (History Tracking)](./scd2.md)
**Problem:** "I need to know what the address was *last month*, not just now."

**Pattern:** Track full history. Old records are closed (valid_to set), new records are opened (valid_to NULL). Preserves point-in-time accuracy.

**When to use:** Slowly Changing Dimensions (Customer address, Product category).

---

### [5. Windowed Reprocess (Gold Layer Aggregates)](./windowed_reprocess.md)
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
| Smart Read | Source + Timestamp | Raw | `append` | Yes (auto-managed) |
| Merge/Upsert | Raw (micro-batch) | Silver | `merge` | Yes (by key) |
| SCD Type 2 | Raw (micro-batch) | Silver/Gold | `overwrite` | Yes (full history) |
| Windowed Reprocess | Silver (window) | Gold | `overwrite` (partition) | Yes (recalculated) |

---

## Further Reading

- [Odibi Architecture Manifesto](../design/02_architecture_manifesto.md)
- [Merge Transformer Spec](../design/03_merge_transformer_spec.md)
- Databricks: "Incremental Processing" documentation
- Book: *Fundamentals of Data Engineering* by Joe Reis & Matt Housley
