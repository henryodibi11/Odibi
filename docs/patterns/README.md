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

**See also:** [Manual HWM Guide](./incremental_stateful.md) - understanding the underlying SQL pattern.

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

### [6. Skip If Unchanged (Snapshot Optimization)](./skip_if_unchanged.md)
**Problem:** My hourly pipeline appends identical data 24 times/day when the source hasn't changed.

**Pattern:** Compute a hash of the DataFrame content before writing. If hash matches previous write, skip the append entirely.

**When to use:** Snapshot tables without timestamps, reference data that changes infrequently, or when change frequency is unknown.

---

## Dimensional Modeling Patterns

These patterns are designed for building star schemas and data warehouses. Use them via `pattern: type: pattern_name` in your node config.

### [7. Dimension Pattern](./dimension.md)
**Problem:** How do I build dimension tables with surrogate keys and SCD support?

**Pattern:** Use `pattern: type: dimension` to auto-generate surrogate keys and handle SCD Type 0/1/2 with optional unknown member rows.

**When to use:** Building any dimension table (dim_customer, dim_product, etc.)

```yaml
pattern:
  type: dimension
  params:
    natural_key: customer_id
    surrogate_key: customer_sk
    scd_type: 2
    track_cols: [name, email, address]
```

---

### [8. Date Dimension Pattern](./date_dimension.md)
**Problem:** How do I generate a complete date dimension with fiscal calendars?

**Pattern:** Use `pattern: type: date_dimension` to generate dates with 19 pre-calculated columns including fiscal year/quarter.

**When to use:** Every data warehouse needs a date dimension. Generate once with a wide range (2015-2035).

```yaml
pattern:
  type: date_dimension
  params:
    start_date: "2020-01-01"
    end_date: "2030-12-31"
    fiscal_year_start_month: 7
    unknown_member: true
```

---

### [9. Fact Pattern](./fact.md)
**Problem:** How do I build fact tables with automatic surrogate key lookups?

**Pattern:** Use `pattern: type: fact` to join source data to dimensions, retrieve SKs, handle orphans, and validate grain.

**When to use:** Building any fact table that references dimensions.

```yaml
pattern:
  type: fact
  params:
    grain: [order_id, line_item_id]
    dimensions:
      - source_column: customer_id
        dimension_table: dim_customer
        dimension_key: customer_id
        surrogate_key: customer_sk
    orphan_handling: unknown
```

---

### [10. Aggregation Pattern](./aggregation.md)
**Problem:** How do I build aggregate tables with declarative GROUP BY and incremental refresh?

**Pattern:** Use `pattern: type: aggregation` with grain (GROUP BY) and measure expressions.

**When to use:** Building aggregate/summary tables, KPI tables, or materializing metrics.

```yaml
pattern:
  type: aggregation
  params:
    grain: [date_sk, product_sk]
    measures:
      - name: total_revenue
        expr: "SUM(line_total)"
      - name: order_count
        expr: "COUNT(*)"
```

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
| Skip If Unchanged | Snapshot Source | Raw | `append` (conditional) | Yes (hash-based) |
| **Dimension** | Staging | Gold (dim_*) | `overwrite` | Yes (SK-based) |
| **Date Dimension** | Generated | Gold (dim_date) | `overwrite` | Yes (no input) |
| **Fact** | Staging + Dims | Gold (fact_*) | `overwrite` | Yes (grain-based) |
| **Aggregation** | Fact | Gold (agg_*) | `overwrite` | Yes (grain-based) |

---

## Further Reading

- Databricks: "Incremental Processing" documentation
- Book: *Fundamentals of Data Engineering* by Joe Reis & Matt Housley
