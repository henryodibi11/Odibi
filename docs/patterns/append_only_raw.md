# Pattern: Append-Only Raw Layer (Landing → Raw)

**Status:** Core Pattern  
**Layer:** Raw (Bronze)  
**Engine:** Spark Structured Streaming or Batch  
**Write Mode:** `append`  

---

## Problem

You have source data coming from multiple places (SQL databases, APIs, files). You need to:
- Preserve the complete history of what arrived and when
- Enable replay/reconstruction if downstream logic breaks
- Avoid data loss from overwriting mistakes

## Solution

**Append every piece of data you receive to the Raw layer without modification.** Raw is the immutable audit log.

---

## Key Principles

1. **Raw is Sacred** → Never delete or overwrite Raw data
2. **Append-Only** → Each run appends new rows (possibly duplicates)
3. **One-to-One Copy** → Raw mirrors the source schema, nothing more
4. **Permanent Retention** → Keep forever; storage is cheap

---

## Pattern Flow

```
Source System (SQL, API, Files)
         ↓
    Read Data
         ↓
   Append to Raw
    (append mode)
         ↓
Raw Layer (history preserved)
         ↓
[Next: Merge into Silver for dedup/cleanup]
```

---

## Example: SQL Source → Raw Layer

### Scenario

You have a SQL table `dbo.orders` with new/updated rows arriving daily.

**First Run (Day 1):**

Source (`dbo.orders`):
```
order_id | product      | qty | created_at
---------|--------------|-----|-------------------
1        | Widget A     | 10  | 2025-11-01 10:00
2        | Widget B     | 5   | 2025-11-01 11:00
3        | Widget A     | 2   | 2025-11-01 12:00
```

**After Append to Raw:**
```
Raw.orders (Delta Table)
order_id | product      | qty | created_at
---------|--------------|-----|-------------------
1        | Widget A     | 10  | 2025-11-01 10:00
2        | Widget B     | 5   | 2025-11-01 11:00
3        | Widget A     | 2   | 2025-11-01 12:00
```

**Second Run (Day 2):**

Source now has 2 new rows + 1 duplicate:
```
order_id | product      | qty | created_at
---------|--------------|-----|-------------------
1        | Widget A     | 10  | 2025-11-01 10:00  ← Already seen
2        | Widget B     | 5   | 2025-11-01 11:00  ← Already seen
3        | Widget A     | 2   | 2025-11-01 12:00  ← Already seen
4        | Widget C     | 8   | 2025-11-02 09:00  ← NEW
5        | Widget B     | 12  | 2025-11-02 10:00  ← NEW
```

**After Append to Raw:**
```
Raw.orders (all 8 rows)
order_id | product      | qty | created_at
---------|--------------|-----|-------------------
1        | Widget A     | 10  | 2025-11-01 10:00
2        | Widget B     | 5   | 2025-11-01 11:00
3        | Widget A     | 2   | 2025-11-01 12:00
1        | Widget A     | 10  | 2025-11-01 10:00  ← DUPLICATE (OK)
2        | Widget B     | 5   | 2025-11-01 11:00  ← DUPLICATE (OK)
3        | Widget A     | 2   | 2025-11-01 12:00  ← DUPLICATE (OK)
4        | Widget C     | 8   | 2025-11-02 09:00
5        | Widget B     | 12  | 2025-11-02 10:00
```

**Note:** Duplicates in Raw are OK. Silver's merge will deduplicate them.

---

## Odibi YAML

### Using High Water Mark (Incremental)

```yaml
pipelines:
  - pipeline: sql_to_raw
    layer: bronze
    nodes:
      - id: load_orders_raw
        name: "Load Orders to Raw (Bronze)"
        read:
          connection: sql_prod
          format: sql
          table: dbo.orders
          options:
            query: |
              SELECT * FROM dbo.orders
              WHERE COALESCE(updated_at, created_at) > (
                SELECT COALESCE(MAX(COALESCE(updated_at, created_at)), '1900-01-01')
                FROM raw.orders
              )
        write:
          connection: adls_prod
          format: delta
          table: raw.orders
          mode: append
```

### Using Scheduled Batch (Full Rescan)

If your source doesn't have timestamps, rescan everything and append:

```yaml
      - id: load_orders_raw
        name: "Load Orders to Raw"
        read:
          connection: sql_prod
          format: sql
          table: dbo.orders
        write:
          connection: adls_prod
          format: delta
          table: raw.orders
          mode: append
```

**Warning:** This will append the entire table each run, creating duplicates. Use High Water Mark pattern when possible.

---

## Trade-Offs

### Advantages
✓ Complete audit trail (when did each row arrive?)  
✓ Safe replay/reconstruction if Silver breaks  
✓ Simple logic (no deduplication, no logic)  
✓ Idempotent (rerun is safe; creates duplicates but that's OK)  

### Disadvantages
✗ Storage grows unbounded (mitigated by cheap cloud storage)  
✗ Duplicates must be handled downstream (Silver's job)  
✗ Full rescans can be slow without timestamp filtering  

---

## When to Use

**Always** use Append-Only for Raw ingestion. No exceptions.

---

## When NOT to Use

Never overwrite Raw. Never delete from Raw. Never use `merge` in Raw.

---

## Related Patterns

- **[High Water Mark](./hwm_pattern_guide.md)** → Efficiently filter SQL sources for incremental reads
- **[Merge/Upsert](./merge_upsert.md)** → Deduplicate Raw data in Silver

---

## References

- [Odibi Architecture Manifesto: Pattern A - Ingestion](../design/02_architecture_manifesto.md#pattern-a-ingestion-landing--raw)
- [Raw is Sacred Principle](../design/02_architecture_manifesto.md#2-raw-is-sacred-the-time-machine-rule)
