# Pattern: Merge/Upsert (Raw → Silver)

**Status:** Core Pattern  
**Layer:** Silver (refined/cleaned)  
**Engine:** Spark (Delta) or Pandas  
**Strategy:** Merge (MERGE INTO in Spark)  
**Idempotent:** Yes (by key)  

---

## Problem

Raw contains duplicates and historical versions of records. You need:
- **One current version per key** (deduplication)
- **Audit columns** to track when each record was created/updated
- **Idempotency** (rerunning doesn't create duplicates or double-count)

How do you efficiently merge new/changed raw data into Silver while maintaining a clean current state?

## Solution

Use **Delta Lake's MERGE operation** to upsert records by key. Odibi provides the **Merge Transformer** to make this configuration-driven.

---

## How It Works

**MERGE Logic:**
1. Read a batch of Raw data (new/changed rows)
2. Join with Silver by key columns
3. **If matched:** Update the Silver row with the new data
4. **If not matched:** Insert the new row
5. Auto-inject audit columns (created_at, updated_at)

---

## Step-by-Step Example

### Scenario: Orders Table

**Raw (after 2 runs; has duplicates):**
```
order_id | product      | qty | created_at          | updated_at
---------|--------------|-----|---------------------|-------------------
1        | Widget A     | 10  | 2025-11-01 10:00:00 | NULL
2        | Widget B     | 5   | 2025-11-01 11:00:00 | NULL
3        | Widget A     | 2   | 2025-11-01 12:00:00 | 2025-11-01 13:00:00
2        | Widget B     | 5   | 2025-11-01 11:00:00 | NULL              ← Duplicate
3        | Widget A     | 2   | 2025-11-01 12:00:00 | 2025-11-01 13:00:00 ← Duplicate
4        | Widget C     | 8   | 2025-11-02 09:00:00 | NULL
5        | Widget B     | 12  | 2025-11-02 10:00:00 | NULL
```

**Silver (before merge):**
```
order_id | product      | qty | created_at          | updated_at          | _created_ts        | _updated_ts
---------|--------------|-----|---------------------|---------------------|--------------------|-------------------
1        | Widget A     | 10  | 2025-11-01 10:00:00 | NULL                | 2025-11-01 10:30   | 2025-11-01 10:30
2        | Widget B     | 5   | 2025-11-01 11:00:00 | NULL                | 2025-11-01 11:30   | 2025-11-01 11:30
3        | Widget A     | 2   | 2025-11-01 12:00:00 | 2025-11-01 13:00:00 | 2025-11-01 12:30   | 2025-11-01 13:30
```

### Merge Operation

**New micro-batch from Raw (after dedup by timestamp):**
```
order_id | product      | qty | created_at          | updated_at
---------|--------------|-----|---------------------|-------------------
2        | Widget B     | 5   | 2025-11-01 11:00:00 | NULL              ← Duplicate, older timestamp
4        | Widget C     | 8   | 2025-11-02 09:00:00 | NULL              ← NEW
5        | Widget B     | 12  | 2025-11-02 10:00:00 | NULL              ← NEW
```

**Merge By Key (`order_id`):**

```
Row (order_id=2):
  - Matches Silver row (order_id=2)
  - Source timestamp: 2025-11-01 11:00
  - Silver timestamp: 2025-11-01 13:30
  - Source is older, skip? OR update anyway?
  → MERGE strategy: UPDATE (keep latest by source created_at/updated_at)
  → Actually: Insert latest version FIRST, then merge handles it
  → Result: Silver row 2 unchanged (already has latest)

Row (order_id=4):
  - No match in Silver
  - NEW row
  → INSERT into Silver
  → Set _created_ts = now(), _updated_ts = now()

Row (order_id=5):
  - No match in Silver
  - NEW row
  → INSERT into Silver
  → Set _created_ts = now(), _updated_ts = now()
```

**Silver (after merge):**
```
order_id | product      | qty | created_at          | updated_at          | _created_ts        | _updated_ts
---------|--------------|-----|---------------------|---------------------|--------------------|-------------------
1        | Widget A     | 10  | 2025-11-01 10:00:00 | NULL                | 2025-11-01 10:30   | 2025-11-01 10:30
2        | Widget B     | 5   | 2025-11-01 11:00:00 | NULL                | 2025-11-01 11:30   | 2025-11-01 11:30  (unchanged)
3        | Widget A     | 2   | 2025-11-01 12:00:00 | 2025-11-01 13:00:00 | 2025-11-01 12:30   | 2025-11-01 13:30  (unchanged)
4        | Widget C     | 8   | 2025-11-02 09:00:00 | NULL                | 2025-11-02 09:30   | 2025-11-02 09:30  ← NEW
5        | Widget B     | 12  | 2025-11-02 10:00:00 | NULL                | 2025-11-02 10:30   | 2025-11-02 10:30  ← NEW
```

**Result:**
- Silver has exactly 5 unique orders (1 per key)
- Duplicates from Raw are deduplicated
- Audit columns track when Odibi processed each row
- Idempotent: rerunning the same batch produces the same result

---

## Odibi YAML

### Minimal Config

```yaml
- id: merge_orders_silver
  name: "Merge Orders to Silver"
  read:
    connection: adls_prod
    format: delta
    table: raw.orders
  transformer: merge
  params:
    target: silver.orders
    keys: [order_id]
    strategy: upsert
```

### Full Config (with Audit Columns)

```yaml
- id: merge_orders_silver
  name: "Merge Orders: Raw → Silver"
  description: "Deduplicate and upsert orders from raw layer"
  depends_on: [load_orders_raw]
  read:
    connection: adls_prod
    format: delta
    table: raw.orders
  transformer: merge
  params:
    target: silver.orders
    keys: [order_id]
    strategy: upsert
    audit_cols:
      created_col: _created_at
      updated_col: _updated_at
  validation:
    not_empty: true
    schema:
      order_id:
        type: integer
        nullable: false
      product:
        type: string
        nullable: false
      qty:
        type: integer
        nullable: false
```

### Multi-Key Example (Composite Key)

```yaml
- id: merge_inventory_silver
  name: "Merge Inventory to Silver"
  read:
    connection: adls_prod
    format: delta
    table: raw.inventory
  transformer: merge
  params:
    target: silver.inventory
    keys: [store_id, material_id]  ← Composite key
    strategy: upsert
    audit_cols:
      created_col: created_ts
      updated_col: updated_ts
```

---

## Merge Transformer Behavior

### Spark (Delta)

Uses native `DeltaTable.merge()`:

```python
# Pseudo-code
delta_table = DeltaTable.forName("silver.orders")
delta_table.merge(
    source_df,
    condition="target.order_id = source.order_id"
) \
.whenMatchedUpdateAll() \
.whenNotMatchedInsertAll() \
.execute()

# Auto-inject audit columns:
# If insert: created_at = now(), updated_at = now()
# If update: updated_at = now(), created_at unchanged
```

### Pandas

Loads, merges, overwrites:

```python
# Pseudo-code
target = pd.read_parquet("silver/orders")
source = df  # Input DataFrame

# Merge indicator
merged = target.merge(source, on=['order_id'], how='outer', indicator=True)

# Apply logic:
# - Rows in target only: keep
# - Rows in source only: insert
# - Rows in both: update source values

# Overwrite
merged.to_parquet("silver/orders", mode="overwrite")
```

---

## Strategy Options

| Strategy | Behavior | Best For |
|----------|----------|----------|
| `upsert` | Insert new, update existing | Standard use case (Raw → Silver) |
| `append_only` | Insert new, ignore duplicates | Append-only tables (no updates) |
| `delete_match` | Delete matching rows | Tombstones, soft deletes |

---

## Audit Columns

### Auto-Injected Columns

When `audit_cols` is specified, Odibi adds two columns:

```python
# On INSERT
_created_at = CURRENT_TIMESTAMP()
_updated_at = CURRENT_TIMESTAMP()

# On UPDATE
_created_at = [unchanged]
_updated_at = CURRENT_TIMESTAMP()
```

This lets you track when Odibi processed each record, separate from the source's created/updated columns.

### Example

```yaml
audit_cols:
  created_col: _sys_created_ts
  updated_col: _sys_updated_ts
```

Result:
```
order_id | product | _sys_created_ts        | _sys_updated_ts
---------|---------|------------------------|-------------------
1        | Widget  | 2025-11-01 10:30:00    | 2025-11-01 10:30:00
2        | Gadget  | 2025-11-01 11:30:00    | 2025-11-02 14:45:00  ← Updated
```

---

## Trade-Offs

### Advantages
✓ Deduplicates Raw data automatically  
✓ Idempotent (safe to rerun)  
✓ Tracks data lineage (audit columns)  
✓ Handles both new and changed rows efficiently  
✓ Spark merge is fast (native Delta operation)  

### Disadvantages
✗ Requires primary key (what makes each row unique?)  
✗ Overwrites previous values (no history of all versions)  
✗ Pandas merge is slower than Spark (pandas mode not recommended for large tables)  

---

## Common Patterns

### Pattern: SCD Type 1 (Current State Only)

Keep only the latest version of each record. This is the default merge pattern.

```yaml
transformer: merge
params:
  target: silver.customers
  keys: [customer_id]
  strategy: upsert
```

### Pattern: SCD Type 2 (Full History)

Keep all historical versions with effective dates. **NOT** supported by standard merge. Use a separate `dim_customers` table with:
- `customer_id`
- `effective_from`, `effective_to`
- `is_current` flag

Then maintain it with a separate pipeline.

### Pattern: Append-Only (No Duplicates)

If your table should never have duplicates and you want to avoid updates:

```yaml
transformer: merge
params:
  target: silver.events
  keys: [event_id]
  strategy: append_only
```

This inserts new rows but ignores duplicates instead of updating.

### Pattern: Connection-Based Path with Table Registration

Use a connection to resolve ADLS paths and register the table in Unity Catalog:

```yaml
transform:
  steps:
    - sql_file: "sql/clean_orders.sql"
    - function: merge
      params:
        connection: adls_prod
        path: sales/silver/orders
        register_table: silver.orders
        keys: [order_id]
        strategy: upsert
        audit_cols:
          created_col: "_created_at"
          updated_col: "_updated_at"
```

This:
1. Resolves the full ADLS path via the `adls_prod` connection
2. Performs the merge operation
3. Registers the Delta table as `silver.orders` in the metastore

### Pattern: Post-Merge Optimization (Spark Only)

Run Delta Lake maintenance operations after the merge completes:

```yaml
transform:
  steps:
    - function: merge
      params:
        connection: adls_prod
        path: sales/silver/orders
        keys: [order_id]
        strategy: upsert
        optimize_write: true          # Run OPTIMIZE after merge
        zorder_by: [customer_id]      # Optional: Z-ORDER by columns
        vacuum_hours: 168             # Optional: VACUUM retaining 7 days
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `optimize_write` | bool | Run `OPTIMIZE` to compact small files (default: false) |
| `zorder_by` | list[str] | Columns to Z-ORDER by for faster filtered queries |
| `vacuum_hours` | int | Hours to retain for `VACUUM`. Set to 168 for 7 days. `None` disables VACUUM. |

**Note:** These operations run only on Spark with Delta Lake. They are skipped silently on Pandas.

---

## Debugging

### Check for Duplicates in Silver

```sql
SELECT order_id, COUNT(*) as count
FROM silver.orders
GROUP BY order_id
HAVING COUNT(*) > 1
```

If you see duplicates, your merge key is wrong.

### Check Merge History

```sql
DESCRIBE HISTORY silver.orders
```

Shows every merge operation, versions, and row counts.

---

## When to Use

- **Always** for Raw → Silver refinement
- Multiple sources merging into same table
- Need to track data lineage (audit columns)
- Want idempotent transformations

## When NOT to Use

- Audit tables (keep appending)
- SCD Type 2 (need version history)
- Data that should be immutable (use append instead)

---

## Related Patterns

- **[Append-Only Raw](./append_only_raw.md)** → Source layer (unmerged, duplicates OK)
- **[High Water Mark](./incremental_stateful.md)** → How to efficiently feed Raw with new data

---

## References

- [Databricks: Delta Lake MERGE](https://docs.databricks.com/en/sql/language-manual/delta-merge-into.html)
- [Fundamentals of Data Engineering: Chapter on SCD](https://www.fundamentalsofdataengineering.com/)
