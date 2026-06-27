# Tutorial: Delete Detection

> **"Records are deleted in our source, but our Silver tables still show them."**

This tutorial shows how to detect deleted records and flag them using odibi's
`detect_deletes` transformer — **CDC without CDC**.

---

## The Problem

Most enterprise sources don't provide Change Data Capture (CDC) feeds. When a
record is deleted from the source, your pipeline's next run simply doesn't see
it. The record stays in your Silver/Gold tables forever — silently stale.

**Delete detection** solves this by comparing what's in your target against
what's in the source. Records present in the target but missing from the source
are flagged as deleted.

---

## Two Modes

| Mode | How It Works | Best For |
|------|-------------|----------|
| `snapshot_diff` | Compares today's full extract with the previous Delta version | Full-snapshot ingestion (daily extracts) |
| `sql_compare` | LEFT ANTI JOIN Silver keys against a live SQL source | Incremental / HWM ingestion |

---

## Scenario

A customer management system where records appear and disappear:

| Day | Customers | What Happened |
|-----|-----------|---------------|
| Day 1 | 100 | Initial load |
| Day 2 | 95 | 5 customers deleted (IDs 12, 27, 45, 68, 91) |
| Day 3 | 97 | 2 came back (IDs 12, 45) + 3 new (IDs 101, 102, 103) |

---

## Mode 1: Snapshot Diff

Use when your source provides **full extracts** (every row, every run).

### How It Works

1. Day 1: Pipeline writes 100 rows → Delta table version 0
2. Day 2: Source sends 95 rows. Transformer compares version 0 keys vs
   incoming keys. 5 keys are in version 0 but not in today's extract →
   flagged as `_is_deleted = True`
3. Day 2 output: 95 active rows + 5 soft-deleted rows = 100 total
4. Day 3: Source sends 97 rows. 2 previously deleted keys reappear →
   `_is_deleted` flips back to `False`

### YAML Configuration

```yaml
transform:
  steps:
    - function: detect_deletes
      params:
        mode: snapshot_diff
        keys: [customer_id]
        connection: silver          # Connection to target Delta table
        path: customers             # Path within that connection
        soft_delete_col: _is_deleted
        on_first_run: skip          # skip | error
        max_delete_percent: 50.0    # Safety: warn if >50% deleted
```

### Key Fields

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `mode` | Yes | `none` | `snapshot_diff` or `sql_compare` |
| `keys` | Yes | — | Business key columns for comparison |
| `connection` | snapshot_diff | — | Connection name to target Delta table |
| `path` | snapshot_diff | — | Path to target Delta table |
| `soft_delete_col` | No | `_is_deleted` | Column name for the delete flag. Set to `null` for hard delete |
| `on_first_run` | No | `skip` | What to do when no previous version exists |
| `max_delete_percent` | No | `50.0` | Safety threshold (0–100) |
| `on_threshold_breach` | No | `warn` | `warn`, `error`, or `skip` |

---

## Mode 2: SQL Compare

Use when your target is loaded **incrementally** (HWM/append) and you need to
check a live source to find what's been deleted.

### How It Works

1. The transformer reads distinct keys from the Silver table (context.df)
2. Queries the live SQL source for current keys
3. Keys in Silver but not in the source → flagged as deleted
4. No Delta time travel needed — works with any target format

### YAML Configuration

```yaml
transform:
  steps:
    - function: detect_deletes
      params:
        mode: sql_compare
        keys: [customer_id]
        source_connection: source_db    # Live SQL connection
        source_table: dbo.Customers     # Or use source_query
        soft_delete_col: _is_deleted
        max_delete_percent: 20.0
        on_threshold_breach: error      # Strict: abort on mass delete
```

### SQL Compare Fields

| Field | Required | Description |
|-------|----------|-------------|
| `source_connection` | Yes | Connection name to the live SQL source |
| `source_table` | Yes* | Table to query for current keys |
| `source_query` | No | Custom SQL (overrides `source_table`) |

*Either `source_table` or `source_query` is required.

---

## Soft Delete vs Hard Delete

### Soft Delete (default)

Adds a boolean column (`_is_deleted`) to flag deleted rows. The rows remain
in the target for audit and historical queries.

```yaml
soft_delete_col: _is_deleted    # default
```

Result:

| customer_id | name | _is_deleted |
|-------------|------|-------------|
| 1 | Alice | False |
| 12 | Bob | True |

### Hard Delete

Set `soft_delete_col` to `null` to physically remove deleted rows.

```yaml
soft_delete_col: null    # removes rows, no flag column
```

**Warning:** Hard deletes are irreversible. Use soft delete for audit trails.

---

## Handling Reappearing Records

When a previously deleted record reappears in the source:

- **snapshot_diff:** The key is in today's extract, so `_is_deleted` is `False`.
  The deleted row from the previous version is not unioned because the key
  now exists in the current data.
- **sql_compare:** The key exists in the live source, so it's not flagged.
  `_is_deleted` stays `False`.

No special configuration needed — reappearance is handled automatically.

---

## Safety Threshold

The `max_delete_percent` guard prevents accidental mass deletions caused by
source outages or partial extracts.

```yaml
max_delete_percent: 20.0          # Flag if >20% of rows are deleted
on_threshold_breach: error        # error | warn | skip
```

| Action | Behavior |
|--------|----------|
| `warn` | Logs a warning, continues processing |
| `error` | Raises `DeleteThresholdExceeded`, aborts the node |
| `skip` | Silently skips delete detection for this run |

**Example:** Source had an outage and returned 0 rows. Without the threshold,
your pipeline would flag all 100 customers as deleted. With
`max_delete_percent: 50.0`, the pipeline stops and alerts you.

---

## Complete Pipeline Example (Snapshot Diff)

```yaml
project: customer_deletes
engine: pandas

connections:
  raw:
    type: local
    base_path: ./data
  silver:
    type: local
    base_path: ./output/silver

story:
  connection: silver
  path: stories/

system:
  connection: silver
  path: _odibi_system

pipelines:
  - pipeline: customer_pipeline
    layer: silver
    nodes:
      - name: detect_customer_deletes
        read:
          connection: raw
          path: day2.csv
          format: csv
        transform:
          steps:
            - function: detect_deletes
              params:
                mode: snapshot_diff
                keys: [customer_id]
                connection: silver
                path: customers
        write:
          connection: silver
          path: customers
          format: delta
          mode: overwrite
```

---

## Common Mistakes

### 1. Using snapshot_diff with incremental loading

snapshot_diff compares Delta versions. If you use HWM/append loading, the
"previous version" only has the last incremental batch — not the full table.
Use `sql_compare` instead.

### 2. Forgetting `connection` and `path` for snapshot_diff

Without these, the transformer can't find the Delta table to compare against.
It will silently skip detection and log a warning.

### 3. Setting max_delete_percent too low

A threshold of 5% with a 100-row table means 6 deletes trigger the safety
guard. Set thresholds based on your data's normal churn rate.

### 4. Using hard delete without a backup

`soft_delete_col: null` removes rows permanently. Always use soft delete
during development and testing.

---

## Quick Reference

```
detect_deletes
├── mode: snapshot_diff
│   ├── keys: [customer_id]
│   ├── connection: silver           # Target Delta connection
│   ├── path: customers              # Target Delta path
│   ├── soft_delete_col: _is_deleted # null = hard delete
│   ├── on_first_run: skip           # skip | error
│   ├── max_delete_percent: 50.0     # Safety guard
│   └── on_threshold_breach: warn    # warn | error | skip
│
└── mode: sql_compare
    ├── keys: [customer_id]
    ├── source_connection: source_db  # Live SQL connection
    ├── source_table: dbo.Customers   # Or source_query
    ├── soft_delete_col: _is_deleted
    ├── max_delete_percent: 50.0
    └── on_threshold_breach: warn
```
