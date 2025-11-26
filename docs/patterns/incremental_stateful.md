# Stateful Incremental Loading

Stateful Incremental Loading is the "Auto-Pilot" mode for ingestion. Unlike **Smart Read (Rolling Window)** which blindly looks back X days, **Stateful Mode** remembers exactly where it left off.

It tracks the **High Water Mark (HWM)**—the maximum value of a column (e.g., `updated_at` or `id`) seen in the previous run—and only fetches records greater than that value.

## When to use this?
*   **CDC-like Ingestion**: You want to sync a large table and only get new rows.
*   **Exactness**: You don't want to guess a lookback window (e.g., "3 days just to be safe").
*   **Performance**: You want to query the absolute minimum data required.

## Configuration

Enable it by setting `mode: stateful` in the `incremental` block.

```yaml
- name: "ingest_orders"
  read:
    connection: "postgres_prod"
    format: "sql"
    table: "public.orders"

    incremental:
      mode: "stateful"              # Enable State Tracking
      key_column: "updated_at"      # Column to track (max value is saved)
      fallback_column: "created_at" # Optional: Use this if key_column is NULL
      watermark_lag: "30m"          # Safety buffer (overlaps the window)
      state_key: "orders_ingest"    # Optional: Custom ID for the state file

  write:
    connection: "bronze"
    format: "delta"
    table: "orders_bronze"
    mode: "append"
```

## How It Works

1.  **First Run (Bootstrap)**
    *   Odibi checks the state backend (Delta table or local JSON).
    *   No state found? $\rightarrow$ **Full Load** (`SELECT * FROM table`).
    *   After success, it saves `MAX(updated_at)` as the HWM.

2.  **Subsequent Runs (Incremental)**
    *   Odibi retrieves the last HWM (e.g., `2023-10-25 10:00:00`).
    *   It subtracts the `watermark_lag` (e.g., 30 mins) $\rightarrow$ `09:30:00`.
    *   Generates query: `SELECT * FROM table WHERE updated_at > '2023-10-25 09:30:00'`.
    *   After success, it updates the HWM with the *new* maximum from the fetched batch.

## Key Features

### 🌊 Watermark Lag
Data often arrives late or out of order. If you run your pipeline at 10:00, you might miss a record timestamped 09:59 that gets committed at 10:01.

The `watermark_lag` creates a safety overlap.
*   **Lag: "30m"** implies: "Give me everything since the last run, but re-read the last 30 minutes just in case."
*   This ensures **At-Least-Once** delivery.
*   **Note:** This causes duplicates in the Bronze layer. This is expected! Your Silver layer (Merge/Upsert) handles deduplication.

### 🛡️ State Backends
Odibi automatically chooses the best backend:
*   **Spark/Databricks**: Uses a Delta table (`odibi_meta.state`) to track HWMs. This is robust and supports concurrency.
*   **Pandas/Local**: Uses a local JSON file (`.odibi/state.json`).

### 🔄 Resets
To reset the state and force a full reload:
1.  Delete the target table/file.
2.  Clear the state entry (manually or via CLI - *CLI command coming soon*).

## Comparison: Rolling Window vs. Stateful

| Feature | Rolling Window (`smart_read`) | Stateful (`stateful`) |
|---|---|---|
| **Logic** | `NOW() - lookback` | `> Last HWM` |
| **State** | Stateless (Time-based) | Stateful (Persisted) |
| **Best For** | Reporting windows ("Last 30 days") | Ingestion / Replication ("Sync table") |
| **Complexity** | Low | Medium |
| **Safety** | Good (if lookback is large) | Excellent (Exact tracking) |

## Example: CDC Ingestion Pipeline

Here is a robust pattern for database replication:

```yaml
nodes:
  # 1. Ingest (Bronze) - Accumulates history with duplicates
  - name: "ingest_users"
    read:
      connection: "db_prod"
      table: "users"
      incremental:
        mode: "stateful"
        key_column: "updated_at"
        watermark_lag: "15m"
    write:
      connection: "lake"
      format: "delta"
      table: "bronze_users"
      mode: "append"

  # 2. Merge (Silver) - Deduplicates and keeps current state
  - name: "dim_users"
    depends_on: ["ingest_users"] # Reads ONLY the new batch
    transformer: "merge"
    params:
      keys: ["user_id"]
      order_by: "updated_at DESC"
    write:
      connection: "lake"
      format: "delta"
      table: "silver_users"
      mode: "upsert"
```
