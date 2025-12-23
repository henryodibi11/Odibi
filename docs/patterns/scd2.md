# SCD Type 2 (Slowly Changing Dimensions)

The **SCD Type 2** pattern allows you to track the full history of changes for a record over time. Unlike a simple update (which overwrites the old value), SCD2 keeps the old version and adds a new version, managing effective dates for you.

## The "Time Machine" Concept

**Business Problem:**
"I need to know what the customer's address was *last month*, not just where they live now."

**The Solution:**
Each record has an "effective window" (`effective_time` to `end_time`) and a flag (`is_current`) indicating if it is the latest version.

### Visual Example

**Input (Source Update):**
*Customer 101 moved to NY on Feb 1st.*

| customer_id | address | tier | txn_date   |
|-------------|---------|------|------------|
| 101         | NY      | Gold | 2024-02-01 |

**Target Table (Before):**
*Customer 101 lived in CA since Jan 1st.*

| customer_id | address | tier | txn_date   | valid_to | is_active |
|-------------|---------|------|------------|----------|-----------|
| 101         | CA      | Gold | 2024-01-01 | NULL     | true      |

**Target Table (After SCD2):**
*Old record CLOSED (valid_to set). New record OPEN (is_active=true).*

| customer_id | address | tier | txn_date   | valid_to   | is_active |
|-------------|---------|------|------------|------------|-----------|
| 101         | CA      | Gold | 2024-01-01 | 2024-02-01 | false     |
| 101         | NY      | Gold | 2024-02-01 | NULL       | true      |

---

## Configuration

Use the `scd2` transformer in your pipeline node.

### Option 1: Using Table Name

```yaml
nodes:
  - name: "dim_customers"
    # ... (read from source) ...

    transformer: "scd2"
    params:
      target: "silver.dim_customers"   # Registered table name
      keys: ["customer_id"]            # Unique ID
      track_cols: ["address", "tier"]  # Changes here trigger a new version
      effective_time_col: "txn_date"   # When the change happened

    write:
      table: "silver.dim_customers"
      format: "delta"
      mode: "overwrite"                # Important: SCD2 returns FULL history
```

### Option 2: Using Connection + Path (ADLS)

```yaml
nodes:
  - name: "dim_customers"
    # ... (read from source) ...

    transformer: "scd2"
    params:
      connection: adls_prod            # Connection name
      path: OEE/silver/dim_customers   # Relative path
      keys: ["customer_id"]
      track_cols: ["address", "tier"]
      effective_time_col: "txn_date"

    write:
      connection: adls_prod
      path: OEE/silver/dim_customers   # Same location as target
      format: "delta"
      mode: "overwrite"
```

### Full Configuration

```yaml
transformer: "scd2"
params:
  target: "silver.dim_customers"       # OR use connection + path
  keys: ["customer_id"]
  track_cols: ["address", "tier", "email"]

  # Source column for start date
  effective_time_col: "updated_at"

  # Target columns to manage (optional defaults shown)
  end_time_col: "valid_to"
  current_flag_col: "is_current"
```

---

## How It Works

The `scd2` transformer performs a complex set of operations automatically:

1.  **Match**: Finds existing records in the `target` table using `keys`.
2.  **Compare**: Checks `track_cols` to see if any data has changed.
3.  **Close**: If a record changed, it updates the *old* record's `end_time_col` to equal the new record's `effective_time_col`, and sets `is_current = false`.
4.  **Insert**: It adds the *new* record with `effective_time_col` as the start date, `NULL` as the end date, and `is_current = true`.
5.  **Preserve**: It keeps all unchanged history records as they are.

## Important Notes

*   **Write Mode**: You must use `mode: overwrite` for the write operation following this transformer. The transformer constructs the *complete* new state of the history table (including old closed records and new open records).
*   **Target Existence**: If the target table doesn't exist (first run), the transformer simply prepares the source data (adds valid_to/is_current columns) and returns it.
*   **Engine Support**: Works on both Spark (Delta Lake) and Pandas (Parquet/CSV).

## When to Use

*   **Dimension Tables**: Customer dimensions, Product dimensions where attributes change slowly over time.
*   **Audit Trails**: When you need exact historical state reconstruction.

## When NOT to Use

*   **Fact Tables**: Events (Transactions, Logs) are immutable; they don't change state, they just occur. Use `append` instead.
*   **Rapidly Changing Data**: If a record changes 100 times a day, SCD2 will explode your storage size. Use a snapshot or aggregate approach instead.
