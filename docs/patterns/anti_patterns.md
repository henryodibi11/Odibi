# ❌ Anti-Patterns: What NOT to Do

This guide documents common mistakes when building data pipelines. For each anti-pattern, we show what NOT to do, why it's bad, and the correct approach.

Learning what NOT to do is just as important as learning what TO do.

---

## Table of Contents

1. [Transforming in Bronze Layer](#1-transforming-in-bronze-layer)
2. [Not Deduplicating Before SCD2](#2-not-deduplicating-before-scd2)
3. [Using SCD2 for Fact Tables](#3-using-scd2-for-fact-tables)
4. [Hardcoding Paths Instead of Connections](#4-hardcoding-paths-instead-of-connections)
5. [Not Handling NULLs in Key Columns](#5-not-handling-nulls-in-key-columns)
6. [Mixing Business Logic Across Layers](#6-mixing-business-logic-across-layers)
7. [Skipping the Silver Layer](#7-skipping-the-silver-layer)
8. [Not Adding Extracted Timestamps in Bronze](#8-not-adding-extracted-timestamps-in-bronze)
9. [Using Append Mode Without Deduplication](#9-using-append-mode-without-deduplication)
10. [Ignoring Schema Evolution](#10-ignoring-schema-evolution)

---

## 1. Transforming in Bronze Layer

### ❌ What NOT to Do

```yaml
# BAD: Cleaning data in the Bronze layer
pipelines:
  - pipeline: "bronze_customers"
    layer: "bronze"
    nodes:
      - name: "ingest_customers"
        read:
          connection: landing
          path: customers.csv
        
        # ❌ DON'T transform in Bronze!
        transform:
          steps:
            - sql: "SELECT * FROM df WHERE status != 'inactive'"
            - function: "clean_text"
              params: { columns: ["name", "email"] }
        
        write:
          connection: bronze
          path: customers
          format: delta
```

### Why It's Bad

**You lose the original data forever.** 

Imagine this scenario:
1. You filter out "inactive" customers in Bronze
2. 6 months later, business says "We need to analyze inactive customers too"
3. You can't—because you threw that data away

Bronze is your "undo button." If you transform data there, you lose the ability to go back to the source.

### ✅ What to Do Instead

```yaml
# GOOD: Keep Bronze raw, transform in Silver
pipelines:
  # Step 1: Bronze - Store raw data exactly as received
  - pipeline: "bronze_customers"
    layer: "bronze"
    nodes:
      - name: "ingest_customers"
        read:
          connection: landing
          path: customers.csv
        
        # No transformations! Just land the data
        write:
          connection: bronze
          path: customers
          format: delta
          mode: append

  # Step 2: Silver - Now you can transform
  - pipeline: "silver_customers"
    layer: "silver"
    nodes:
      - name: "clean_customers"
        read:
          connection: bronze
          path: customers
        
        # ✅ Transform in Silver
        transform:
          steps:
            - sql: "SELECT * FROM df WHERE status != 'inactive'"
            - function: "clean_text"
              params: { columns: ["name", "email"] }
        
        write:
          connection: silver
          path: dim_customers
          format: delta
```

### 💡 The One Exception

You MAY add metadata columns in Bronze (they don't alter original data):

```yaml
# OK: Adding metadata in Bronze
transform:
  steps:
    - function: "derive_columns"
      params:
        columns:
          _extracted_at: "current_timestamp()"
          _source_file: "'customers.csv'"
```

---

## 2. Not Deduplicating Before SCD2

### ❌ What NOT to Do

```yaml
# BAD: Source has duplicates, feeding directly to SCD2
nodes:
  - name: "dim_customers"
    read:
      connection: bronze
      path: customers  # Contains duplicate customer_id rows!
    
    # ❌ SCD2 without deduplication
    transformer: "scd2"
    params:
      target: silver.dim_customers
      keys: ["customer_id"]
      track_cols: ["name", "email", "address"]
      effective_time_col: "updated_at"
    
    write:
      connection: silver
      table: dim_customers
      format: delta
      mode: overwrite
```

### Why It's Bad

**Your history table explodes with duplicate versions.**

If your source has:
```
customer_id | name  | updated_at
101         | Alice | 2024-01-01 10:00:00
101         | Alice | 2024-01-01 10:00:01  <- Duplicate from same extract
101         | Alice | 2024-01-01 10:00:02  <- Another duplicate
```

SCD2 sees three "changes" and creates three history rows, even though nothing actually changed:

```
customer_id | name  | effective_time          | end_time               | is_current
101         | Alice | 2024-01-01 10:00:00     | 2024-01-01 10:00:01    | false
101         | Alice | 2024-01-01 10:00:01     | 2024-01-01 10:00:02    | false  
101         | Alice | 2024-01-01 10:00:02     | NULL                   | true
```

Your dimension table grows 3x faster than it should, wasting storage and slowing queries.

### ✅ What to Do Instead

```yaml
# GOOD: Deduplicate first, then SCD2
nodes:
  - name: "dedup_customers"
    read:
      connection: bronze
      path: customers
    
    # ✅ Deduplicate first - keep most recent per customer
    transformer: "deduplicate"
    params:
      keys: ["customer_id"]
      order_by: "updated_at DESC"
    
    write:
      connection: staging
      path: customers_deduped

  - name: "dim_customers"
    depends_on: ["dedup_customers"]
    
    # ✅ Now SCD2 sees clean data
    transformer: "scd2"
    params:
      target: silver.dim_customers
      keys: ["customer_id"]
      track_cols: ["name", "email", "address"]
      effective_time_col: "updated_at"
    
    write:
      connection: silver
      table: dim_customers
      format: delta
      mode: overwrite
```

---

## 3. Using SCD2 for Fact Tables

### ❌ What NOT to Do

```yaml
# BAD: SCD2 on a fact table
nodes:
  - name: "fact_orders"
    read:
      connection: bronze
      path: orders
    
    # ❌ DON'T use SCD2 for facts!
    transformer: "scd2"
    params:
      target: silver.fact_orders
      keys: ["order_id"]
      track_cols: ["quantity", "total_amount"]
      effective_time_col: "order_date"
    
    write:
      connection: silver
      table: fact_orders
```

### Why It's Bad

**Facts don't change—they happen.**

An order is an event. Customer 101 placed order #5001 on January 15th for $99.00. That's a historical fact. It doesn't "change."

If the source shows a different amount for the same order, that's either:
1. A **correction** (handle with a correction fact, not by changing history)
2. A **data quality issue** (should be caught by validation)

Using SCD2 on facts:
- Bloats your table unnecessarily
- Creates confusing history for immutable events
- Slows down analytical queries

### ✅ What to Do Instead

For fact tables, use **append** mode (for new records) or **merge** mode (for late-arriving corrections):

```yaml
# GOOD: Append mode for facts
nodes:
  - name: "fact_orders"
    read:
      connection: bronze
      path: orders
    
    # ✅ Just append new orders
    write:
      connection: silver
      table: fact_orders
      format: delta
      mode: append

# OR: Merge mode if you expect corrections
nodes:
  - name: "fact_orders"
    read:
      connection: bronze
      path: orders
    
    # ✅ Merge handles late corrections
    transformer: "merge"
    params:
      target: silver.fact_orders
      keys: ["order_id"]
      # Updates existing, inserts new
    
    write:
      connection: silver
      table: fact_orders
      format: delta
      mode: overwrite
```

### 💡 When to Use Each

| Scenario | Pattern |
|----------|---------|
| New orders arriving daily | `append` |
| Orders may be corrected later | `merge` |
| Customer info changes over time | `scd2` |

---

## 4. Hardcoding Paths Instead of Connections

### ❌ What NOT to Do

```yaml
# BAD: Hardcoded paths everywhere
nodes:
  - name: "load_sales"
    read:
      # ❌ Hardcoded path - breaks when moving to prod
      path: "abfss://raw@devstorageaccount.dfs.core.windows.net/sales/2024/"
    
    write:
      # ❌ Another hardcoded path
      path: "abfss://bronze@devstorageaccount.dfs.core.windows.net/sales"
```

### Why It's Bad

**Your pipeline breaks when moving between environments.**

Development, staging, and production have different:
- Storage account names
- Credentials
- Base paths

If you hardcode paths, you need to edit the YAML for every environment. This leads to:
- Copy-paste errors
- Secrets accidentally committed to git
- "It works on my machine" syndrome

### ✅ What to Do Instead

```yaml
# GOOD: Use connections with environment variables
connections:
  landing:
    type: azure_blob
    account: ${STORAGE_ACCOUNT}  # From environment
    container: raw
    credential: ${STORAGE_KEY}   # Secret from Key Vault
  
  bronze:
    type: azure_blob
    account: ${STORAGE_ACCOUNT}
    container: bronze
    credential: ${STORAGE_KEY}

pipelines:
  - pipeline: "load_sales"
    nodes:
      - name: "ingest_sales"
        read:
          # ✅ Use connection name + relative path
          connection: landing
          path: sales/2024/
        
        write:
          # ✅ Portable across environments
          connection: bronze
          path: sales
```

Now the same YAML works in dev, staging, and prod—just change the environment variables.

---

## 5. Not Handling NULLs in Key Columns

### ❌ What NOT to Do

```yaml
# BAD: Joining on columns that might be NULL
nodes:
  - name: "enrich_orders"
    read:
      connection: silver
      path: fact_orders  # customer_id can be NULL!
    
    # ❌ Join without NULL handling
    transformer: "join"
    params:
      right: silver.dim_customer
      on: ["customer_id"]
      how: "left"
```

**Source data:**
```
order_id | customer_id | amount
1001     | 101         | 99.00
1002     | NULL        | 45.00   <- Guest checkout, no customer
1003     | 102         | 150.00
```

### Why It's Bad

**NULL never equals NULL in SQL.**

When you join on `customer_id`:
- `101 = 101` ✅ Match
- `NULL = NULL` ❌ No match! (NULL is "unknown", and unknown ≠ unknown)

Your orders with NULL customer_id get dropped or get incorrect dimension values.

### ✅ What to Do Instead

```yaml
# GOOD: Handle NULLs before joining
nodes:
  - name: "prep_orders"
    read:
      connection: silver
      path: fact_orders
    
    # ✅ Option 1: Fill NULLs with a placeholder that maps to "unknown" customer
    transform:
      steps:
        - function: "fill_nulls"
          params:
            columns: ["customer_id"]
            value: 0  # Maps to unknown member in dim_customer
    
    write:
      connection: staging
      path: orders_with_valid_keys

  - name: "enrich_orders"
    depends_on: ["prep_orders"]
    
    transformer: "join"
    params:
      right: silver.dim_customer  # Has customer_id=0 as unknown member
      on: ["customer_id"]
      how: "left"
```

Or use the **fact pattern** with `orphan_handling: unknown`:

```yaml
# GOOD: Use the fact pattern for automatic NULL handling
nodes:
  - name: "fact_orders"
    read:
      connection: silver
      path: orders_clean
    
    pattern:
      type: fact
      params:
        dimensions:
          - source_column: customer_id
            dimension_table: dim_customer
            dimension_key: customer_id
            surrogate_key: customer_sk
        # ✅ NULLs get SK=0 (unknown member)
        orphan_handling: unknown
```

---

## 6. Mixing Business Logic Across Layers

### ❌ What NOT to Do

```yaml
# BAD: Business logic scattered everywhere
pipelines:
  - pipeline: "bronze_sales"
    layer: "bronze"
    nodes:
      - name: "ingest_sales"
        transform:
          steps:
            # ❌ Business calculation in Bronze?!
            - sql: "SELECT *, quantity * unit_price * 0.92 as net_amount FROM df"
        write:
          connection: bronze
          path: sales

  - pipeline: "silver_sales"
    layer: "silver"
    nodes:
      - name: "clean_sales"
        transform:
          steps:
            # ❌ More business logic here
            - sql: "SELECT *, CASE WHEN net_amount > 1000 THEN 'high' ELSE 'low' END as tier FROM df"
        write:
          connection: silver
          path: sales

  - pipeline: "gold_sales"
    layer: "gold"
    nodes:
      - name: "report_sales"
        transform:
          steps:
            # ❌ And here too!
            - sql: "SELECT *, net_amount * 1.1 as projected_amount FROM df"
```

### Why It's Bad

**Debugging becomes a nightmare.**

When someone asks "Why is projected_amount $1,100?", you have to trace through:
1. Bronze: `quantity * unit_price * 0.92 = net_amount` (8% discount)
2. Silver: No change to amounts
3. Gold: `net_amount * 1.1 = projected_amount` (10% markup)

The business logic is hidden in three different places. Any change requires editing multiple pipelines.

### ✅ What to Do Instead

**Keep business logic in ONE place—Silver or Gold, not both.**

```yaml
# GOOD: Clear separation of concerns
pipelines:
  # Bronze: Raw data only
  - pipeline: "bronze_sales"
    layer: "bronze"
    nodes:
      - name: "ingest_sales"
        read:
          connection: landing
          path: sales.csv
        # ✅ No transformations in Bronze
        write:
          connection: bronze
          path: sales

  # Silver: Cleaning + business logic
  - pipeline: "silver_sales"
    layer: "silver"
    nodes:
      - name: "clean_sales"
        read:
          connection: bronze
          path: sales
        transform:
          steps:
            # ✅ All business calculations in ONE place
            - sql: |
                SELECT 
                  *,
                  quantity * unit_price as gross_amount,
                  quantity * unit_price * 0.92 as net_amount,
                  quantity * unit_price * 0.92 * 1.1 as projected_amount,
                  CASE WHEN quantity * unit_price * 0.92 > 1000 THEN 'high' ELSE 'low' END as tier
                FROM df
        write:
          connection: silver
          path: fact_sales

  # Gold: Aggregation only (no new business logic)
  - pipeline: "gold_sales"
    layer: "gold"
    nodes:
      - name: "daily_summary"
        read:
          connection: silver
          path: fact_sales
        # ✅ Gold just aggregates what Silver prepared
        pattern:
          type: aggregation
          params:
            grain: [sale_date, region]
            measures:
              - name: total_net
                expr: "SUM(net_amount)"
              - name: total_projected
                expr: "SUM(projected_amount)"
```

---

## 7. Skipping the Silver Layer

### ❌ What NOT to Do

```yaml
# BAD: Going directly from Bronze to Gold
pipelines:
  - pipeline: "bronze_orders"
    layer: "bronze"
    nodes:
      - name: "ingest_orders"
        read:
          connection: landing
          path: orders.csv
        write:
          connection: bronze
          path: orders

  - pipeline: "gold_summary"
    layer: "gold"
    nodes:
      - name: "daily_sales"
        read:
          connection: bronze
          path: orders  # ❌ Reading raw Bronze directly!
        
        # Trying to do EVERYTHING in one step
        transform:
          steps:
            - sql: "SELECT * FROM df WHERE order_id IS NOT NULL"
            - function: "deduplicate"
              params: { keys: ["order_id"] }
        
        pattern:
          type: aggregation
          params:
            grain: [order_date]
            measures:
              - name: total_sales
                expr: "SUM(amount)"
```

### Why It's Bad

**Every downstream consumer has to repeat the cleaning.**

If you have 5 Gold tables that all read from Bronze:
1. Each one cleans duplicates (same code x5)
2. Each one handles nulls (same code x5)
3. Each one applies business rules (same code x5)

Any cleaning bug must be fixed in 5 places. And if different teams make slightly different cleaning decisions, your reports don't match.

### ✅ What to Do Instead

```yaml
# GOOD: Silver is your "single source of truth"
pipelines:
  - pipeline: "bronze_orders"
    layer: "bronze"
    nodes:
      - name: "ingest_orders"
        read:
          connection: landing
          path: orders.csv
        write:
          connection: bronze
          path: orders

  # ✅ Silver: Clean ONCE, use EVERYWHERE
  - pipeline: "silver_orders"
    layer: "silver"
    nodes:
      - name: "clean_orders"
        read:
          connection: bronze
          path: orders
        
        # All cleaning happens here, once
        transform:
          steps:
            - sql: "SELECT * FROM df WHERE order_id IS NOT NULL"
            - function: "deduplicate"
              params: { keys: ["order_id"] }
        
        validation:
          contracts:
            - type: not_null
              columns: [order_id, customer_id, amount]
        
        write:
          connection: silver
          path: fact_orders

  # ✅ Gold: Just aggregate clean data
  - pipeline: "gold_daily"
    layer: "gold"
    nodes:
      - name: "daily_sales"
        read:
          connection: silver
          path: fact_orders  # ✅ Reading from Silver
        
        pattern:
          type: aggregation
          params:
            grain: [order_date]
            measures:
              - name: total_sales
                expr: "SUM(amount)"

  # ✅ Another Gold table reads the same Silver
  - pipeline: "gold_regional"
    layer: "gold"
    nodes:
      - name: "regional_sales"
        read:
          connection: silver
          path: fact_orders  # ✅ Same Silver source
        
        pattern:
          type: aggregation
          params:
            grain: [region, month]
            measures:
              - name: total_sales
                expr: "SUM(amount)"
```

---

## 8. Not Adding Extracted Timestamps in Bronze

### ❌ What NOT to Do

```yaml
# BAD: No extraction timestamp
nodes:
  - name: "ingest_sales"
    read:
      connection: landing
      path: sales/
    
    # ❌ No metadata about when this was loaded
    write:
      connection: bronze
      path: sales
      mode: append
```

### Why It's Bad

**You can't debug timing issues.**

Scenario: Data looks wrong for January 15th.

Questions you can't answer:
- When was the January 15th data loaded?
- Was it loaded multiple times?
- Did the source file change between loads?

Without timestamps, your Bronze table is just a pile of data with no history of how it got there.

### ✅ What to Do Instead

```yaml
# GOOD: Add extraction metadata
nodes:
  - name: "ingest_sales"
    read:
      connection: landing
      path: sales/
    
    # ✅ Add metadata columns
    transform:
      steps:
        - function: "derive_columns"
          params:
            columns:
              _extracted_at: "current_timestamp()"
              _source_file: "input_file_name()"
              _batch_id: "'${BATCH_ID}'"  # From orchestrator
    
    write:
      connection: bronze
      path: sales
      mode: append
```

Now your Bronze data includes:

```
order_id | amount | _extracted_at       | _source_file        | _batch_id
1001     | 99.00  | 2024-01-16 06:00:00 | sales_20240115.csv  | batch_42
1002     | 45.00  | 2024-01-16 06:00:00 | sales_20240115.csv  | batch_42
1001     | 99.00  | 2024-01-16 18:00:00 | sales_20240115.csv  | batch_43  <- Aha! Loaded twice!
```

---

## 9. Using Append Mode Without Deduplication

### ❌ What NOT to Do

```yaml
# BAD: Append mode on a table that gets reprocessed
nodes:
  - name: "load_daily_sales"
    read:
      connection: landing
      path: sales/${YESTERDAY}/  # Same file every rerun
    
    # ❌ Append without deduplication
    write:
      connection: bronze
      path: sales
      mode: append
```

### Why It's Bad

**Re-running the pipeline doubles your data.**

- First run: 1,000 rows appended ✅
- Pipeline fails later, you rerun from scratch
- Second run: Same 1,000 rows appended again ❌
- Now you have 2,000 rows (1,000 duplicates)

Your aggregations now show 2x the real sales.

### ✅ What to Do Instead

**Option 1: Use merge mode for idempotent writes**

```yaml
# GOOD: Merge mode is idempotent
nodes:
  - name: "load_daily_sales"
    read:
      connection: landing
      path: sales/${YESTERDAY}/
    
    # ✅ Merge inserts new, updates existing
    transformer: "merge"
    params:
      target: bronze.sales
      keys: ["order_id"]
    
    write:
      connection: bronze
      table: sales
      format: delta
      mode: overwrite
```

**Option 2: Deduplicate in Silver**

```yaml
# GOOD: Bronze appends, Silver deduplicates
nodes:
  - name: "load_sales_bronze"
    write:
      connection: bronze
      path: sales
      mode: append  # OK because Silver deduplicates

  - name: "clean_sales_silver"
    read:
      connection: bronze
      path: sales
    
    # ✅ Deduplicate the appended data
    transformer: "deduplicate"
    params:
      keys: ["order_id"]
      order_by: "_extracted_at DESC"  # Keep most recent if duplicated
    
    write:
      connection: silver
      path: fact_sales
      mode: overwrite  # Full refresh
```

---

## 10. Ignoring Schema Evolution

### ❌ What NOT to Do

```yaml
# BAD: No schema handling
nodes:
  - name: "load_api_data"
    read:
      connection: api
      endpoint: /customers
    
    # ❌ Just writing whatever comes from the API
    write:
      connection: bronze
      path: customers
      format: delta
      # No schema_mode specified
```

**What happens when the API adds a new field:**

```
Day 1: {"id": 1, "name": "Alice"}
Day 2: {"id": 2, "name": "Bob", "loyalty_points": 500}  <- New field!
```

Pipeline fails with:
```
SchemaEvolutionException: Found new column 'loyalty_points' 
not present in the target schema
```

### Why It's Bad

**APIs and source systems change without warning.**

Vendors add fields, rename columns, or change types. Without explicit schema handling, your pipeline breaks whenever this happens—usually at 3 AM on a weekend.

### ✅ What to Do Instead

```yaml
# GOOD: Explicit schema evolution handling
nodes:
  - name: "load_api_data"
    read:
      connection: api
      endpoint: /customers
    
    write:
      connection: bronze
      path: customers
      format: delta
      # ✅ Allow schema to grow
      delta_options:
        mergeSchema: true

    # ✅ Or use schema_policy for fine control
    schema_policy:
      on_new_column: add       # Add new columns automatically
      on_missing_column: warn  # Log warning but continue
      on_type_mismatch: error  # Fail on type changes (dangerous!)
```

For production, consider **schema contracts**:

```yaml
# GOOD: Schema contract catches unexpected changes
contracts:
  - type: schema
    expected:
      - column: id
        type: integer
        nullable: false
      - column: name
        type: string
        nullable: false
      - column: loyalty_points
        type: integer
        nullable: true  # We know about this field
    on_extra_columns: warn  # New fields trigger warning, not failure
```

---

## Quick Reference: Anti-Patterns Cheat Sheet

| Anti-Pattern | Why It's Bad | Do This Instead |
|--------------|--------------|-----------------|
| Transforming in Bronze | Lose original data | Keep Bronze raw, transform in Silver |
| No dedup before SCD2 | History table explodes | Deduplicate first, then SCD2 |
| SCD2 on fact tables | Facts are immutable events | Use append or merge |
| Hardcoded paths | Breaks across environments | Use connections + env vars |
| NULLs in join keys | NULL ≠ NULL, rows get dropped | Handle NULLs with placeholders |
| Business logic everywhere | Can't debug, inconsistent | Centralize in Silver |
| Bronze → Gold directly | Cleaning repeated everywhere | Use Silver as single source of truth |
| No _extracted_at | Can't debug timing | Add metadata columns |
| Append without dedup | Reruns create duplicates | Use merge or deduplicate downstream |
| Ignore schema changes | Pipeline breaks on changes | Use mergeSchema or schema_policy |

---

## Next Steps

- [SCD2 Troubleshooting](./scd2.md#common-errors-and-debugging)
- [Validation Patterns](./validation.md)
- [Best Practices Guide](../guides/best_practices.md)
- [Troubleshooting Guide](../troubleshooting.md)
