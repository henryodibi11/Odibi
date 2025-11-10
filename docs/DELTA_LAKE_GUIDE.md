# Delta Lake Quick Reference Guide

**Version:** v1.2.0-alpha.2-phase2b  
**Last Updated:** November 9, 2025

---

## Overview

Delta Lake provides ACID transactions, time travel, and schema evolution for data lakes. ODIBI supports Delta Lake in both PandasEngine and SparkEngine.

---

## Installation

```bash
# For PandasEngine Delta support
pip install "odibi[pandas]"

# For SparkEngine Delta support  
pip install "odibi[spark]"

# For both + Azure
pip install "odibi[pandas,spark,azure]"
```

---

## Quick Start

### Write Delta Table

```python
from odibi.engine.pandas_engine import PandasEngine
from odibi.connections.local import LocalConnection

engine = PandasEngine()
conn = LocalConnection(base_path="./data")

# Write Delta table
engine.write(
    df,
    connection=conn,
    format="delta",
    path="sales.delta",
    mode="append"
)
```

### Read Delta Table

```python
# Read latest version
df = engine.read(
    connection=conn,
    format="delta",
    path="sales.delta"
)
```

---

## Time Travel

### Read Specific Version

```python
# Read version 5
df_v5 = engine.read(
    connection=conn,
    format="delta",
    path="sales.delta",
    options={"versionAsOf": 5}
)
```

### Use Case: Compare Versions

```python
# Latest version
df_latest = engine.read(conn, format="delta", path="sales.delta")

# Yesterday's version
df_yesterday = engine.read(
    conn,
    format="delta",
    path="sales.delta",
    options={"versionAsOf": 10}
)

# Find changes
changes = len(df_latest) - len(df_yesterday)
print(f"Rows changed: {changes}")
```

---

## History Tracking

### Get Table History

```python
# Get all history
history = engine.get_delta_history(
    connection=conn,
    path="sales.delta"
)

for entry in history:
    print(f"Version {entry['version']}: {entry['operation']}")
    print(f"  Timestamp: {entry['timestamp']}")
```

### Get Recent History

```python
# Last 10 versions only
recent = engine.get_delta_history(
    connection=conn,
    path="sales.delta",
    limit=10
)
```

---

## Restore Operations

### Restore to Previous Version

```python
# Restore to version 5
engine.restore_delta(
    connection=conn,
    path="sales.delta",
    version=5
)

print("✅ Restored to version 5")
```

### Use Case: Undo Bad Write

```python
# Oops! Wrote bad data
engine.write(bad_df, conn, format="delta", path="sales.delta", mode="overwrite")

# Check history to find good version
history = engine.get_delta_history(conn, "sales.delta", limit=5)
good_version = history[1]['version']  # Version before bad write

# Restore
engine.restore_delta(conn, "sales.delta", version=good_version)
print("✅ Bad data undone!")
```

---

## VACUUM Operations

### Clean Old Files

```python
# Delete files older than 7 days
result = engine.vacuum_delta(
    connection=conn,
    path="sales.delta",
    retention_hours=168  # 7 days
)

print(f"Deleted {result['files_deleted']} files")
```

### Dry Run (Preview)

```python
# See what would be deleted (don't delete yet)
result = engine.vacuum_delta(
    connection=conn,
    path="sales.delta",
    retention_hours=168,
    dry_run=True  # ← Preview mode
)

print(f"Would delete {result['files_deleted']} files")
```

### Recommended Schedule

```python
# Run weekly in production
def weekly_vacuum():
    tables = ["sales.delta", "events.delta", "users.delta"]

    for table in tables:
        result = engine.vacuum_delta(
            conn,
            table,
            retention_hours=168  # Keep last 7 days for time travel
        )
        print(f"{table}: deleted {result['files_deleted']} files")
```

---

## Partitioning

### Write Partitioned Table

```python
import warnings

# Partitioning emits a warning
with warnings.catch_warnings():
    warnings.simplefilter("always")

    engine.write(
        df,
        connection=conn,
        format="delta",
        path="sales.delta",
        mode="overwrite",
        options={"partition_by": ["year", "month"]}
    )
```

### Good Partitioning Examples

```python
# ✅ Good: Low-cardinality columns
partition_by = ["year", "month"]        # ~12 values per year
partition_by = ["country"]              # ~200 values
partition_by = ["event_type"]           # ~10 values

# ❌ Bad: High-cardinality columns
partition_by = ["user_id"]              # Millions of values
partition_by = ["timestamp"]            # Infinite values
partition_by = ["transaction_id"]       # Millions of values
```

### Partitioning Guidelines

**When to Partition:**
- Column has < 1000 unique values
- Each partition will have > 1000 rows
- You frequently filter by that column

**When NOT to Partition:**
- Small datasets (< 100K rows)
- High-cardinality columns
- Rarely filter by that column

---

## Delta + ADLS Integration

### Setup ADLS Connection

```python
from odibi.connections.azure_adls import AzureADLS

# Key Vault authentication (recommended)
adls_conn = AzureADLS(
    account="myaccount",
    container="bronze",
    path_prefix="raw",
    auth_mode="key_vault",
    key_vault_name="my-kv",
    secret_name="storage-key"
)
```

### Write Delta to ADLS

```python
# Same API as local!
engine.write(
    df,
    connection=adls_conn,
    format="delta",
    path="sales.delta",
    mode="append"
)

# Location: abfss://bronze@myaccount.dfs.core.windows.net/raw/sales.delta
```

### Read Delta from ADLS

```python
# Read from cloud
df = engine.read(
    connection=adls_conn,
    format="delta",
    path="sales.delta"
)

# Time travel works too!
df_v5 = engine.read(
    connection=adls_conn,
    format="delta",
    path="sales.delta",
    options={"versionAsOf": 5}
)
```

---

## SparkEngine Support

### Initialize with Delta

```python
from odibi.engine.spark_engine import SparkEngine

# Auto-configures Delta Lake if delta-spark is installed
spark_engine = SparkEngine(connections={"local": conn})

print(f"Spark version: {spark_engine.spark.version}")
```

### Write Delta with Spark

```python
# Create Spark DataFrame
spark_df = spark_engine.spark.createDataFrame(pandas_df)

# Write Delta
spark_engine.write(
    spark_df,
    connection=conn,
    format="delta",
    path="sales.delta",
    mode="overwrite"
)
```

### Read Delta with Spark

```python
# Read Delta
spark_df = spark_engine.read(
    connection=conn,
    format="delta",
    path="sales.delta"
)

spark_df.show()
```

---

## YAML Configuration

### Basic Delta Pipeline

```yaml
pipelines:
  - pipeline: etl
    nodes:
      - name: read_csv
        read:
          connection: bronze
          path: raw/sales.csv
          format: csv

      - name: write_delta
        depends_on: [read_csv]
        write:
          connection: silver
          path: clean/sales.delta
          format: delta  # ← Use Delta!
          mode: append
```

### Delta with Time Travel

```yaml
pipelines:
  - pipeline: compare_versions
    nodes:
      # Read latest
      - name: read_latest
        read:
          connection: silver
          path: sales.delta
          format: delta

      # Read version 0
      - name: read_v0
        read:
          connection: silver
          path: sales.delta
          format: delta
          options:
            versionAsOf: 0  # ← Time travel!
```

### Delta with Partitioning

```yaml
pipelines:
  - pipeline: partitioned_etl
    nodes:
      - name: write_partitioned
        write:
          connection: silver
          path: events.delta
          format: delta
          mode: append
          options:
            partition_by:
              - year
              - month
```

---

## Best Practices

### 1. Use Delta for Production Data

```python
# ❌ Bad: CSV for production
engine.write(df, conn, format="csv", path="sales.csv")

# ✅ Good: Delta for production
engine.write(df, conn, format="delta", path="sales.delta")
```

**Why Delta?**
- ACID transactions (no partial writes)
- Schema evolution (add columns safely)
- Time travel (audit trail)
- Better compression (same as Parquet)

### 2. Run VACUUM Regularly

```python
# Weekly VACUUM job
def vacuum_all_tables():
    tables = get_all_delta_tables()

    for table in tables:
        result = engine.vacuum_delta(
            conn,
            table,
            retention_hours=168  # 7 days
        )
        log(f"{table}: cleaned {result['files_deleted']} files")
```

### 3. Use Time Travel for Debugging

```python
# Bug found - what changed?
df_now = engine.read(conn, format="delta", path="sales.delta")
df_before = engine.read(
    conn,
    format="delta",
    path="sales.delta",
    options={"versionAsOf": 10}  # Before the bug
)

# Find differences
diff = df_now.compare(df_before)
print("Changes that introduced bug:")
print(diff)
```

### 4. Document Partition Strategy

```python
# Good: Document why you're partitioning
"""
Partitioned by (year, month) because:
- Low cardinality: ~12 values per year
- Each partition has ~10K rows (> 1000 minimum)
- 90% of queries filter by month
- Total partitions: ~24 (< 1000 maximum)
"""
engine.write(
    df,
    conn,
    format="delta",
    path="sales.delta",
    options={"partition_by": ["year", "month"]}
)
```

### 5. Test Restore Procedure

```python
# Practice disaster recovery
def test_restore():
    # Take snapshot
    original = engine.read(conn, format="delta", path="sales.delta")

    # Simulate corruption
    engine.write(bad_df, conn, format="delta", path="sales.delta", mode="overwrite")

    # Get version before corruption
    history = engine.get_delta_history(conn, "sales.delta")
    good_version = history[1]['version']

    # Restore
    engine.restore_delta(conn, "sales.delta", version=good_version)

    # Verify
    restored = engine.read(conn, format="delta", path="sales.delta")
    assert len(restored) == len(original)
    print("✅ Restore test passed")
```

---

## Troubleshooting

### Issue: Import Error

```
ImportError: Delta Lake support requires 'pip install odibi[pandas]'
```

**Solution:**
```bash
pip install "odibi[pandas]"  # For Pandas
pip install "odibi[spark]"   # For Spark
```

### Issue: Cannot Time Travel After VACUUM

```
Error: Version 5 not found
```

**Cause:** VACUUM deleted old versions

**Solution:** Increase retention period
```python
# Keep versions longer
engine.vacuum_delta(
    conn,
    "sales.delta",
    retention_hours=336  # 14 days instead of 7
)
```

### Issue: Too Many Small Files

**Symptoms:**
- Slow reads
- Many partitions

**Solution:** Don't partition, or use fewer partition columns
```python
# ❌ Bad: Too many partitions
partition_by = ["year", "month", "day", "hour"]  # 1000s of partitions

# ✅ Good: Fewer partitions
partition_by = ["year", "month"]  # ~24 partitions
```

---

## Performance Tips

### 1. Batch Writes

```python
# ❌ Bad: Many small writes
for batch in small_batches:
    engine.write(batch, conn, format="delta", path="sales.delta", mode="append")

# ✅ Good: One large write
large_df = pd.concat(small_batches)
engine.write(large_df, conn, format="delta", path="sales.delta", mode="append")
```

### 2. Use Spark for Large Data

```python
# For data > 1GB, use SparkEngine
if df.memory_usage(deep=True).sum() > 1e9:  # > 1GB
    spark_engine = SparkEngine(connections={"local": conn})
    spark_df = spark_engine.spark.createDataFrame(df)
    spark_engine.write(spark_df, conn, format="delta", path="sales.delta")
else:
    # PandasEngine fine for smaller data
    engine.write(df, conn, format="delta", path="sales.delta")
```

### 3. Limit History Calls

```python
# ❌ Bad: Get all history every time
history = engine.get_delta_history(conn, "sales.delta")

# ✅ Good: Only get what you need
recent = engine.get_delta_history(conn, "sales.delta", limit=10)
```

---

## Next Steps

1. **Try the Walkthrough:** `walkthroughs/phase2b_delta_lake.ipynb`
2. **See Examples:** `examples/example_delta_pipeline.yaml`
3. **Read Design Docs:** `docs/PHASE2_DESIGN_DECISIONS.md`
4. **Check Changelog:** `CHANGELOG.md`

---

**Phase 2B Complete!** ✅  
Ready for production Delta Lake data engineering.
