# Delete Detection Implementation Design

> **Status:** Design Complete  
> **Target:** Silver layer CDC-like correctness for non-CDC sources

---

## 1. Overview

Delete detection identifies records that existed in a previous extraction but no longer exist, enabling CDC-like behavior for sources without native Change Data Capture.

```
Bronze (append-only) → Silver (dedupe + delete detection) → Gold (clean KPIs)
```

---

## 2. Delete Detection Modes

| Mode | How it works | Use when |
|------|--------------|----------|
| `none` | Pass-through (no detection) | Immutable facts (logs, events, sensors) |
| `snapshot_diff` | Compare Delta version N vs N-1 keys | Dimensions, staging tables, RPA sources |
| `sql_compare` | LEFT ANTI JOIN Silver keys against live source | SQL is authoritative & always reachable |

### Mode: `none` (Default)

No delete detection. Use for append-only facts.

```yaml
transform:
  steps:
    - operation: deduplicate
      params:
        keys: [event_id]
        order_by: _extracted_at DESC
    # detect_deletes omitted = none
```

### Mode: `snapshot_diff`

Compares current Delta version to previous version. Keys missing = deleted.

**Important:** This mode only works correctly with full snapshot extracts, not HWM incremental. Use `sql_compare` if you're doing HWM ingestion.

```yaml
transform:
  steps:
    - operation: deduplicate
      params:
        keys: [customer_id]
        order_by: _extracted_at DESC
    - operation: detect_deletes
      params:
        mode: snapshot_diff
        keys: [customer_id]
```

**Logic:**
```
prev_keys = Delta version N-1, SELECT DISTINCT keys
curr_keys = Delta version N, SELECT DISTINCT keys
deleted   = prev_keys EXCEPT curr_keys
→ Flag with _is_deleted = true (soft) or remove rows (hard)
```

**Implementation:** Uses Delta time travel (works in both Spark and Pandas via `deltalake` library).

### Mode: `sql_compare`

Queries live source to find keys that no longer exist. **Recommended for HWM ingestion** when source is authoritative and reachable.

```yaml
transform:
  steps:
    - operation: deduplicate
      params:
        keys: [customer_id]
        order_by: _extracted_at DESC
    - operation: detect_deletes
      params:
        mode: sql_compare
        keys: [customer_id]
        source_connection: azure_sql
        source_table: dbo.Customers
```

**Logic:**
```
silver_keys = SELECT DISTINCT keys FROM silver
source_keys = SELECT DISTINCT keys FROM live_source
deleted     = silver_keys EXCEPT source_keys
→ Flag with _is_deleted = true (soft) or remove rows (hard)
```

**Warning:** Never use `sql_compare` on staging tables - they may be empty between loads, causing false deletes.

---

## 3. Config Model

```python
from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field, model_validator


class DeleteDetectionMode(str, Enum):
    NONE = "none"
    SNAPSHOT_DIFF = "snapshot_diff"
    SQL_COMPARE = "sql_compare"


class DeleteDetectionConfig(BaseModel):
    """
    Configuration for delete detection in Silver layer.

    Example (snapshot_diff):
    ```yaml
    detect_deletes:
      mode: snapshot_diff
      keys: [customer_id]
      soft_delete_col: _is_deleted
    ```

    Example (sql_compare):
    ```yaml
    detect_deletes:
      mode: sql_compare
      keys: [customer_id]
      source_connection: azure_sql
      source_table: dbo.Customers
    ```
    """

    mode: DeleteDetectionMode = Field(
        default=DeleteDetectionMode.NONE,
        description="Delete detection strategy: none, snapshot_diff, sql_compare"
    )

    keys: List[str] = Field(
        default_factory=list,
        description="Business key columns for comparison"
    )

    # Soft vs Hard delete
    soft_delete_col: Optional[str] = Field(
        default="_is_deleted",
        description="Column to flag deletes. Set to null for hard-delete."
    )

    # sql_compare mode options
    source_connection: Optional[str] = Field(
        default=None,
        description="For sql_compare: connection name to query source"
    )
    source_table: Optional[str] = Field(
        default=None,
        description="For sql_compare: table to query"
    )
    source_query: Optional[str] = Field(
        default=None,
        description="For sql_compare: custom SQL query (overrides source_table)"
    )

    # Fallback for non-Delta sources (rare)
    snapshot_column: Optional[str] = Field(
        default=None,
        description="For snapshot_diff on non-Delta: column to identify snapshots. "
                    "If None, uses Delta version (default)."
    )

    # Edge case handling
    on_first_run: Literal["skip", "error"] = Field(
        default="skip",
        description="Behavior when no previous version exists for snapshot_diff"
    )

    max_delete_percent: Optional[float] = Field(
        default=50.0,
        description="Safety threshold: warn/error if more than X% of rows would be deleted"
    )

    on_threshold_breach: Literal["warn", "error", "skip"] = Field(
        default="warn",
        description="Behavior when delete percentage exceeds max_delete_percent"
    )

    @model_validator(mode="after")
    def validate_mode_requirements(self):
        if self.mode == DeleteDetectionMode.NONE:
            return self

        if not self.keys:
            raise ValueError(
                f"delete_detection: 'keys' required for mode='{self.mode}'"
            )

        if self.mode == DeleteDetectionMode.SQL_COMPARE:
            if not self.source_connection:
                raise ValueError(
                    "delete_detection: 'source_connection' required for sql_compare"
                )
            if not self.source_table and not self.source_query:
                raise ValueError(
                    "delete_detection: 'source_table' or 'source_query' required"
                )

        return self
```

---

## 4. Transformer Interface

```python
@transform("detect_deletes", category="transformer", param_model=DeleteDetectionConfig)
def detect_deletes(context: EngineContext, params: DeleteDetectionConfig) -> EngineContext:
    """
    Detects deleted records based on configured mode.

    Returns:
    - soft_delete_col set: Adds boolean column (True = deleted)
    - soft_delete_col = None: Removes deleted rows (hard delete)
    """
    if params.mode == DeleteDetectionMode.NONE:
        return context  # Pass-through

    if params.mode == DeleteDetectionMode.SNAPSHOT_DIFF:
        return _detect_deletes_snapshot_diff(context, params)

    if params.mode == DeleteDetectionMode.SQL_COMPARE:
        return _detect_deletes_sql_compare(context, params)

    raise ValueError(f"Unknown delete detection mode: {params.mode}")
```

### snapshot_diff Implementation

```python
def _detect_deletes_snapshot_diff(
    context: EngineContext,
    params: DeleteDetectionConfig
) -> EngineContext:
    """
    Compare current Delta version to previous version.
    Keys in previous but not in current = deleted.
    """
    keys = params.keys

    if context.engine_type == EngineType.SPARK:
        # Spark: Delta time travel
        current_version = context.delta_version
        prev_version = current_version - 1

        curr_keys = context.df.select(keys).distinct()
        prev_keys = (
            context.spark.read
            .format("delta")
            .option("versionAsOf", prev_version)
            .load(context.table_path)
            .select(keys)
            .distinct()
        )

        deleted_keys = prev_keys.exceptAll(curr_keys)

    else:
        # Pandas: deltalake time travel
        from deltalake import DeltaTable

        dt = DeltaTable(context.table_path)
        current_version = dt.version()
        prev_version = current_version - 1

        curr_keys = context.df[keys].drop_duplicates()
        prev_df = DeltaTable(context.table_path, version=prev_version).to_pandas()
        prev_keys = prev_df[keys].drop_duplicates()

        # Use merge with indicator to find deleted
        merged = prev_keys.merge(curr_keys, on=keys, how="left", indicator=True)
        deleted_keys = merged[merged["_merge"] == "left_only"][keys]

    return _apply_deletes(context, deleted_keys, params)


def _apply_deletes(
    context: EngineContext,
    deleted_keys,
    params: DeleteDetectionConfig
) -> EngineContext:
    """Apply soft or hard delete based on config."""
    keys = params.keys

    if params.soft_delete_col:
        # Soft delete: add flag column
        if context.engine_type == EngineType.SPARK:
            from pyspark.sql.functions import lit, col, when

            deleted_keys_flagged = deleted_keys.withColumn(
                params.soft_delete_col, lit(True)
            )
            result = context.df.join(
                deleted_keys_flagged,
                on=keys,
                how="left"
            ).withColumn(
                params.soft_delete_col,
                when(col(params.soft_delete_col).isNull(), False)
                .otherwise(True)
            )
        else:
            # Pandas
            df = context.df.copy()
            deleted_keys[params.soft_delete_col] = True
            df = df.merge(deleted_keys, on=keys, how="left")
            df[params.soft_delete_col] = df[params.soft_delete_col].fillna(False)
            result = df

        return context.with_df(result)

    else:
        # Hard delete: remove rows
        if context.engine_type == EngineType.SPARK:
            result = context.df.join(deleted_keys, on=keys, how="left_anti")
        else:
            df = context.df.copy()
            merged = df.merge(deleted_keys, on=keys, how="left", indicator=True)
            result = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])

        return context.with_df(result)
```

### sql_compare Implementation

```python
def _detect_deletes_sql_compare(
    context: EngineContext,
    params: DeleteDetectionConfig
) -> EngineContext:
    """
    Compare Silver keys against live source.
    Keys in Silver but not in source = deleted.
    """
    from odibi.connections import get_connection

    keys = params.keys
    conn = get_connection(params.source_connection)

    # Build source keys query
    if params.source_query:
        source_keys_query = params.source_query
    else:
        key_cols = ", ".join(keys)
        source_keys_query = f"SELECT DISTINCT {key_cols} FROM {params.source_table}"

    if context.engine_type == EngineType.SPARK:
        source_keys = (
            context.spark.read
            .format("jdbc")
            .option("url", conn.jdbc_url)
            .option("query", source_keys_query)
            .load()
        )
        silver_keys = context.df.select(keys).distinct()
        deleted_keys = silver_keys.exceptAll(source_keys)

    else:
        # Pandas
        import pandas as pd

        source_keys = pd.read_sql(source_keys_query, conn.engine)
        silver_keys = context.df[keys].drop_duplicates()

        merged = silver_keys.merge(source_keys, on=keys, how="left", indicator=True)
        deleted_keys = merged[merged["_merge"] == "left_only"][keys]

    return _apply_deletes(context, deleted_keys, params)
```

---

## 5. Engine Parity

Both Pandas and Spark support all operations:

| Operation | Spark | Pandas |
|-----------|-------|--------|
| Delta time travel | `versionAsOf` option | `DeltaTable(..., version=N)` |
| EXCEPT ALL | Native DataFrame | `merge` + indicator |
| LEFT ANTI JOIN | Native DataFrame | `merge` + filter |
| JDBC read | Native | SQLAlchemy `pd.read_sql` |

---

## 6. Bronze Metadata Columns

Bronze writes should include metadata for lineage and debugging. Use `add_metadata: true` for all applicable columns, or specify individual ones.

### Available Metadata Columns

| Column | Description | Applies to |
|--------|-------------|------------|
| `_extracted_at` | Pipeline execution timestamp | All sources |
| `_source_file` | Source filename/path | File sources (CSV, Parquet, JSON) |
| `_source_connection` | Connection name used | All sources |
| `_source_table` | Table or query name | SQL sources |

### Metadata Config Model

```python
class WriteMetadataConfig(BaseModel):
    """Metadata columns to add during Bronze writes."""

    extracted_at: bool = Field(default=True)
    source_file: bool = Field(default=True)
    source_connection: bool = Field(default=False)
    source_table: bool = Field(default=False)
```

### YAML Examples

**Add all applicable metadata (recommended):**
```yaml
write:
  connection: bronze
  table: customers
  mode: append
  add_metadata: true  # adds all applicable columns
```

**Specify individual columns:**
```yaml
write:
  connection: bronze
  table: customers
  mode: append
  add_metadata:
    extracted_at: true
    source_file: true
    source_connection: true
```

### Implementation Notes

**Spark:**
```python
from pyspark.sql.functions import input_file_name, current_timestamp, lit

df = df.withColumn("_extracted_at", current_timestamp())
df = df.withColumn("_source_file", input_file_name())  # file sources only
df = df.withColumn("_source_connection", lit(connection_name))
```

**Pandas:**
```python
df["_extracted_at"] = pd.Timestamp.now()
df["_source_file"] = source_path  # tracked during file read
df["_source_connection"] = connection_name
```

---

## 7. Hash Optimization for Merge

For large tables, use hashes to optimize change detection. Use the existing `generate_surrogate_key` transformer twice:

```yaml
transform:
  steps:
    # REQUIRED: Deduplication
    - operation: deduplicate
      params:
        keys: [customer_id]
        order_by: _extracted_at DESC

    # Hash for join key (optional - useful for composite keys)
    - operation: generate_surrogate_key
      params:
        columns: [customer_id]
        output_col: _hash_key

    # Hash for change detection
    - operation: generate_surrogate_key
      params:
        columns: [name, email, address, phone, status]  # non-key columns
        output_col: _hash_diff

    - operation: detect_deletes
      params:
        mode: sql_compare
        keys: [customer_id]
        source_connection: azure_sql
        source_table: dbo.Customers

write:
  connection: silver
  table: customers
  mode: upsert
  keys: [customer_id]
  update_condition: "source._hash_diff != target._hash_diff"  # skip unchanged rows
```

**Benefits:**
- Skip unchanged rows during merge (performance)
- Single-column comparison vs N columns
- Deterministic across runs

---

## 8. Full YAML Examples

### Example 1: Dimension with snapshot_diff (Full Snapshot Only)

**Note:** Only use `snapshot_diff` with full snapshot ingestion, not HWM.

```yaml
nodes:
  - name: bronze_customers
    read:
      connection: azure_sql
      format: sql
      table: dbo.Customers
      # No incremental = full snapshot
    write:
      connection: bronze
      table: customers
      mode: append
      add_metadata: true

  - name: silver_customers
    read:
      connection: bronze
      table: customers
    transform:
      steps:
        - operation: deduplicate
          params:
            keys: [customer_id]
            order_by: _extracted_at DESC
        - operation: detect_deletes
          params:
            mode: snapshot_diff
            keys: [customer_id]
            soft_delete_col: _is_deleted
    write:
      connection: silver
      table: customers
      mode: upsert
      keys: [customer_id]
```

### Example 2: Dimension with sql_compare (Recommended for HWM)

**Recommended:** Use `sql_compare` when source is authoritative and reachable.

```yaml
nodes:
  - name: bronze_products
    read:
      connection: erp_sql
      format: sql
      table: dbo.Products
      incremental:
        mode: stateful
        column: updated_at
        watermark_lag: 2h
    write:
      connection: bronze
      table: products
      mode: append
      add_metadata: true

  - name: silver_products
    read:
      connection: bronze
      table: products
    transform:
      steps:
        - operation: deduplicate
          params:
            keys: [product_id]
            order_by: _extracted_at DESC
        - operation: detect_deletes
          params:
            mode: sql_compare
            keys: [product_id]
            source_connection: erp_sql
            source_table: dbo.Products
            soft_delete_col: _is_deleted
    write:
      connection: silver
      table: products
      mode: upsert
      keys: [product_id]
```

### Example 3: Fact table (no delete detection)

```yaml
nodes:
  - name: silver_orders
    read:
      connection: bronze
      table: orders
    transform:
      steps:
        - operation: deduplicate
          params:
            keys: [order_id]
            order_by: _extracted_at DESC
        # No detect_deletes - facts are immutable
    write:
      connection: silver
      table: orders
      mode: upsert
      keys: [order_id]
```

### Example 4: Hard delete instead of soft delete

```yaml
transform:
  steps:
    - operation: detect_deletes
      params:
        mode: sql_compare
        keys: [customer_id]
        source_connection: azure_sql
        source_table: dbo.Customers
        soft_delete_col: null  # removes rows instead of flagging
```

### Example 5: Conservative threshold for critical dimension

```yaml
transform:
  steps:
    - operation: detect_deletes
      params:
        mode: sql_compare
        keys: [customer_id]
        source_connection: erp
        source_table: dbo.Customers
        max_delete_percent: 20.0    # fail if >20% would be deleted
        on_threshold_breach: error  # stop pipeline
```

### Example 6: First run handling

```yaml
transform:
  steps:
    - operation: detect_deletes
      params:
        mode: snapshot_diff
        keys: [product_id]
        on_first_run: skip  # default: no deletes on initial load
```

---

## 9. Edge Case Handling

| Scenario | Default Behavior | Config Override |
|----------|------------------|-----------------|
| **First run** (no previous version) | Skip detection | `on_first_run: error` |
| **>50% rows deleted** | Warn and continue | `max_delete_percent`, `on_threshold_breach` |
| **Source returns 0 rows** | Triggers threshold warning | Set `max_delete_percent: 0` to disable |
| **Connection failure** | Pipeline fails | Handle via standard error handling |

### Threshold Calculation

```python
delete_percent = (deleted_count / total_silver_rows) * 100

if delete_percent > params.max_delete_percent:
    if params.on_threshold_breach == "error":
        raise DeleteThresholdExceeded(f"{delete_percent:.1f}% exceeds {params.max_delete_percent}%")
    elif params.on_threshold_breach == "warn":
        logger.warning(f"Delete detection: {delete_percent:.1f}% of rows flagged for deletion")
    elif params.on_threshold_breach == "skip":
        logger.info("Delete threshold exceeded, skipping delete detection")
        return context  # no changes
```

---

## 10. Design Decisions Summary

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Default delete behavior | Soft delete (`_is_deleted`) | Audit trail, undo capability |
| State for snapshot_diff | Delta time travel | No extra columns needed, works in Pandas + Spark |
| batch_id column | Not needed | Delta versioning handles this |
| _snapshot_date column | Not needed | Over-engineering for edge cases |
| Version picker for snapshot_diff | Not supported | Always compare N to N-1, correct semantics |
| Bundled SilverTransformConfig | Not implemented | Keep transformers separate for flexibility |
| source_flagged mode | Not implemented | Use existing filter/rename transforms |
| Hash for merge | Use `generate_surrogate_key` twice | No new transformer needed |
| First run behavior | Skip by default | No deletes to detect on initial load |
| Delete threshold | 50% default, warn | Safety net for staging table accidents |
| Bronze metadata | Flexible config | `true` for all, or specify individual columns |

---

## 11. Implementation Checklist

### Phase 1: Config Models
- [ ] Add `DeleteDetectionMode` enum to `odibi/config.py`
- [ ] Add `DeleteDetectionConfig` model to `odibi/config.py`
- [ ] Add `WriteMetadataConfig` model to `odibi/config.py`
- [ ] Update `WriteConfig` to accept `add_metadata: bool | WriteMetadataConfig`

### Phase 2: Delete Detection Transformer
- [ ] Create `odibi/transformers/delete_detection.py`
- [ ] Implement `detect_deletes` transformer function
- [ ] Implement `_detect_deletes_snapshot_diff` for Spark
- [ ] Implement `_detect_deletes_snapshot_diff` for Pandas
- [ ] Implement `_detect_deletes_sql_compare` for Spark
- [ ] Implement `_detect_deletes_sql_compare` for Pandas
- [ ] Implement `_apply_deletes` helper (soft/hard delete logic)
- [ ] Implement threshold check logic
- [ ] Implement first-run detection (no previous version)
- [ ] Register transformer in `odibi/transformers/__init__.py`

### Phase 3: Bronze Metadata
- [ ] Add `add_metadata` support to write path (Spark engine)
- [ ] Add `add_metadata` support to write path (Pandas engine)
- [ ] Implement `_extracted_at` column addition
- [ ] Implement `_source_file` column addition (file sources)
- [ ] Implement `_source_connection` column addition
- [ ] Implement `_source_table` column addition (SQL sources)

### Phase 4: Testing
- [ ] Write unit tests for `DeleteDetectionConfig` validation
- [ ] Write unit tests for `detect_deletes` - mode: none
- [ ] Write unit tests for `detect_deletes` - mode: snapshot_diff (Spark)
- [ ] Write unit tests for `detect_deletes` - mode: snapshot_diff (Pandas)
- [ ] Write unit tests for `detect_deletes` - mode: sql_compare (Spark)
- [ ] Write unit tests for `detect_deletes` - mode: sql_compare (Pandas)
- [ ] Write unit tests for edge cases (first run, threshold breach)
- [ ] Write unit tests for soft delete vs hard delete
- [ ] Write unit tests for metadata columns

### Phase 5: Introspect & Documentation
- [ ] Update `odibi/introspect.py` GROUP_MAPPING with new configs
- [ ] Update `odibi/introspect.py` TRANSFORM_CATEGORY_MAP for delete_detection
- [ ] Update `odibi/introspect.py` CUSTOM_ORDER for new configs
- [ ] Ensure `DeleteDetectionConfig` has top-tier docstrings with examples (like MergeParams)
- [ ] Ensure `WriteMetadataConfig` has comprehensive docstrings
- [ ] Add YAML examples in docstrings following existing patterns
- [ ] Update `docs/reference/yaml_schema.md`
- [ ] Update `docs/architecture/ingestion-correctness.md` (done)
- [ ] Add cookbook examples

---

## 12. Docstring Standards (for Introspect)

All new config models must follow the existing docstring pattern for auto-generated docs:

```python
class DeleteDetectionConfig(BaseModel):
    """
    Configuration for delete detection in Silver layer.

    ### 🔍 "CDC Without CDC" Guide

    **Business Problem:**
    "Records are deleted in our Azure SQL source, but our Silver tables still show them."

    **The Solution:**
    Use delete detection to identify and flag records that no longer exist in the source.

    **Recipe 1: SQL Compare (Recommended for HWM)**
    ```yaml
    transform:
      steps:
        - operation: detect_deletes
          params:
            mode: sql_compare
            keys: [customer_id]
            source_connection: azure_sql
            source_table: dbo.Customers
    ```

    **Recipe 2: Snapshot Diff (For Staging Tables)**
    ```yaml
    transform:
      steps:
        - operation: detect_deletes
          params:
            mode: snapshot_diff
            keys: [customer_id]
    ```

    **Recipe 3: Conservative Threshold**
    ```yaml
    transform:
      steps:
        - operation: detect_deletes
          params:
            mode: sql_compare
            keys: [customer_id]
            source_connection: erp
            source_table: dbo.Customers
            max_delete_percent: 20.0
            on_threshold_breach: error
    ```
    """
```

This ensures the introspect tool generates high-quality, example-rich documentation.
