# Feature: SQL Server Merge/Upsert and Enhanced Overwrite Support

## Problem

odibi's JDBC write to SQL Server only supports basic `overwrite`, `append`, `ignore`, `error` modes. There's no native merge/upsert capability, forcing users to either:
1. Full overwrite (inefficient for large tables)
2. Manually create staging tables + stored procedures (tedious)

## Use Case

Syncing Delta tables from Databricks to Azure SQL Server for Power BI consumption. Need incremental updates based on composite keys without rewriting entire tables.

---

## Proposed Solution

### 1. New Write Mode: `merge`

```yaml
write:
  connection: my_azure_sql
  format: azure_sql
  table: oee.oee_fact
  mode: merge
  merge_keys: [DateId, P_ID]
  merge_options:
    # Conditions
    update_condition: "source._hash_diff != target._hash_diff"
    delete_condition: "source._is_deleted = true"
    insert_condition: "source.is_valid = 1"
    
    # Columns
    exclude_columns: [_hash_diff, _is_deleted]
    column_mapping:
      source_col_name: target_col_name
    
    # Audit
    audit_cols:
      created_col: created_ts
      updated_col: updated_ts
    
    # Keys & Indexing
    auto_generate_pk: true
    pk_column: oee_fact_id
    create_clustered_index: true
    
    # Staging
    staging_schema: staging
    cleanup_staging: true
    
    # Batching
    batch_size: 100000
    
    # Schema Evolution
    schema_evolution:
      mode: evolve                # strict | evolve | ignore
      add_columns: true
      widen_types: true
      drop_columns: false
      fail_on_incompatible: true
    
    # Validations
    validations:
      require_layer: gold
      check_null_keys: true
      check_duplicate_keys: true
      check_schema_match: true
```

### 2. Enhanced Write Mode: `overwrite`

```yaml
write:
  connection: my_azure_sql
  format: azure_sql
  table: oee.oee_fact
  mode: overwrite
  overwrite_options:
    strategy: truncate_insert    # truncate_insert | drop_create | delete_insert
    
    # Shared with merge
    auto_generate_pk: true
    pk_column: oee_fact_id
    create_clustered_index: true
    
    audit_cols:
      created_col: created_ts
      updated_col: updated_ts
    
    schema_evolution:
      mode: evolve
      add_columns: true
    
    validations:
      require_layer: gold
      check_schema_match: true
```

---

## Implementation Details

### Merge Flow

1. **Validate source data**
   - Check merge_keys exist in DataFrame
   - Check for null keys (fail if `check_null_keys: true`)
   - Check for duplicate keys (fail if `check_duplicate_keys: true`)
   - Check layer if `require_layer` is set

2. **Generate PK** (if `auto_generate_pk: true`)
   - Create deterministic integer from merge_keys hash
   - Add as column to DataFrame

3. **Create target table** (if not exists)
   - Infer schema from DataFrame
   - Apply column_mapping
   - Exclude columns in exclude_columns
   - Add audit columns
   - Create clustered index on PK

4. **Schema evolution** (if target exists)
   - Compare source vs target schemas
   - Add new columns if `add_columns: true`
   - Widen types if `widen_types: true`
   - Fail on incompatible changes if `fail_on_incompatible: true`

5. **Create staging table**
   - `{staging_schema}.{table_name}_staging`
   - Mirror target schema

6. **Write to staging**
   - Use existing JDBC write with `mode: overwrite`

7. **Generate T-SQL MERGE**
   ```sql
   BEGIN TRANSACTION;
   
   MERGE [oee].[oee_fact] AS target
   USING [staging].[oee_fact_staging] AS source
   ON target.[DateId] = source.[DateId] AND target.[P_ID] = source.[P_ID]
   
   WHEN MATCHED AND source._hash_diff != target._hash_diff THEN 
     UPDATE SET 
       [col1] = source.[col1],
       [updated_ts] = GETUTCDATE(),
       ...
   
   WHEN NOT MATCHED BY TARGET AND source.is_valid = 1 THEN
     INSERT ([col1], [created_ts], [updated_ts], ...)
     VALUES (source.[col1], GETUTCDATE(), GETUTCDATE(), ...)
   
   WHEN MATCHED AND source._is_deleted = 1 THEN
     DELETE
   
   OUTPUT $action, inserted.*, deleted.*;
   
   COMMIT TRANSACTION;
   ```

8. **Log results**
   - Count inserts, updates, deletes from OUTPUT
   - Log to odibi structured logging

9. **Cleanup**
   - Drop staging table if `cleanup_staging: true`

### Overwrite Strategies

| Strategy | SQL Generated |
|----------|---------------|
| `truncate_insert` | `TRUNCATE TABLE [schema].[table]; INSERT INTO ...` |
| `drop_create` | `DROP TABLE IF EXISTS [schema].[table]; CREATE TABLE ...; INSERT INTO ...` |
| `delete_insert` | `DELETE FROM [schema].[table]; INSERT INTO ...` |

### Schema Evolution Modes

| Mode | New Column | Missing Column | Type Widening | Incompatible Type |
|------|------------|----------------|---------------|-------------------|
| `strict` | Fail | Fail | Fail | Fail |
| `evolve` | ALTER ADD | Ignore | ALTER COLUMN | Fail |
| `ignore` | Skip | Skip | Use target | Use target |

### Validations

| Check | Default | Behavior |
|-------|---------|----------|
| `require_layer` | None | Warn if layer doesn't match |
| `check_null_keys` | true | Fail if merge_keys contain nulls |
| `check_duplicate_keys` | true | Fail if duplicate merge_keys |
| `check_schema_match` | warn | Log warning on mismatch |

---

## Files to Modify

| File | Changes |
|------|---------|
| `odibi/engine/spark_engine.py` | Add `merge` mode handling, call new merge logic |
| `odibi/engine/pandas_engine.py` | Add `merge` mode handling (Pandas → SQL support) |
| `odibi/connections/azure_sql.py` | Add `execute_merge()`, `execute_schema_evolution()`, `create_table()` |
| `odibi/config.py` | Add `MergeOptions`, `OverwriteOptions`, `SchemaEvolutionConfig`, `ValidationConfig` |
| `odibi/writers/sql_server_writer.py` | New file: SQL Server write logic |
| `odibi/writers/__init__.py` | Export new writer |

---

## Config Schema (Pydantic)

```python
class SchemaEvolutionConfig(BaseModel):
    mode: Literal["strict", "evolve", "ignore"] = "evolve"
    add_columns: bool = True
    widen_types: bool = True
    drop_columns: bool = False
    fail_on_incompatible: bool = True


class ValidationConfig(BaseModel):
    require_layer: Optional[str] = None
    check_null_keys: bool = True
    check_duplicate_keys: bool = True
    check_schema_match: Literal["fail", "warn", "ignore"] = "warn"


class AuditColsConfig(BaseModel):
    created_col: Optional[str] = None
    updated_col: Optional[str] = None


class MergeOptions(BaseModel):
    # Conditions
    update_condition: Optional[str] = None
    delete_condition: Optional[str] = None
    insert_condition: Optional[str] = None
    
    # Columns
    exclude_columns: List[str] = Field(default_factory=list)
    column_mapping: Dict[str, str] = Field(default_factory=dict)
    
    # Audit
    audit_cols: Optional[AuditColsConfig] = None
    
    # Keys & Indexing
    auto_generate_pk: bool = False
    pk_column: str = "id"
    create_clustered_index: bool = True
    
    # Staging
    staging_schema: str = "staging"
    cleanup_staging: bool = True
    
    # Batching
    batch_size: Optional[int] = None
    
    # Schema & Validation
    schema_evolution: SchemaEvolutionConfig = Field(default_factory=SchemaEvolutionConfig)
    validations: ValidationConfig = Field(default_factory=ValidationConfig)


class OverwriteOptions(BaseModel):
    strategy: Literal["truncate_insert", "drop_create", "delete_insert"] = "truncate_insert"
    
    # Shared with merge
    auto_generate_pk: bool = False
    pk_column: str = "id"
    create_clustered_index: bool = True
    audit_cols: Optional[AuditColsConfig] = None
    schema_evolution: SchemaEvolutionConfig = Field(default_factory=SchemaEvolutionConfig)
    validations: ValidationConfig = Field(default_factory=ValidationConfig)


class WriteConfig(BaseModel):
    # ... existing fields ...
    mode: Literal["overwrite", "append", "ignore", "error", "merge", "upsert"]
    merge_keys: Optional[List[str]] = None
    merge_options: Optional[MergeOptions] = None
    overwrite_options: Optional[OverwriteOptions] = None
```

---

## Edge Cases

1. **No matching rows** - Pure insert (MERGE handles this)
2. **All rows match** - Pure update (MERGE handles this)
3. **Empty source** - Skip or warn? (Configurable)
4. **Large tables** - Batch processing with configurable batch_size
5. **Transaction timeout** - Set appropriate timeout, log progress
6. **Column name escaping** - Handle spaces/special chars with `[]`
7. **Null in non-nullable column** - Fail with clear error
8. **Type coercion** - Spark types → SQL Server types mapping
9. **Unicode/encoding** - Ensure proper NVARCHAR handling
10. **Concurrent writes** - Table locking during MERGE
11. **Identity columns** - Skip in INSERT, let SQL auto-generate

---

## Example Usage

### Sync dimension with SCD (current values only)
```yaml
- name: sync_dim_plantprocess
  inputs:
    source: $silver.dim_plantprocess
  transform:
    steps:
      - sql: "SELECT * FROM df WHERE is_current = true"
  write:
    connection: azure_sql_prod
    format: azure_sql
    table: dim.plantprocess
    mode: merge
    merge_keys: [P_ID]
    merge_options:
      audit_cols:
        created_col: created_ts
        updated_col: updated_ts
```

### Sync fact table with hash diff
```yaml
- name: sync_oee_fact
  inputs:
    source: $gold.oee_fact
  write:
    connection: azure_sql_prod
    format: azure_sql
    table: fact.oee
    mode: merge
    merge_keys: [DateId, P_ID]
    merge_options:
      update_condition: "source._hash_diff != target._hash_diff"
      exclude_columns: [_hash_diff]
      auto_generate_pk: true
      pk_column: oee_fact_id
      audit_cols:
        created_col: created_ts
        updated_col: updated_ts
```

### Full overwrite with schema evolution
```yaml
- name: sync_combined_downtime
  inputs:
    source: $gold.combined_downtime
  write:
    connection: azure_sql_prod
    format: azure_sql
    table: fact.combined_downtime
    mode: overwrite
    overwrite_options:
      strategy: truncate_insert
      schema_evolution:
        mode: evolve
        add_columns: true
```

---

## Documentation Updates

After implementation, update the following:

| Doc | Changes |
|-----|---------|
| `docs/patterns/sql_server_merge.md` | New file: comprehensive guide |
| `docs/reference/yaml_schema.md` | Add MergeOptions, OverwriteOptions schemas |
| `docs/guides/dimensional_modeling_guide.md` | Add section on syncing to SQL Server |
| `docs/guides/production_deployment.md` | Add SQL Server sync patterns |
| `CHANGELOG.md` | Document new feature |
| `mkdocs.yml` | Add new doc to nav |

### New Doc: `docs/patterns/sql_server_merge.md`

```markdown
# SQL Server Merge Pattern

Sync Delta tables to SQL Server with merge/upsert semantics.

## When to Use
- Syncing gold tables to Azure SQL for Power BI
- Incremental updates without full table overwrites
- Maintaining audit trails in SQL Server

## Basic Usage
[examples...]

## Configuration Reference
[full options...]

## Best Practices
- Use merge for incremental, overwrite for full refresh
- Always set audit_cols for traceability
- Use hash_diff for efficient change detection
- Set appropriate batch_size for large tables
```

---

## Testing

| Test | Description |
|------|-------------|
| `tests/unit/test_sql_server_merge.py` | Unit tests with mocked JDBC |
| `tests/unit/test_sql_server_overwrite.py` | Unit tests for overwrite strategies |
| `tests/unit/test_merge_config.py` | Config validation tests |
| `tests/integration/test_sql_server_merge.py` | Integration tests (requires SQL Server) |

### Test Cases
- [ ] Merge with composite keys
- [ ] Merge with single key
- [ ] Update condition filtering
- [ ] Delete condition (soft deletes)
- [ ] Insert condition filtering
- [ ] Exclude columns
- [ ] Column mapping
- [ ] Audit column population
- [ ] Auto PK generation
- [ ] Clustered index creation
- [ ] Schema evolution - add columns
- [ ] Schema evolution - widen types
- [ ] Schema evolution - incompatible type fails
- [ ] Null key validation
- [ ] Duplicate key validation
- [ ] Layer validation
- [ ] Overwrite truncate_insert
- [ ] Overwrite drop_create
- [ ] Overwrite delete_insert
- [ ] Batch processing
- [ ] Transaction rollback on failure
- [ ] Column name with spaces
- [ ] Empty source handling

---

## Acceptance Criteria

- [ ] `mode: merge` works for Spark engine → SQL Server
- [ ] `mode: merge` works for Pandas engine → SQL Server
- [ ] All merge_options are implemented and tested
- [ ] All overwrite_options are implemented and tested
- [ ] Schema evolution works correctly
- [ ] Validations work correctly
- [ ] Audit columns are populated
- [ ] Row counts are logged (inserts/updates/deletes)
- [ ] Error handling with transaction rollback
- [ ] Works with column names containing spaces
- [ ] Documentation complete
- [ ] mkdocs builds successfully
- [ ] All unit tests pass
- [ ] Integration tests pass (optional)

---

## Priority

**High** - Blocks production sync pipeline for OEE project

## Labels

`enhancement`, `sql-server`, `write-modes`, `high-priority`

## Estimated Effort

4-5 days
- Day 1-2: Core merge implementation
- Day 2-3: Overwrite enhancements, schema evolution
- Day 3-4: Validations, edge cases, testing
- Day 4-5: Documentation, integration testing
