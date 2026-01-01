# Feature: SQL Server Merge/Upsert and Enhanced Overwrite Support

## Problem

odibi's JDBC write to SQL Server only supports basic `overwrite`, `append`, `ignore`, `error` modes. There's no native merge/upsert capability, forcing users to either:
1. Full overwrite (inefficient for large tables)
2. Manually create staging tables + stored procedures (tedious)

## Use Case

Syncing Delta tables from Databricks to Azure SQL Server for Power BI consumption. Need incremental updates based on composite keys without rewriting entire tables.

---

## Phased Implementation

### Phase 1: Minimal Spark → SQL Server MERGE (MVP) ✅ COMPLETE

**Scope:**
- **Engine:** Spark only
- **Target:** SQL Server connections
- **Assumptions:** Target table and schema already exist (no DDL)

**Features:**
- New `write.mode: "merge"` for Spark→SQL Server
- `merge_keys` (required) - composite key support
- `update_condition` (optional) - e.g., `"source._hash_diff != target._hash_diff"`
- `delete_condition` (optional) - e.g., `"source._is_deleted = true"`
- `insert_condition` (optional) - e.g., `"source.is_valid = 1"`
- `exclude_columns` - columns to exclude from merge
- `audit_cols` - created/updated timestamp columns
- Staging table: `{staging_schema}.{table}_staging`, truncate before write
- Transaction-wrapped MERGE execution with count logging

**Config (Phase 1):**
```yaml
write:
  connection: my_azure_sql
  format: sql_server
  table: oee.oee_fact
  mode: merge
  merge_keys: [DateId, P_ID]
  merge_options:
    update_condition: "source._hash_diff != target._hash_diff"
    delete_condition: "source._is_deleted = true"
    exclude_columns: [_hash_diff, _is_deleted]
    staging_schema: staging
    audit_cols:
      created_col: created_ts
      updated_col: updated_ts
```

**Files to Modify:**
| File | Changes |
|------|---------|
| `odibi/config.py` | Add `MERGE` to `WriteMode`, add `MergeOptions`, `AuditColsConfig` |
| `odibi/writers/sql_server_writer.py` | New file: MERGE logic |
| `odibi/writers/__init__.py` | Export new writer |
| `odibi/engine/spark_engine.py` | Route `merge` mode to SQL Server writer |

**Estimated Effort:** 1-3 days

---

### Phase 2: Overwrite Strategies + Validations ✅ COMPLETE

**Scope:**
- Enhanced `overwrite` with strategies: `truncate_insert`, `drop_create`, `delete_insert`
- Key validations: null checks, duplicate checks
- `SqlServerOverwriteOptions` and `SqlServerMergeValidationConfig` models

**Files Modified:**
| File | Changes |
|------|---------|
| `odibi/config.py` | Added `SqlServerOverwriteStrategy`, `SqlServerMergeValidationConfig`, `SqlServerOverwriteOptions` |
| `odibi/writers/sql_server_writer.py` | Added `OverwriteResult`, `ValidationResult`, overwrite strategies, validation methods |
| `odibi/engine/spark_engine.py` | Added enhanced overwrite handling |

---

### Phase 3: Pandas Engine Support ✅ COMPLETE

**Scope:**
- Reuse `SqlServerMergeWriter` from Pandas engine
- `merge_pandas()` and `overwrite_pandas()` methods
- Full parity with Spark for SQL Server operations

**Files Modified:**
| File | Changes |
|------|---------|
| `odibi/engine/pandas_engine.py` | Added merge and enhanced overwrite support in `_write_sql()` |

---

### Phase 4: Advanced Features ✅ COMPLETE

**Scope:**
- Polars engine support (`merge_polars`, `overwrite_polars`)
- Auto schema creation (`auto_create_schema: true`)
- Auto table creation (`auto_create_table: true`)
- Schema evolution (modes: `strict`, `evolve`, `ignore`)
- Batch processing (`batch_size` option)

**Files Modified:**
| File | Changes |
|------|---------|
| `odibi/config.py` | Added `SqlServerSchemaEvolutionMode`, `SqlServerSchemaEvolutionConfig`; Phase 4 fields in `SqlServerMergeOptions` and `SqlServerOverwriteOptions` |
| `odibi/writers/sql_server_writer.py` | Added `validate_keys_polars`, `merge_polars`, `overwrite_polars`, schema/table creation, schema evolution handling, batch processing |
| `odibi/engine/polars_engine.py` | Added SQL Server format support with `_write_sql()` method |
| `odibi/introspect.py` | Added Phase 4 config classes to GROUP_MAPPING |
| `docs/patterns/sql_server_merge.md` | New comprehensive documentation |
| `docs/reference/PARITY_TABLE.md` | Updated with SQL Server feature parity |
| `tests/unit/test_sql_server_merge.py` | 39 new Phase 4 tests (86 total)

**Future (not in scope):**
- Auto PK generation from hash
- Clustered index creation
- Type widening in schema evolution

---

## Phase 1 Implementation Details

### Merge Flow

1. **Validate inputs**
   - Check `merge_keys` provided
   - Check connection is SQL Server type
   - Check target table exists (fail with clear error if not)

2. **Prepare staging table name**
   - `{staging_schema}.{table_name}_staging`

3. **Truncate staging** (if exists)
   ```sql
   IF OBJECT_ID('[staging].[oee_fact_staging]', 'U') IS NOT NULL
       TRUNCATE TABLE [staging].[oee_fact_staging]
   ```

4. **Write to staging**
   - Use existing Spark JDBC write with `mode: overwrite`

5. **Build MERGE T-SQL**
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
     DELETE;
   
   -- Capture counts
   DECLARE @inserted INT, @updated INT, @deleted INT;
   -- (counts captured via OUTPUT INTO temp table)
   
   COMMIT TRANSACTION;
   ```

6. **Execute and log results**
   - Log insert/update/delete counts via structured logging

### Error Handling

- If `mode == "merge"` and connection is not SQL Server → raise clear error
- If target table doesn't exist → fail with actionable message
- Transaction rollback on any failure

---

## Phase 1 Config Schema (Pydantic)

```python
class AuditColsConfig(BaseModel):
    """Audit column configuration for merge operations."""
    created_col: Optional[str] = None
    updated_col: Optional[str] = None


class MergeOptions(BaseModel):
    """Options for SQL Server MERGE operations (Phase 1)."""
    # Conditions
    update_condition: Optional[str] = None
    delete_condition: Optional[str] = None
    insert_condition: Optional[str] = None
    
    # Columns
    exclude_columns: List[str] = Field(default_factory=list)
    
    # Staging
    staging_schema: str = "staging"
    
    # Audit
    audit_cols: Optional[AuditColsConfig] = None


# Extend WriteMode enum
class WriteMode(str, Enum):
    OVERWRITE = "overwrite"
    APPEND = "append"
    UPSERT = "upsert"
    APPEND_ONCE = "append_once"
    MERGE = "merge"  # New for SQL Server
```

---

## Phase 1 Testing

| Test | Description |
|------|-------------|
| `tests/unit/test_sql_server_writer.py` | MERGE SQL generation, staging logic |
| `tests/unit/test_merge_config.py` | Config validation |

### Test Cases (Phase 1)
- [ ] Merge with composite keys
- [ ] Merge with single key
- [ ] Update condition filtering
- [ ] Delete condition (soft deletes)
- [ ] Insert condition filtering
- [ ] Exclude columns
- [ ] Audit column population in generated SQL
- [ ] Error when connection is not SQL Server
- [ ] Error when target table doesn't exist
- [ ] Column name escaping (spaces, special chars)

---

## Phase 1 Acceptance Criteria

- [ ] `mode: merge` works for Spark engine → SQL Server
- [ ] All Phase 1 merge_options implemented
- [ ] Staging table truncated before write
- [ ] MERGE executed in transaction
- [ ] Row counts logged (inserts/updates/deletes)
- [ ] Clear error messages for unsupported scenarios
- [ ] Unit tests pass

---

## Priority

**High** - Blocks production sync pipeline for OEE project

## Labels

`enhancement`, `sql-server`, `write-modes`, `high-priority`

## Estimated Total Effort

- Phase 1: 1-3 days ← **Current Focus**
- Phase 2: 2-3 days
- Phase 3: 1-2 days
- Phase 4: TBD
