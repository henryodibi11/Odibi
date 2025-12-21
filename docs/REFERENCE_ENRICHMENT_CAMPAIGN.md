# Reference Enrichment Campaign

## Goal

Make the auto-generated `yaml_schema.md` reference beginner-friendly, complete, and cross-linked to relevant guides/tutorials.

## Context

**Current State:**
- `docs/reference/yaml_schema.md` is auto-generated from Pydantic docstrings via `odibi/introspect.py`
- Source of truth: **docstrings in `odibi/config.py`**
- Currently 3400+ lines with good structure but inconsistent depth
- Missing: beginner context, cross-references, "when to use" guidance

**Key Pain Points Identified:**
- Validation vs Contracts vs Quality Gates - confusing overlap
- Tests/Contracts terminology inconsistent
- Some configs have great examples, others are bare
- No links to relevant tutorials/guides
- Assumes reader knows SCD2, Delta, HWM, etc.

**Success Criteria:**
- [ ] Every major config has a "When to Use" one-liner
- [ ] Every major config has a "See Also" linking to relevant guide/tutorial
- [ ] Validation/Contracts/Quality Gates clearly distinguished
- [ ] All pattern configs have realistic YAML examples
- [ ] `mkdocs build` passes
- [ ] `python odibi/introspect.py` regenerates successfully
- [ ] `ruff check . && ruff format .` passes
- [ ] All tests pass

---

## Phase 1: Clarify Validation Architecture (HIGH PRIORITY)

The current docs are confusing about:
- **Contracts** (pre-transform checks)
- **Validation** (post-transform checks)
- **Quality Gates** (pipeline-level thresholds)
- **Quarantine** (where bad rows go)

### 1.1 Update SECTION_INTROS in introspect.py

**File:** `odibi/introspect.py` → `SECTION_INTROS["Contract"]`

Add clear disambiguation:

```python
SECTION_INTROS["Contract"] = """
### Contracts (Pre-Transform Checks)

Contracts are **fail-fast data quality checks** that run on input data **before** transformation.
They always halt execution on failure - use them to prevent bad data from entering the pipeline.

**Contracts vs Validation vs Quality Gates:**

| Feature | When it Runs | On Failure | Use Case |
|---------|--------------|------------|----------|
| **Contracts** | Before transform | Always fails | Input data quality (not-null, unique keys) |
| **Validation** | After transform | Configurable (fail/warn/quarantine) | Output data quality (ranges, formats) |
| **Quality Gates** | After validation | Configurable (abort/warn) | Pipeline-level thresholds (pass rate, row counts) |
| **Quarantine** | With validation | Routes bad rows | Capture invalid records for review |

**See Also:**
- [Validation Guide](../features/quality_gates.md) - Full validation configuration
- [Quarantine Guide](../features/quarantine.md) - Quarantine setup and review
- [Getting Started: Validation](../tutorials/getting_started.md#add-data-validation)

**Example:**
```yaml
- name: "process_orders"
  contracts:
    - type: not_null
      columns: [order_id, customer_id]
    - type: row_count
      min: 100
    - type: freshness
      column: created_at
      max_age: "24h"
  read:
    source: raw_orders
```
"""
```

### 1.2 Update Contract Config Docstrings

**File:** `odibi/config.py`

For each test type, add:
1. One-liner "when to use"
2. See Also reference

**NotNullTest:**
```python
class NotNullTest(BaseModel):
    """Ensures specified columns contain no NULL values.

    **When to Use:** Primary keys, required fields, foreign keys that must resolve.

    **See Also:** [Contracts Overview](#contracts-data-quality-gates)

    ```yaml
    contracts:
      - type: not_null
        columns: [order_id, customer_id, created_at]
    ```
    """
```

**UniqueTest:**
```python
class UniqueTest(BaseModel):
    """Ensures specified columns (or combination) contain unique values.

    **When to Use:** Primary keys, natural keys, deduplication verification.

    **See Also:** [Contracts Overview](#contracts-data-quality-gates)

    ```yaml
    contracts:
      - type: unique
        columns: [order_id]  # Single column
      # OR composite key:
      - type: unique
        columns: [customer_id, order_date]  # Composite uniqueness
    ```
    """
```

**FreshnessContract:**
```python
class FreshnessContract(BaseModel):
    """Validates that data is not stale.

    **When to Use:** Source systems that should update regularly, SLA monitoring.

    **See Also:** [Contracts Overview](#contracts-data-quality-gates)

    ```yaml
    contracts:
      - type: freshness
        column: updated_at
        max_age: "24h"  # Fail if no data newer than 24 hours
    ```
    """
```

---

## Phase 2: Enrich Core Configs (MEDIUM PRIORITY)

### 2.1 ReadConfig

**File:** `odibi/config.py` → `ReadConfig` docstring

Add:
```python
class ReadConfig(BaseModel):
    """Configuration for reading data into a node.

    **When to Use:** First node in a pipeline, or any node that reads from storage.

    **Key Concepts:**
    - `connection`: References a named connection from `connections:` section
    - `format`: File format (csv, parquet, delta, json, avro)
    - `incremental`: Enable incremental loading (only new data)

    **See Also:**
    - [Smart Read Pattern](../patterns/smart_read.md) - Automatic full/incremental switching
    - [Incremental Loading](../patterns/incremental_stateful.md) - HWM-based loading
    - [Supported Formats](./supported_formats.md) - All format options

    **Examples:**

    Basic Read:
    ```yaml
    read:
      connection: bronze
      format: parquet
      path: raw/customers/
    ```

    Incremental Read (Stateful HWM):
    ```yaml
    read:
      connection: bronze
      format: delta
      table: raw_orders
      incremental:
        type: stateful
        column: updated_at
    ```

    First Run Override:
    ```yaml
    read:
      connection: source_db
      query: "SELECT * FROM orders"
      first_run_query: "SELECT * FROM orders WHERE order_date >= '2023-01-01'"
    ```
    """
```

### 2.2 WriteConfig

**File:** `odibi/config.py` → `WriteConfig` docstring

Add:
```python
class WriteConfig(BaseModel):
    """Configuration for writing data from a node.

    **When to Use:** Any node that persists data to storage.

    **Key Concepts:**
    - `mode`: How to handle existing data (overwrite, append, upsert)
    - `keys`: Required for upsert mode - columns that identify unique records
    - `partition_by`: Columns to partition output by (improves query performance)

    **See Also:**
    - [Merge/Upsert Pattern](../patterns/merge_upsert.md) - Delta merge operations
    - [Performance Tuning](../guides/performance_tuning.md) - Partitioning strategies

    **Examples:**

    Simple Overwrite:
    ```yaml
    write:
      connection: silver
      format: parquet
      path: cleaned/customers/
      mode: overwrite
    ```

    Delta Upsert (Merge):
    ```yaml
    write:
      connection: gold
      format: delta
      table: dim_customer
      mode: upsert
      keys: [customer_id]  # Match on this column
    ```

    Partitioned Write:
    ```yaml
    write:
      connection: gold
      format: delta
      table: fact_orders
      mode: append
      partition_by: [order_date]
    ```
    """
```

### 2.3 TransformConfig

**File:** `odibi/config.py` → `TransformConfig` docstring

Add:
```python
class TransformConfig(BaseModel):
    """Configuration for transformation steps within a node.

    **When to Use:** Custom business logic, data cleaning, SQL transformations.

    **Key Concepts:**
    - `steps`: Ordered list of operations (SQL, functions, or both)
    - Each step receives `df` (the DataFrame from previous step)
    - Steps execute in order: step1 → step2 → step3

    **See Also:**
    - [Writing Transformations](../guides/writing_transformations.md) - Custom functions
    - [Transformer Catalog](#transformer-catalog) - Built-in functions

    **Transformer vs Transform:**
    - `transformer`: Single heavy operation (scd2, merge, deduplicate)
    - `transform.steps`: Chain of lighter operations

    **Examples:**

    SQL-Only:
    ```yaml
    transform:
      steps:
        - sql: "SELECT *, UPPER(name) as name_upper FROM df"
        - sql: "SELECT * FROM df WHERE status = 'active'"
    ```

    Function Steps:
    ```yaml
    transform:
      steps:
        - function: clean_text
          params:
            columns: [email, name]
            case: lower
        - function: fill_nulls
          params:
            defaults: {status: 'unknown', region: 'US'}
    ```

    Mixed SQL + Functions:
    ```yaml
    transform:
      steps:
        - sql: "SELECT * FROM df WHERE order_total > 0"
        - function: derive_columns
          params:
            expressions:
              profit: "revenue - cost"
              margin: "profit / revenue * 100"
    ```
    """
```

---

## Phase 3: Enrich Pattern Configs (MEDIUM PRIORITY)

### 3.1 DimensionPattern

Add beginner context to existing docstring:

```python
"""Build complete dimension tables with surrogate keys and SCD support.

**When to Use:**
- Building dimension tables from source systems (customers, products, locations)
- Need surrogate keys for star schema joins
- Need to track historical changes (SCD Type 2)

**Beginner Note:**
Dimensions are the "who, what, where, when" of your data warehouse.
A customer dimension has customer_id (natural key) and customer_sk (surrogate key).
Fact tables join to dimensions via surrogate keys.

**See Also:**
- [Dimension Tutorial](../tutorials/dimensional_modeling/02_dimension_pattern.md)
- [SCD2 Pattern](../patterns/scd2.md) - Slowly Changing Dimensions
- [Fact Pattern](#factpattern) - Build facts that reference dimensions
"""
```

### 3.2 FactPattern

Add:
```python
"""Build fact tables with automatic surrogate key lookups from dimensions.

**When to Use:**
- Building fact tables from transactional data (orders, events, transactions)
- Need to look up surrogate keys from dimension tables
- Need to handle orphan records (missing dimension matches)

**Beginner Note:**
Facts are the "how much, how many" of your data warehouse.
An orders fact has measures (quantity, revenue) and dimension keys (customer_sk, product_sk).
The pattern automatically looks up SKs from dimensions.

**See Also:**
- [Fact Tutorial](../tutorials/dimensional_modeling/04_fact_pattern.md)
- [Dimension Pattern](#dimensionpattern) - Build dimensions first
- [FK Validation](../validation/fk.md) - Validate referential integrity
"""
```

### 3.3 AggregationPattern

Add:
```python
"""Declarative aggregation with GROUP BY and optional incremental merge.

**When to Use:**
- Building summary/aggregate tables (daily sales, monthly metrics)
- Need incremental aggregation (update existing aggregates)
- Gold layer reporting tables

**Beginner Note:**
Aggregations summarize facts at a higher grain.
Example: daily_sales aggregates orders by date with SUM(revenue).

**See Also:**
- [Aggregation Tutorial](../tutorials/dimensional_modeling/05_aggregation_pattern.md)
- [Windowed Reprocess](../patterns/windowed_reprocess.md) - Rolling aggregations
"""
```

---

## Phase 4: Enrich Connection Configs (LOW PRIORITY)

### 4.1 Add "When to Use" to Each Connection

**LocalConnectionConfig:**
```python
"""Local filesystem connection.

**When to Use:** Development, testing, small datasets, local processing.

**See Also:** [Azure Setup](../guides/setup_azure.md) for cloud alternatives.
"""
```

**DeltaConnectionConfig:**
```python
"""Delta Lake connection for ACID-compliant data lakes.

**When to Use:**
- Production data lakes on Azure/AWS/GCP
- Need time travel, ACID transactions, schema evolution
- Upsert/merge operations

**See Also:**
- [Supported Formats](./supported_formats.md#delta) - Delta-specific options
- [Merge Pattern](../patterns/merge_upsert.md) - Delta merge operations
"""
```

**AzureBlobConnectionConfig:**
```python
"""Azure Blob Storage / ADLS Gen2 connection.

**When to Use:** Azure-based data lakes, landing zones, raw data storage.

**See Also:**
- [Azure Setup Guide](../guides/setup_azure.md) - Full Azure configuration
- [Secrets Management](../guides/secrets.md) - Secure credential handling
"""
```

---

## Phase 5: Fix Cross-References in introspect.py

### 5.1 Add See Also Links Generator

**File:** `odibi/introspect.py`

Add a mapping for auto-generating "See Also" sections:

```python
SEE_ALSO_LINKS = {
    "ReadConfig": [
        ("Smart Read Pattern", "../patterns/smart_read.md"),
        ("Incremental Loading", "../patterns/incremental_stateful.md"),
    ],
    "WriteConfig": [
        ("Merge/Upsert Pattern", "../patterns/merge_upsert.md"),
        ("Performance Tuning", "../guides/performance_tuning.md"),
    ],
    "ValidationConfig": [
        ("Validation Guide", "../features/quality_gates.md"),
        ("Quarantine", "../features/quarantine.md"),
    ],
    "QuarantineConfig": [
        ("Quarantine Guide", "../features/quarantine.md"),
        ("Validation", "../features/quality_gates.md"),
    ],
    "GateConfig": [
        ("Quality Gates", "../features/quality_gates.md"),
    ],
    # Add more as needed
}
```

---

## Phase 6: Regenerate and Verify

### 6.1 Regenerate yaml_schema.md

```bash
python odibi/introspect.py
```

### 6.2 Verify Build

```bash
python -m mkdocs build
```

### 6.3 Lint and Format

```bash
ruff check . --fix
ruff format .
```

### 6.4 Run Tests

```bash
pytest tests/ -v
```

### 6.5 Commit and Push

```bash
git add .
git commit -m "docs: Reference enrichment campaign - beginner-friendly yaml_schema"
git push
```

---

## Files to Modify

| File | Changes |
|------|---------|
| `odibi/config.py` | Update docstrings for ~25 config classes |
| `odibi/introspect.py` | Update SECTION_INTROS, add SEE_ALSO_LINKS |
| `docs/reference/yaml_schema.md` | Auto-regenerated |

---

## Config Classes to Enrich

### Critical (Must Have)
- [ ] `NotNullTest` - Add when-to-use, see-also
- [ ] `UniqueTest` - Add when-to-use, see-also
- [ ] `FreshnessContract` - Add when-to-use, see-also
- [ ] `RowCountTest` - Add when-to-use, see-also
- [ ] `RangeTest` - Add when-to-use, see-also
- [ ] `AcceptedValuesTest` - Add when-to-use, see-also
- [ ] `RegexMatchTest` - Add when-to-use, see-also
- [ ] `VolumeDropTest` - Add when-to-use, see-also
- [ ] `SchemaContract` - Add when-to-use, see-also
- [ ] `DistributionContract` - Add when-to-use, see-also
- [ ] `ValidationConfig` - Add disambiguation, see-also
- [ ] `QuarantineConfig` - Add when-to-use, see-also
- [ ] `GateConfig` - Add when-to-use, see-also

### Important (Should Have)
- [ ] `ReadConfig` - Add full examples, see-also
- [ ] `WriteConfig` - Add full examples, see-also
- [ ] `TransformConfig` - Add examples, see-also
- [ ] `IncrementalConfig` - Add when-to-use, examples
- [ ] `DimensionPattern` - Add beginner context
- [ ] `FactPattern` - Add beginner context
- [ ] `AggregationPattern` - Add beginner context
- [ ] `DateDimensionPattern` - Add when-to-use

### Nice to Have
- [ ] `LocalConnectionConfig` - Add when-to-use
- [ ] `DeltaConnectionConfig` - Add when-to-use
- [ ] `AzureBlobConnectionConfig` - Add when-to-use
- [ ] `SQLServerConnectionConfig` - Add when-to-use
- [ ] `HttpConnectionConfig` - Add when-to-use
- [ ] `RetryConfig` - Add when-to-use
- [ ] `AlertConfig` - Add when-to-use
- [ ] `PerformanceConfig` - Add see-also

---

## Estimated Effort

| Phase | Focus | Time |
|-------|-------|------|
| 1 | Validation/Contracts/Gates disambiguation | 1 hour |
| 2 | Core configs (Read/Write/Transform) | 1.5 hours |
| 3 | Pattern configs | 1 hour |
| 4 | Connection configs | 30 min |
| 5 | Cross-references in introspect.py | 30 min |
| 6 | Regenerate, test, push | 30 min |

**Total: ~5 hours**

---

## Validation Checklist

Before marking complete:

- [ ] `python odibi/introspect.py` runs without errors
- [ ] `docs/reference/yaml_schema.md` is regenerated
- [ ] `python -m mkdocs build` passes
- [ ] `ruff check .` passes
- [ ] `ruff format .` passes (no changes)
- [ ] `pytest tests/` passes
- [ ] Git commit and push successful
