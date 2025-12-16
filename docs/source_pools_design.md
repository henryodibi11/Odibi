# Source Pools Design Document

**Phase**: 7.B.1 (Preparation Only)  
**Status**: Design Complete  
**Last Updated**: 2024-12-14

---

## 1. Overview

### 1.1 Purpose

Source Pools provide **deterministic, replayable test data** that exercises all supported Odibi data types and ingestion paths. They enable:

- Reproducible test runs across environments
- Coverage of all file formats and source types
- Testing of both clean and messy data scenarios
- Cryptographic verification of data integrity

### 1.2 Design Principles

| Principle | Description |
|-----------|-------------|
| **Determinism** | All data is pre-generated and frozen. No random generation at runtime. |
| **Explicit Schemas** | No schema inference. All types declared upfront. |
| **Disk-Backed** | All sources are local files with SHA256 hashes. |
| **Immutability** | Frozen pools cannot be modified during test cycles. |
| **Discoverability** | Central index enables programmatic pool discovery. |

### 1.3 Non-Goals

- ❌ Runtime data generation
- ❌ Live API connections
- ❌ Authentication-requiring sources
- ❌ Agent autonomy to modify pools during cycles
- ❌ Schema inference at runtime

---

## 2. SourcePool Schema

### 2.1 Pydantic Model Location

```
odibi/testing/source_pool.py
```

### 2.2 Core Classes

| Class | Purpose |
|-------|---------|
| `FileFormat` | Enum: csv, json, parquet, avro, delta |
| `SourceType` | Enum: local, adls_emulated, azure_blob_emulated, sql_jdbc_local, cloudfiles |
| `DataQuality` | Enum: clean, messy, mixed |
| `PoolStatus` | Enum: draft, frozen, deprecated |
| `ColumnSchema` | Column definition (name, dtype, nullable, etc.) |
| `TableSchema` | Table schema with columns and keys |
| `DataCharacteristics` | Metadata about data properties |
| `IntegrityManifest` | SHA256 hashes for frozen pools |
| `SourcePoolConfig` | Main pool definition |
| `SourcePoolIndex` | Registry of all pools |

### 2.3 Schema Example

```yaml
pool_id: nyc_taxi_csv_clean
version: "1.0.0"
name: "NYC Taxi Trips - CSV Clean"
file_format: csv
source_type: local
data_quality: clean
schema:
  columns:
    - name: trip_id
      dtype: string
      nullable: false
      primary_key: true
    - name: fare_amount
      dtype: float64
      nullable: false
  primary_keys: [trip_id]
cache_path: "nyc_taxi/csv/clean/"
characteristics:
  row_count: 10000
  has_nulls: false
status: frozen
integrity:
  algorithm: sha256
  file_hashes:
    "data.csv": "abc123..."
  manifest_hash: "def456..."
  frozen_at: "2024-12-14T00:00:00Z"
```

---

## 3. Proposed Source Pools

### 3.1 Coverage Matrix

| Pool ID | Format | Source Type | Quality | Row Count | Primary Use Case |
|---------|--------|-------------|---------|-----------|------------------|
| `nyc_taxi_csv_clean` | CSV | LOCAL | Clean | 10,000 | Bronze ingestion baseline |
| `nyc_taxi_csv_messy` | CSV | LOCAL | Messy | 5,000 | Validation & quarantine |
| `github_events_json_clean` | JSON | LOCAL | Clean | 10,000 | JSON parsing, nested structures |
| `tpch_lineitem_parquet` | Parquet | LOCAL | Clean | 60,175 | Columnar reads, partitioning |
| `synthetic_customers_avro` | Avro | LOCAL | Clean | 5,000 | Schema evolution, complex types |
| `cdc_orders_delta` | Delta | LOCAL | Clean | 1,200 | CDC/MERGE, time travel |
| `northwind_sqlite` | CSV* | SQL_JDBC_LOCAL | Clean | 830 | SQL ingestion, HWM |
| `edge_cases_mixed` | CSV | LOCAL | Messy | 500 | All edge cases, regression |

*Extracted from SQLite database

### 3.2 Format Coverage

- ✅ **CSV** (3 pools)
- ✅ **JSON** (1 pool)
- ✅ **Parquet** (1 pool)
- ✅ **Avro** (1 pool)
- ✅ **Delta** (1 pool)

### 3.3 Source Type Coverage

- ✅ **LOCAL** (6 pools)
- ✅ **SQL_JDBC_LOCAL** (1 pool)
- 🔲 **ADLS_EMULATED** (planned Phase 7.C)
- 🔲 **AZURE_BLOB_EMULATED** (planned Phase 7.C)
- 🔲 **CLOUDFILES** (planned Phase 7.C)

### 3.4 Data Quality Coverage

- ✅ **Clean** (6 pools)
- ✅ **Messy** (2 pools)

---

## 4. Recommended Public Datasets

### 4.1 Real Datasets for Local Caching

| Dataset | Source | License | Use For |
|---------|--------|---------|---------|
| **NYC TLC Taxi Data** | [nyc.gov/tlc](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) | Public Domain | CSV ingestion, large files |
| **GitHub Archive** | [gharchive.org](https://www.gharchive.org/) | CC BY 4.0 | JSON/NDJSON, nested structures |
| **TPC-H** | [tpc.org](http://www.tpc.org/tpch/) | TPC Fair Use | Parquet, benchmarking |
| **Northwind Database** | [GitHub](https://github.com/jpwhite3/northwind-SQLite3) | Public Domain | SQLite/JDBC testing |
| **Faker-based Synthetic** | Generated locally | CC0 | Avro, controlled schema |

### 4.2 Data Preparation Guidelines

1. **Download once** → Store in `.odibi/source_cache/`
2. **Subset if needed** → Keep pools < 100MB each
3. **Convert formats** → Generate Parquet/Avro from CSV
4. **Freeze with hashes** → Generate `IntegrityManifest`
5. **Never re-download during cycles** → All data pre-staged

---

## 5. Disk Layout

### 5.1 Directory Structure

```
.odibi/
├── source_cache/                    # Actual data files
│   ├── nyc_taxi/
│   │   ├── csv/
│   │   │   ├── clean/
│   │   │   │   └── data.csv         # Frozen data file
│   │   │   └── messy/
│   │   │       └── data.csv
│   │   └── parquet/                 # Future: same data in Parquet
│   ├── github_events/
│   │   └── json/
│   │       └── clean/
│   │           └── events.ndjson
│   ├── tpch/
│   │   └── parquet/
│   │       └── lineitem/
│   │           ├── l_shipdate_year=1992/
│   │           │   └── part-0000.parquet
│   │           └── ...
│   ├── synthetic/
│   │   └── avro/
│   │       └── customers/
│   │           └── customers.avro
│   ├── cdc/
│   │   └── delta/
│   │       └── orders/
│   │           ├── _delta_log/      # Delta transaction log
│   │           └── part-0000.parquet
│   ├── northwind/
│   │   └── sqlite/
│   │       └── northwind.db         # SQLite database file
│   └── edge_cases/
│       └── mixed/
│           ├── data.csv
│           ├── data.json
│           └── data.parquet
│
└── source_metadata/                 # Pool definitions & index
    ├── pool_index.yaml              # Central registry
    └── pools/                       # Individual pool metadata
        ├── nyc_taxi_csv_clean.yaml
        ├── nyc_taxi_csv_messy.yaml
        ├── github_events_json_clean.yaml
        ├── tpch_lineitem_parquet.yaml
        ├── synthetic_customers_avro.yaml
        ├── cdc_orders_delta.yaml
        ├── northwind_sqlite.yaml
        └── edge_cases_mixed.yaml
```

### 5.2 File Naming Conventions

| Component | Convention | Example |
|-----------|------------|---------|
| Pool ID | `snake_case` | `nyc_taxi_csv_clean` |
| Cache Path | `{dataset}/{format}/{quality}/` | `nyc_taxi/csv/clean/` |
| Metadata File | `{pool_id}.yaml` | `nyc_taxi_csv_clean.yaml` |
| Data Files | Format-specific | `data.csv`, `events.ndjson`, `part-*.parquet` |

---

## 6. Registration, Freezing, and Indexing

### 6.1 Pool Lifecycle

```
┌─────────────────────────────────────────────────────────────┐
│                      POOL LIFECYCLE                         │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│   DRAFT ──────────────> FROZEN ──────────────> DEPRECATED   │
│     │                      │                       │        │
│     │ - Schema defined     │ - Data prepared       │ - EOL  │
│     │ - No data yet        │ - Hashes computed     │ - Keep │
│     │ - Can modify         │ - IMMUTABLE           │   for  │
│     │                      │ - Used in tests       │   ref  │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 6.2 Freezing Process

1. **Validate Schema**: Ensure all columns match actual data
2. **Compute File Hashes**: SHA256 for each file in `cache_path`
3. **Compute Manifest Hash**: SHA256 of sorted `{path: hash}` pairs
4. **Update Metadata**: Set `status: frozen`, add `integrity` block
5. **Update Index**: Ensure pool is registered in `pool_index.yaml`

### 6.3 Verification Process

```python
def verify_pool(pool: SourcePoolConfig) -> bool:
    """Verify frozen pool integrity."""
    if pool.status != PoolStatus.FROZEN:
        return True  # Only verify frozen pools

    for file_path, expected_hash in pool.integrity.file_hashes.items():
        actual_hash = compute_sha256(pool.cache_path / file_path)
        if actual_hash != expected_hash:
            return False
    return True
```

### 6.4 Index Operations

| Operation | Description | When |
|-----------|-------------|------|
| `register_pool()` | Add pool to index | After creating metadata |
| `freeze_pool()` | Compute hashes, update status | After data is ready |
| `deprecate_pool()` | Mark as deprecated | Before removal |
| `list_pools()` | Enumerate all pools | Test discovery |
| `get_pool()` | Load pool by ID | Test setup |
| `verify_pool()` | Check integrity | Before test run |

---

## 7. Agent Rules

### 7.1 What Agents MAY Do with Sources

| Action | Allowed | Notes |
|--------|---------|-------|
| **Read** pool metadata | ✅ YES | Discovery and planning |
| **Read** cached data files | ✅ YES | For test execution |
| **List** available pools | ✅ YES | Test coverage analysis |
| **Verify** pool integrity | ✅ YES | Pre-test validation |
| **Use** pools in pipelines | ✅ YES | Primary purpose |
| **Report** on pool coverage | ✅ YES | Observability |

### 7.2 What Agents MAY NOT Do with Sources

| Action | Forbidden | Reason |
|--------|-----------|--------|
| **Modify** frozen data files | ❌ NO | Breaks determinism |
| **Delete** pool metadata | ❌ NO | Breaks registry |
| **Create** new pools during cycles | ❌ NO | Phase 7.B.1 constraint |
| **Download** live data during cycles | ❌ NO | No network during tests |
| **Change** pool status | ❌ NO | Lifecycle is controlled |
| **Add** entries to `pool_index.yaml` | ❌ NO | Human-controlled |
| **Infer** schemas at runtime | ❌ NO | Explicit schemas only |

### 7.3 Enforcement Invariants

```python
# These invariants MUST hold during any test cycle:

INVARIANT_1 = "pool_index.yaml is read-only during execution"
INVARIANT_2 = "source_cache/ contents match integrity manifests"
INVARIANT_3 = "No network calls during pool access"
INVARIANT_4 = "Schema comes from metadata, not from data inspection"
INVARIANT_5 = "Frozen pools are immutable"
```

---

## 8. Implementation Recommendations

### 8.1 Phase 7.B.2 (Next Steps)

1. **Data Preparation Script**
   - Download public datasets
   - Convert to target formats
   - Subset to manageable sizes
   - Generate hash manifests

2. **Freeze Tool**
   ```bash
   python -m odibi.testing.freeze_pool --pool-id nyc_taxi_csv_clean
   ```

3. **Verification Tool**
   ```bash
   python -m odibi.testing.verify_pools --all
   ```

### 8.2 Integration Points

| Component | Integration |
|-----------|-------------|
| `TestRunner` | Load pools via `SourcePoolIndex` |
| `ReadConfig` | Accept `source_pool` as input type |
| `ExecutionGateway` | Verify pool integrity before cycle |
| `Story` | Record which pools were used |

### 8.3 Future Extensions

- **ADLS Emulator**: Azurite for blob storage testing
- **CloudFiles Simulation**: Directory-based auto-ingest
- **Schema Evolution**: Multiple versions per pool
- **Streaming Pools**: Simulated real-time data feeds

---

## 9. Appendix

### 9.1 Example Integrity Manifest

```yaml
integrity:
  algorithm: sha256
  file_hashes:
    "data.csv": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    "schema.json": "d7a8fbb307d7809469ca9abcb0082e4f8d5651e46d3cdb762d02d0bf37c9e592"
  manifest_hash: "a948904f2f0f479b8f8564cbf12dac6b89824ca8f989c8c44c3a"
  frozen_at: "2024-12-14T10:30:00Z"
  frozen_by: "prep-script-v1"
```

### 9.2 Valid Data Types

| Type | Python | Spark | Description |
|------|--------|-------|-------------|
| `string` | `str` | `StringType` | Text data |
| `int64` | `int` | `LongType` | 64-bit integer |
| `float64` | `float` | `DoubleType` | 64-bit float |
| `bool` | `bool` | `BooleanType` | Boolean |
| `datetime` | `datetime` | `TimestampType` | Timestamp |
| `date` | `date` | `DateType` | Date only |
| `binary` | `bytes` | `BinaryType` | Raw bytes |

### 9.3 Edge Case Categories

| Category | Examples |
|----------|----------|
| **Unicode** | Emoji, RTL text, combining characters |
| **Null Variants** | NULL, empty string, whitespace-only |
| **Numeric Edge** | NaN, Inf, -Inf, MAX_INT, MIN_INT, -0 |
| **Date Edge** | 1970-01-01, 2000-01-01, 2038-01-19, far future |
| **Injection** | SQL injection, JSON breaking, CSV escaping |
| **Length** | Empty, single char, 10KB string |
| **Duplicates** | Exact duplicates, case-insensitive duplicates |
