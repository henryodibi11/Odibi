# Odibi Stability Campaign

## Goal

Make odibi stable and bug-free so it buys back time and freedom for a solo data engineer.

## Context

**Creator:** Solo DE on an analytics team (only DE among data analysts). Works in operations, not IT. Odibi was born from needing unique solutions due to IT roadblocks and working alone.

**Short-term:** Stable, bug-free framework
**Medium-term:** Find gaps and improve coverage  
**Ultimate:** Framework so easy it gives time back to focus on what matters

---

## Assets

### Datasets (d:/odibi/.odibi/source_cache)

| Dataset | Format | Rows | Focus Area |
|---------|--------|------|------------|
| `nyc_taxi/csv/clean` | CSV | 10,000 | DateTime parsing, numeric precision, Fact pattern |
| `nyc_taxi/csv/messy` | CSV | 5,000 | Validation rules, quarantine, null handling |
| `github_events/json` | NDJSON | 10,000 | JSON parsing, nested flattening, schema evolution |
| `tpch/parquet/lineitem` | CSV | 3,750 | Composite keys, decimal precision, Merge pattern |
| `synthetic/avro/customers` | NDJSON | 5,000 | Nested structs, arrays, Dimension pattern |
| `cdc/delta/orders` | CSV | 1,200 | MERGE/UPSERT, delete detection, SCD2 pattern |
| `northwind/sqlite` | SQLite+CSV | 16,282 | SQL ingestion, HWM incremental, FK relationships |
| `edge_cases/mixed` | CSV | 500 | All validation rules, quarantine, security |

### Engines (must maintain parity)

- **Pandas** - Quick local testing
- **Spark** - Via WSL: `wsl -d Ubuntu-20.04 -- bash -c "cd /mnt/d/odibi && python3.9 -m pytest tests/"`
- **Polars** - Lightweight alternative

### Patterns (odibi/patterns/)

- `scd2.py` - Slowly Changing Dimension Type 2
- `merge.py` - MERGE/UPSERT operations
- `aggregation.py` - Aggregation patterns
- `dimension.py` - Dimension table pattern
- `fact.py` - Fact table pattern
- `date_dimension.py` - Date dimension generation

---

## Campaign Phases

### Phase 1: Baseline Assessment

1. Run full test suite: `pytest tests/ -v`
2. Run Spark tests via WSL
3. Document all failures in BUGS.md
4. Count passing/failing per module

**Success:** Clear picture of current state

### Phase 2: Dataset-Driven Testing

For each dataset, create pipelines that exercise:

```
Raw Data → Bronze (typed) → Silver (clean/valid) → Gold (aggregated)
```

| Dataset | Bronze Test | Silver Test | Gold Test |
|---------|-------------|-------------|-----------|
| nyc_taxi_clean | CSV read, type casting | Null check, range validation | Fare aggregation by day |
| nyc_taxi_messy | Same + error handling | Quarantine bad rows | Same on clean subset |
| github_events | NDJSON parse, flatten | Dedupe by event_id | Event counts by type |
| tpch_lineitem | CSV with composite PK | Decimal precision check | Revenue by order |
| synthetic_customers | Nested struct flatten | Email validation | Customer by region |
| cdc_orders | Multi-version read | SCD2 merge | Order history |
| northwind | SQLite extraction | FK validation | Sales by category |
| edge_cases | All formats | Quarantine flow | Pass/fail counts |

**Success:** Every dataset has passing pipeline on all 3 engines

### Phase 3: Engine Parity Verification

For each pattern + dataset combination:

1. Run with Pandas engine
2. Run with Spark engine (via WSL)
3. Run with Polars engine
4. Compare outputs (schema, row counts, values)
5. Flag any differences

**Success:** Identical behavior across all engines

### Phase 4: Pattern Deep-Dive

For each pattern:

1. **SCD2**: Test with CDC orders - inserts, updates, deletes, late-arriving
2. **Merge**: Test with TPC-H - composite keys, partial updates
3. **Aggregation**: Test with NYC taxi - groupby, window functions
4. **Dimension**: Test with customers - slowly changing attributes
5. **Fact**: Test with NYC taxi - measures, dimensions, grain
6. **DateDimension**: Generate and validate date range

**Success:** All patterns have comprehensive edge case coverage

### Phase 5: Gap Analysis

Document in GAPS.md:

- Missing validations
- Usability pain points
- Missing pattern options
- Documentation gaps
- Error message improvements
- Performance issues

**Success:** Prioritized roadmap for improvements

---

## Commands Reference

```bash
# Run all tests (Windows)
pytest tests/ -v

# Run all tests (Spark via WSL)
wsl -d Ubuntu-20.04 -- bash -c "cd /mnt/d/odibi && python3.9 -m pytest tests/"

# Run specific test file
pytest tests/unit/test_scd2.py -v

# Run with coverage
pytest tests/ --cov=odibi --cov-report=html

# Lint
ruff check .
ruff check . --fix

# Format
ruff format .
```

---

## Rules

1. **Every fix needs a test** - No exceptions
2. **Engine parity** - If Pandas has it, Spark and Polars must too
3. **Use `get_logging_context()`** - Structured logging only
4. **Don't add features without tests** - Stability first
5. **Document bugs** - Add to BUGS.md before fixing

---

## File Tracking

Create these files as you work:

- `BUGS.md` - Bugs found during campaign
- `GAPS.md` - Missing features and pain points
- `tests/integration/test_source_pools.py` - Dataset integration tests

---

## Success Criteria

- [ ] All 1068+ tests passing
- [ ] All 8 datasets have working pipelines
- [ ] All 3 engines produce identical results
- [ ] All 6 patterns have edge case coverage
- [ ] BUGS.md is empty (all fixed)
- [ ] GAPS.md has prioritized roadmap
