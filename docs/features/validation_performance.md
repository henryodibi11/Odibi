# Validation Performance Optimization Guide

This document details the performance optimizations made to odibi's contracts and validation system.

## Summary of Issues Found and Fixed

### 1. Critical Bottlenecks Identified

| Issue | Engine | Impact | Fix Applied |
|-------|--------|--------|-------------|
| Double scan in UNIQUE check | Pandas | 2x slower | Single `duplicated()` call with cached result |
| Full DataFrame copy for invalid rows | Pandas | O(N) memory waste | Mask-based operations only |
| Eager LazyFrame collection | Polars | Defeats optimizer | Lazy aggregations, scalar-only collects |
| Missing contract types | Polars | Inconsistent behavior | Added UNIQUE, ACCEPTED_VALUES, RANGE, REGEX, ROW_COUNT |
| Per-row test_results lists | Quarantine | O(N×tests) memory | Aggregate counts only |
| No fail-fast mode | All | Full scan even on early failure | Added `fail_fast` config option |
| No FRESHNESS minutes support | Spark/Pandas | Missing 'm' unit | Added minutes parsing |

### 2. Performance Anti-Patterns Fixed

#### Pandas Engine

**Before (UNIQUE check):**
```python
# TWO full scans + two boolean Series allocations
if df.duplicated(subset=cols).any():
    dup_count = df.duplicated(subset=cols).sum()
```

**After:**
```python
# ONE scan, cached result
dups = df.duplicated(subset=cols)
dup_count = int(dups.sum())
if dup_count > 0:
    ...
```

**Before (ACCEPTED_VALUES):**
```python
# Creates full invalid DataFrame in memory
invalid = df[~df[col].isin(test.values)]
if not invalid.empty:
    examples = invalid[col].unique()[:3]
```

**After:**
```python
# Mask-only, minimal memory
mask = ~df[col].isin(test.values)
invalid_count = int(mask.sum())
if invalid_count > 0:
    examples = df.loc[mask, col].dropna().unique()[:3]
```

#### Polars Engine

**Before:**
```python
# Forces full collection, defeats lazy optimization
if isinstance(df, pl.LazyFrame):
    df = df.collect()  # Materializes everything!
```

**After:**
```python
# Keeps lazy, collects only scalars
if is_lazy:
    row_count = df.select(pl.len()).collect().item()
    null_count = df.select(pl.col(col).is_null().sum()).collect().item()
```

#### Quarantine System

**Before:**
```python
# O(N × num_tests) memory usage
for name, mask in test_masks.items():
    test_results[name] = mask.tolist()  # Huge Python list per test!
```

**After:**
```python
# O(num_tests) memory - aggregate counts only
for name, mask in test_masks.items():
    pass_count = int(mask.sum())
    fail_count = len(df) - pass_count
    test_results[name] = {"pass_count": pass_count, "fail_count": fail_count}
```

## New Configuration Options

### Fail-Fast Mode

Stop validation on first failure for faster feedback:

```yaml
validation:
  fail_fast: true  # Stop on first failure
  tests:
    - type: not_null
      columns: [id, customer_id]
    - type: unique
      columns: [id]
```

### DataFrame Caching (Spark)

Cache DataFrame before validation when running many tests:

```yaml
validation:
  cache_df: true  # Cache for multi-test validation
  tests:
    - type: not_null
      columns: [id]
    # ... 10+ more tests
```

### Quarantine Sampling

Limit quarantined rows to prevent storage blowup:

```yaml
validation:
  tests:
    - type: not_null
      columns: [customer_id]
      on_fail: quarantine
  quarantine:
    connection: silver
    path: customers_quarantine
    max_rows: 10000         # Cap at 10K rows
    sample_fraction: 0.1    # Or sample 10% of invalid rows
```

## Engine Parity

All three engines now support the same contract types:

| Contract Type | Pandas | Polars | Spark |
|--------------|--------|--------|-------|
| not_null | ✅ | ✅ | ✅ |
| unique | ✅ | ✅ | ✅ |
| accepted_values | ✅ | ✅ | ✅ |
| row_count | ✅ | ✅ | ✅ |
| range | ✅ | ✅ | ✅ |
| regex_match | ✅ | ✅ | ✅ |
| freshness | ✅ | ✅ | ✅ |
| schema | ✅ | ✅ | ✅ |
| custom_sql | ✅ | ⚠️ Skipped | ✅ |

### FRESHNESS Duration Units

All engines now support:
- `"24h"` - hours
- `"7d"` - days
- `"30m"` - minutes (NEW)

## Benchmark Tests

Run benchmarks to verify performance:

```bash
pytest tests/benchmarks/test_validation_perf.py -v -s
```

Benchmark scenarios:
- 10 contracts on 100K rows (Pandas)
- 15 contracts on 100K rows (Pandas)
- Fail-fast vs full validation comparison
- Polars eager vs lazy comparison
- Quarantine split performance
- Memory efficiency verification

## Expected Performance Gains

| Scenario | Before | After | Improvement |
|----------|--------|-------|-------------|
| 10 contracts / 100K rows (Pandas) | ~5s | ~3s | 40% faster |
| Fail-fast on early failure | Full scan | Early exit | Up to 10x faster |
| Polars LazyFrame (10 contracts) | Eager collect | Lazy | 30-50% faster |
| Quarantine memory (100K rows, 5 tests) | ~4MB lists | ~1KB dicts | 4000x less memory |

## Best Practices

1. **Use fail-fast for CI/CD**: Quick feedback when data is clearly bad
2. **Enable cache_df for Spark**: When running 5+ contracts per node
3. **Set quarantine limits**: Prevent storage blowup on high-failure batches
4. **Prefer Polars LazyFrame**: Let the query optimizer work for you
5. **Batch similar tests**: Multiple NOT_NULL columns in one test
