# Simulation Feature - Implementation Overview

**For Code Review**  
**Author:** Henry Odibi  
**Date:** March 8, 2026  
**Version:** V3.2  

---

## Executive Summary

The simulation feature enables Odibi to generate synthetic data declaratively via YAML configuration, eliminating external dependencies for testing, development, and demos. It supports 11 generator types, incremental mode with HWM persistence, entity-based generation, chaos engineering, and derived columns with dependency resolution.

**Key Stats:**
- **Implementation:** 2,400 lines of production code
- **Test Coverage:** 53 unit tests (100% passing)
- **Documentation:** 700+ lines across 3 mkdocs pages
- **Generators:** 11 types (range, categorical, boolean, timestamp, sequential, constant, UUID, email, IP, geo, derived)
- **Engines:** Pandas (native), Spark & Polars (delegation)

---

## Problem Statement

**Pain Point:** Data engineers need synthetic data for:
- Testing pipelines without dependencies
- CI/CD with deterministic datasets
- Demos and prototyping
- Stress testing at scale
- Continuous dataset generation

**Existing Solutions:**
- External tools (Mockaroo, Faker) - not integrated, cost/row limits
- Manual SQL/Python scripts - not declarative, hard to maintain
- Production data copies - PII concerns, stale, large

**Odibi Solution:** Native YAML-first synthetic data generation integrated with pipeline framework, supporting incremental/continuous datasets with state management.

---

## Architecture

### Design Philosophy

**Follows Odibi's Core Principles:**
1. **YAML-first** - Zero code required
2. **Engine-agnostic** - Same config works on Pandas/Spark/Polars
3. **Deterministic** - Seeded RNG for reproducibility
4. **Incremental-aware** - Integrates with StateManager for HWM
5. **Validation-ready** - Works with contracts, quarantine, quality gates

### Module Structure

```
odibi/simulation/
├── __init__.py           # Public API exports
├── generator.py          # Core simulation engine (900 LOC)
└── README.md             # Module documentation

odibi/config.py           # Pydantic models for validation (450 LOC added)
```

### Key Components

**1. SimulationEngine (`generator.py`)**
- Core generation logic
- Entity-based RNG seeding (deterministic with hashlib.md5)
- Topological sort for derived column dependencies
- Chaos parameter application
- Multi-engine delegation

**2. Configuration Models (`config.py`)**
- `SimulationConfig` - Root configuration
- `SimulationScope` - Time range, seed, entities
- `ColumnGeneratorConfig` - Column definitions with generators
- 11 generator configs (RangeGeneratorConfig, UUIDGeneratorConfig, etc.)
- `ChaosConfig` - Outliers, duplicates, downtime
- Pydantic validation ensures correctness at config time

**3. Integration Points**
- **Read Format:** `format: simulation` in node.read
- **Incremental Mode:** HWM from `incremental.state_key`
- **StateManager:** Persists HWM across runs
- **Engines:** Pandas (native), Spark/Polars (via `generate()` delegation)

---

## Implementation Deep Dive

### 1. Entity-Based Determinism

**Challenge:** Python's `hash()` is process-randomized (Python 3.3+)

**Solution:** Use `hashlib.md5` for stable entity seeding:

```python
# Stable cross-process hash
entity_hash = hashlib.md5(entity_name.encode()).hexdigest()[:8]
entity_seed = base_seed + int(entity_hash, 16) % (2**31)
entity_rng = np.random.default_rng(entity_seed)
```

**Test:** `test_deterministic_entity_rng_across_processes` spawns subprocess to verify

### 2. Incremental RNG Advancement

**Challenge:** Incremental runs shouldn't repeat same random values

**Solution:** Advance RNG state based on rows already generated:

```python
if hwm_timestamp:
    rows_before_hwm = int(
        (effective_start_time - start_time).total_seconds() / timestep_seconds
    )
    entity_seed = base_entity_seed + rows_before_hwm
```

**Test:** `test_incremental_produces_different_values` verifies different values across runs

### 3. Derived Column Dependency Resolution

**Challenge:** Derived columns may depend on other derived columns

**Solution:** Topological sort with cycle detection:

```python
def _resolve_column_dependencies(self) -> List[ColumnGeneratorConfig]:
    """Topologically sort columns by dependency."""
    graph = {col.name: self._extract_dependencies(col) for col in columns}
    sorted_cols = topological_sort(graph)
    if has_cycle(graph):
        raise ValueError("Circular dependency detected")
    return sorted_cols
```

**Test:** `test_chain_derived_columns` verifies 3-level derived chains

### 4. Null-Safe Derived Expressions

**Challenge:** `None` values crash arithmetic expressions

**Solution:** Safe helper functions in eval namespace:

```python
safe_builtins = {
    'safe_div': lambda a, b, default=None: (a / b) if (a and b and b != 0) else default,
    'coalesce': lambda *args: next((a for a in args if a is not None), None),
}
```

**Test:** `test_multi_level_null_propagation` verifies nulls cascade through 3 levels

### 5. Chaos Parameters

**Applied AFTER derived columns:**

```python
def generate(self):
    rows = []
    for entity in entities:
        entity_rows = self._generate_entity_rows(entity)  # Includes derived
        rows.extend(entity_rows)

    rows = self._apply_chaos(rows)  # Outliers, duplicates, downtime
    return rows
```

**Why:** Derived columns use clean values, chaos simulates real-world corruption

### 6. Timestamp Standardization

**Format:** Zulu time (`2026-01-01T00:00:00Z`) not `+00:00`

**Reason:** Cross-system compatibility, HWM string comparison stability

```python
def _generate_timestamp(self, timestamp: datetime) -> str:
    return timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
```

---

## Generator Types (11 Total)

### Numeric
- **Range** - Uniform/normal distribution, min/max, outliers
- **Sequential** - Auto-increment with start/step

### Categorical
- **Categorical** - Discrete values with optional weights
- **Boolean** - True/false with probability

### Temporal
- **Timestamp** - Auto-generated from scope timestep

### Identifiers
- **UUID** - v4 (random but seeded) or v5 (deterministic from entity+row)
- **Sequential** - Integer IDs

### Text
- **Email** - Pattern-based with domain
- **Constant** - Fixed values or templates (`{entity_id}`, `{row_index}`)

### Network
- **IPv4** - Full range or subnet-constrained

### Geographic
- **Geo** - Lat/lon within bounding box, tuple/wkt/geojson output

### Computed
- **Derived** - Python expressions with topological dependency resolution

---

## Test Coverage

### Unit Tests (53 total)

**test_simulation.py (18 tests)**
- Configuration validation (scopes, entities, generators)
- Basic generator functionality
- Null rates, entity overrides
- Determinism with seeds
- Incremental mode basics

**test_derived_columns.py (9 tests)**
- Simple derived columns
- Conditional logic
- Chained dependencies (3+ levels)
- Multiple column dependencies
- Circular dependency detection
- Builtin function support
- Complex expressions

**test_simulation_fixes.py (8 tests)**
- Cross-process determinism (subprocess)
- Zero timestep validation
- Empty dataset edge cases
- UUID uniqueness (v4/v5)
- Incremental exhausted datasets

**test_p1_features.py (9 tests)**
- Incremental RNG advancement
- Null-safe helpers (safe_div, coalesce)
- Multi-level null propagation
- Timestamp Zulu format
- Common generators (email, IP, geo)

**test_simulation_coverage_gaps.py (9 tests)**
- Write mode compatibility (UUID + upsert)
- Large-scale performance (100K, 500K rows)
- Chaos + derived columns
- Entity overrides + incremental
- Edge case combinations

### Integration Tests (7 skipped)

**Status:** Skipped pending ReadConfig API updates
- `test_incremental_simulation.py` (3 tests)
- `test_simulation_pipeline.py` (4 tests)

**Note:** Core functionality validated via unit tests

---

## Performance Characteristics

### Memory Profile
- **100K rows:** ~10 MB (Pandas DataFrame)
- **500K rows:** ~50 MB (10 entities × 50K rows)
- **Limitation:** In-memory generation (no streaming yet)

**Validated in tests:**
- `test_100k_rows_generation` - 100K rows, single entity
- `test_500k_rows_multi_entity` - 500K rows, 10 entities
- `test_derived_columns_at_scale` - 100K rows with calculations

### Generation Speed
- **Pandas:** ~10K rows/sec (simple generators)
- **Spark:** Delegates to Pandas, then parallelizes
- **Polars:** Delegates to Pandas, converts to Polars

---

## Code Quality

### Validation Strategy
- **Pydantic models** - Config validation at parse time
- **Type hints** - Full type coverage
- **Docstrings** - All public methods documented

### Error Handling
- **Clear messages** - "Timestep must be positive" not "ValueError"
- **Early validation** - Fail fast on config errors
- **Circular deps** - Detected with actionable error

### Logging
- **Structured logging** - Uses `get_logging_context()`
- **Progress indicators** - Log every 10 entities for large datasets
- **Debug traces** - RNG state, dependency order

---

## Critical Fixes (V3.2)

**Oracle gap analysis identified 22 issues - 4 P0 critical fixed:**

### P0-1: Non-Deterministic Entity RNG ✅
- **Issue:** `hash()` is process-randomized
- **Fix:** Use `hashlib.md5` for stable hashing
- **Test:** Subprocess verification

### P0-2: Zero Timestep Not Validated ✅
- **Issue:** `timestep: "0s"` allowed, causes infinite loops
- **Fix:** Validation: `if seconds <= 0: raise ValueError`
- **Test:** `test_zero_timestep_rejected`

### P0-3: End Time Edge Case ✅
- **Issue:** `end < start` produced 1 phantom row
- **Fix:** `max(0, ...)` not `max(1, ...)`
- **Test:** `test_end_time_before_start_produces_empty_dataset`

### P0-4: Missing UUID Generator ✅
- **Issue:** No unique key support for upsert modes
- **Fix:** Added UUID4 (random) and UUID5 (deterministic)
- **Test:** `test_uuid_generator_for_upsert_mode`

---

## Usage Examples

### Basic Simulation

```yaml
read:
  format: simulation
  options:
    simulation:
      scope:
        start_time: "2026-01-01T00:00:00Z"
        timestep: "5m"
        row_count: 1000
        seed: 42

      entities:
        count: 3
        id_prefix: "sensor_"

      columns:
        - name: sensor_id
          data_type: string
          generator: {type: constant, value: "{entity_id}"}

        - name: timestamp
          data_type: timestamp
          generator: {type: timestamp}

        - name: temperature
          data_type: float
          generator: {type: range, min: 60, max: 80}
```

### With Derived Columns

```yaml
columns:
  - name: input
    data_type: float
    generator: {type: range, min: 1, max: 100}

  - name: output
    data_type: float
    generator: {type: range, min: 0, max: 100}

  - name: efficiency
    data_type: float
    generator:
      type: derived
      expression: "(output / input * 100) if input > 0 else 0"
```

### With Chaos

```yaml
columns:
  - name: value
    data_type: float
    generator: {type: range, min: 50, max: 100}
    null_rate: 0.05  # 5% nulls

chaos:
  outlier_rate: 0.02
  outlier_factor: 5.0
  duplicate_rate: 0.01
```

### Incremental Mode

```yaml
read:
  format: simulation
  options:
    simulation:
      scope:
        start_time: "2026-01-01T00:00:00Z"
        timestep: "1h"
        row_count: 24  # 24 hours per run
        seed: 42
      # ... columns ...

  incremental:
    mode: stateful
    column: timestamp
    state_key: telemetry_hwm
```

**First run:** Generates 24 hours  
**Second run:** Generates next 24 hours (HWM advanced automatically)

---

## Design Decisions

### Why Entity-Based?
- **Domain agnostic** - Works for sensors, users, devices, products
- **Realistic variability** - Each entity has unique RNG stream
- **Overrides** - Entity-specific parameter tuning

### Why Not Faker?
- **Dependencies** - Avoid heavy external libs
- **Integration** - Native to pipeline framework
- **Incremental** - Faker doesn't support stateful generation
- **Performance** - Pure NumPy/Pandas is faster

### Why Chaos After Derived?
- **Clean inputs** - Derived columns use valid data
- **Realistic corruption** - Outliers/nulls simulate real-world issues
- **Testability** - Easier to reason about derived logic

### Why Topological Sort?
- **Flexibility** - Define columns in any order
- **Safety** - Detect circular dependencies early
- **Clarity** - Users don't think about execution order

---

## Future Enhancements (Backlog)

### P2: Medium Priority
- **Streaming generation** - Batch mode for >10M rows
- **Write mode validation** - Ensure upsert has keys
- **HWM reset CLI** - `odibi state reset <key>`
- **Progress logging** - Better UX for large datasets

### P3-P4: Nice-to-Have
- **Variable timestep** - Poisson arrivals for event-driven
- **Cross-entity FKs** - Reference other simulated entities
- **Time-series patterns** - Seasonality, trends, autocorrelation
- **Correlation modeling** - Enforce relationships between columns

---

## Documentation

### User-Facing
- **`docs/features/simulation.md`** - Overview and concepts
- **`docs/guides/simulation.md`** - Detailed guide with examples
- **`docs/reference/simulation_generators.md`** - Quick reference

### Developer-Facing
- **`odibi/simulation/README.md`** - Module architecture
- **`docs/ODIBI_DEEP_CONTEXT.md`** - Framework integration (2,200+ lines)
- **This document** - Implementation overview

### CLI Introspection
```bash
odibi list transformers        # Includes simulation info
odibi explain simulation        # Detailed docs
odibi templates show simulation # YAML examples
```

---

## Review Checklist

### Architecture
- ✅ Follows Odibi's YAML-first philosophy
- ✅ Engine-agnostic (Pandas/Spark/Polars)
- ✅ Integrates with existing features (incremental, validation, state)
- ✅ Clean separation of concerns (config vs execution)

### Code Quality
- ✅ Pydantic validation for all configs
- ✅ Type hints throughout
- ✅ Structured logging with context
- ✅ Clear error messages

### Testing
- ✅ 53 unit tests (100% passing)
- ✅ Edge cases covered (nulls, empty datasets, circular deps)
- ✅ Cross-process determinism verified
- ✅ Large-scale performance validated (500K rows)

### Documentation
- ✅ User guides (features, guides, reference)
- ✅ Code comments minimal (self-documenting)
- ✅ Examples in docs
- ✅ CLI introspection support

### Security
- ✅ Safe expression evaluation (restricted namespace, no imports)
- ✅ No file I/O in derived columns
- ✅ No system calls
- ✅ Deterministic RNG (not cryptographic, by design)

---

## Questions for Reviewer

1. **Scope:** Is the feature set appropriate for V1 release?
2. **API:** Is the YAML schema intuitive and extensible?
3. **Performance:** Are memory limits (10M rows) acceptable for initial release?
4. **Testing:** Are skipped integration tests a concern, or do unit tests suffice?
5. **Chaos Design:** Is applying chaos after derived columns the right choice?

---

## Conclusion

The simulation feature is production-ready for initial release:
- **Comprehensive:** 11 generators, chaos, derived columns, incremental
- **Well-tested:** 53 unit tests, all P0 critical issues fixed
- **Documented:** 700+ lines of user documentation
- **Integrated:** Works seamlessly with Odibi's existing features

**Recommendation:** Ship as V3.2 with simulation feature flag enabled by default.

**Next Steps:**
1. Merge to main (done)
2. GitHub Actions verification (in progress)
3. User feedback collection
4. P2 enhancements based on usage patterns
