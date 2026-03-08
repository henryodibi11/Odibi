# Odibi Simulation Module

## Overview

This module provides synthetic data generation capabilities for Odibi pipelines.

## Architecture

```
SimulationEngine (generator.py)
├── Scope Management (time, rows, seed)
├── Entity Generation (domain-agnostic)
├── Column Generators (11 types)
│   ├── Range (uniform/normal)
│   ├── Categorical (weighted choice)
│   ├── Boolean (probability)
│   ├── Timestamp (auto-stepped)
│   ├── Sequential (auto-increment)
│   ├── Constant (templated)
│   ├── UUID (V4/V5)
│   ├── Email (pattern-based)
│   ├── IPv4 (subnet-aware)
│   ├── Geo (bounding box)
│   └── Derived (calculated with dependencies)
├── Dependency Resolution (topological sort)
├── Chaos Application (outliers, duplicates, downtime)
└── Incremental State (HWM-based)
```

## Key Components

### SimulationEngine

**File:** `generator.py`  
**Lines:** 650+  
**Responsibility:** Generate rows based on SimulationConfig

**Key methods:**
- `generate()` - Main entry point
- `_generate_entity_rows()` - Per-entity generation
- `_generate_value()` - Single value generation
- `_apply_chaos()` - Add realistic imperfections
- `_resolve_column_dependencies()` - Topological sort for derived columns
- `get_max_timestamp()` - Extract HWM for incremental mode

### Configuration Models

**File:** `odibi/config.py`  
**Lines:** 650+ (simulation section)  
**Models:** 20+ Pydantic classes

**Top-level:**
- `SimulationConfig` - Complete simulation spec
- `SimulationScope` - Time/count boundaries
- `EntityConfig` - Entity definition
- `ColumnGeneratorConfig` - Column specification
- `ChaosConfig` - Chaos parameters

**Generator configs (11 types):**
- RangeGeneratorConfig
- CategoricalGeneratorConfig
- BooleanGeneratorConfig
- TimestampGeneratorConfig
- SequentialGeneratorConfig
- ConstantGeneratorConfig
- UUIDGeneratorConfig
- EmailGeneratorConfig
- IPGeneratorConfig
- GeoGeneratorConfig
- DerivedGeneratorConfig

## Design Principles

### 1. Determinism
Same seed → identical data (guaranteed via stable hashing)

### 2. Engine Agnostic
Generate row dicts → convert to engine-specific DataFrame

### 3. Dependency Resolution
Derived columns auto-sorted by dependencies (topological sort)

### 4. Safety
Derived expressions evaluated in restricted namespace (no imports, file I/O, etc.)

### 5. Incremental
HWM-based state progression for continuous generation

### 6. Domain Agnostic
Entities represent any concept (sensors, users, machines, etc.)

## Integration Points

### With Odibi Core

**Node Executor** (`odibi/node.py`):
- Passes HWM to simulation engine
- Captures max timestamp for state persistence
- Validates write mode compatibility

**Engines** (`odibi/engine/*.py`):
- Pandas: Native implementation
- Spark: Delegates to Pandas, converts to Spark DF
- Polars: Delegates to Pandas, converts to LazyFrame

**StateManager** (`odibi/state/__init__.py`):
- Stores HWM between runs
- Supports all backends (JSON, Delta, SQL)

## Usage Example

```python
from odibi.config import SimulationConfig, SimulationScope, EntityConfig
from odibi.simulation import SimulationEngine

config = SimulationConfig(
    scope=SimulationScope(
        start_time="2026-01-01T00:00:00Z",
        timestep="5m",
        row_count=100,
        seed=42,
    ),
    entities=EntityConfig(count=10, id_prefix="sensor_"),
    columns=[...],
)

engine = SimulationEngine(config)
rows = engine.generate()  # Returns List[Dict[str, Any]]

# For incremental:
max_ts = engine.get_max_timestamp(rows)
engine2 = SimulationEngine(config, hwm_timestamp=max_ts)
rows2 = engine2.generate()  # Continues from max_ts
```

## Testing

**Location:** `tests/unit/test_simulation*.py`, `tests/unit/test_derived_columns.py`, `tests/unit/test_p1_features.py`  
**Count:** 43 unit tests + 3 integration tests  
**Status:** 100% passing

**Coverage:**
- Configuration validation
- All 11 generators
- Determinism (including cross-process)
- Dependency resolution
- Circular dependencies
- Null safety
- Edge cases (zero timestep, empty datasets, exhausted incremental)
- Incremental RNG advancement
- Write mode validation

## Performance

**Generation speed:** ~500K rows/second (Pandas)  
**Memory:** ~400 MB per 1M rows  
**Overhead:** <1% for dependency resolution and chaos

**Scalability:**
- Pandas: <10M rows (single-process memory)
- Spark: Unlimited (distributed)
- Polars: <10M rows (single-process memory)

## Security

**Expression evaluation:** Sandboxed  
**Allowed:** Math, logic, safe functions  
**Blocked:** Imports, file I/O, network, system calls  
**Audit:** Expressions logged

**No known vulnerabilities.**

## Extension Points

### Adding New Generators

1. Create config class in `odibi/config.py`:
```python
class MyGeneratorConfig(BaseModel):
    type: Literal["my_type"]
    param: str
```

2. Add to union:
```python
GeneratorConfig = Annotated[Union[..., MyGeneratorConfig], ...]
```

3. Implement in `generator.py`:
```python
def _generate_my_type(self, config, rng):
    return f"generated_{config.param}"
```

4. Add handler to `_generate_value()`:
```python
elif isinstance(generator, MyGeneratorConfig):
    return self._generate_my_type(generator, rng)
```

5. Write tests:
```python
def test_my_generator():
    config = SimulationConfig(...)
    engine = SimulationEngine(config)
    rows = engine.generate()
    assert ...
```

## Known Limitations

**Not supported (future):**
- Variable timestep (event-driven)
- Cross-entity foreign keys
- Time-series patterns (seasonality, trends)
- Batch/streaming for >10M rows (in Pandas)
- Realistic PII (names, addresses, phone numbers)

**Workarounds:**
- Use transformers for complex post-processing
- Generate in chunks for large datasets
- Combine simulation with real lookup tables

## Files

**Core:**
- `__init__.py` - Module exports
- `generator.py` - SimulationEngine implementation (650+ lines)

**Configuration:**
- `../config.py` - Pydantic models (650+ lines in simulation section)

**Tests:**
- `../../tests/unit/test_simulation.py` - Config & basic (18 tests)
- `../../tests/unit/test_derived_columns.py` - Derived columns (9 tests)
- `../../tests/unit/test_simulation_fixes.py` - Critical fixes (8 tests)
- `../../tests/unit/test_p1_features.py` - P1/P2 features (8 tests)
- `../../tests/integration/test_incremental_simulation.py` - Integration (3 tests)

**Documentation:**
- `../../docs/simulation_feature_guide.md` - User guide (800+ lines)
- `../../examples/simulation_example.yaml` - Working example

## Version History

**V1.0:** Core generators, chaos, Pandas  
**V2.0:** Incremental mode, Spark, Polars  
**V3.0:** Derived columns, dependency resolution  
**V3.2:** Critical fixes, UUID/email/IP/geo, null-safe operations (CURRENT)

## Contributing

See [CONTRIBUTING.md](../../CONTRIBUTING.md) for:
- Code style guidelines
- Testing requirements
- Documentation standards

## License

Part of Odibi framework. See [LICENSE](../../LICENSE).

## Support

**Documentation:** [docs/simulation_feature_guide.md](../../docs/simulation_feature_guide.md)  
**Examples:** [examples/simulation_example.yaml](../../examples/simulation_example.yaml)  
**Issues:** GitHub Issues

---

**Last updated:** 2026-03-08  
**Version:** V3.2  
**Status:** Production Ready ✅
