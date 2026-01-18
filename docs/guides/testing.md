# Testing Guide

Test your Odibi pipelines with built-in utilities for assertions, fixtures, and deterministic data.

## Overview

Odibi provides testing utilities in `odibi.testing`:

- **Assertions**: Compare DataFrames and schemas
- **Fixtures**: Generate sample data and temporary directories
- **Source Pools**: Deterministic, frozen test data for replay

## Assertions

### assert_frame_equal

Compare two DataFrames for equality (supports Pandas and Spark):

```python
from odibi.testing.assertions import assert_frame_equal

# Compare two DataFrames
assert_frame_equal(actual_df, expected_df)

# With options
assert_frame_equal(
    actual_df,
    expected_df,
    check_dtype=True,      # Check column types
    check_exact=False,     # Allow float tolerance
    atol=1e-8,             # Absolute tolerance
    rtol=1e-5              # Relative tolerance
)
```

### assert_schema_equal

Compare schemas (column names and types):

```python
from odibi.testing.assertions import assert_schema_equal

assert_schema_equal(df_a, df_b)
```

## Fixtures

### temp_directory

Create a temporary directory that auto-cleans:

```python
from odibi.testing.fixtures import temp_directory

with temp_directory() as temp_dir:
    path = os.path.join(temp_dir, "test.csv")
    df.to_csv(path)
    # Directory is deleted after context exits
```

### generate_sample_data

Generate sample DataFrames for testing:

```python
from odibi.testing.fixtures import generate_sample_data

# Default schema: id (int), value (float), category (str), timestamp (date)
df = generate_sample_data(rows=100)

# Custom schema
df = generate_sample_data(
    rows=50,
    engine_type="spark",  # or "pandas"
    schema={
        "user_id": "int",
        "score": "float",
        "name": "str",
        "created_at": "date"
    }
)
```

## Unit Testing Nodes

Test individual pipeline nodes with mock data:

```python
from odibi.pipeline import PipelineManager
import pandas as pd

manager = PipelineManager.from_yaml("odibi.yaml")
pipeline = manager.get_pipeline("main_etl")

# Mock input data
mock_data = {
    "read_customers": pd.DataFrame([
        {"id": 1, "email": "BAD_EMAIL"},
        {"id": 2, "email": "good@test.com"}
    ])
}

# Run single node with mock
result = pipeline.run_node("clean_customers", mock_data=mock_data)

# Assert output
output_df = pipeline.context.get("clean_customers")
assert len(output_df) == 1
```

## Source Pools

For deterministic, replayable tests, see [Source Pools Design](../source_pools_design.md).

Source pools provide:

- **Frozen data**: Hash-verified, immutable test datasets
- **Quality variants**: Clean, messy, and mixed data
- **Schema definitions**: Explicit, no runtime inference
- **Test coverage hints**: Know what scenarios each pool covers

## End-to-End Test Campaign

For comprehensive validation of core patterns, run the test campaign:

```bash
python scripts/run_test_campaign.py
```

This validates:

| Phase | What It Tests |
|-------|---------------|
| Phase 1 | CSV read, Parquet write, schema validation |
| Phase 3 | State/HWM persistence |
| Phase 4 | Merge pattern (upsert) |
| Phase 5 | SCD2 pattern |
| Phase 6 | Logical path resolution |
| Phase 11 | 10k row scaling |

All phases run on the Pandas engine. For Spark validation, see [Spark Engine Testing](#spark-engine-testing).

## Spark Engine Testing

The Spark engine is validated in production on Databricks rather than in CI due to JVM/environment complexity.

**Local Spark testing (WSL required on Windows):**

```bash
wsl -d Ubuntu-20.04 -- bash -c "cd /mnt/d/odibi && python3.9 -m pytest tests/ -k spark"
```

**Databricks validation:**
- Deploy pipeline to Databricks workspace
- Run with `engine: spark` configuration
- Validate outputs match Pandas engine results

**Mock-based Spark tests (no JVM required):**

```python
# tests/integration/test_patterns_spark_mock.py
# Uses mocked SparkSession to test logic without real Spark
```

## Related

- [Python API Guide](python_api_guide.md) — Programmatic pipeline execution
- [Source Pools Design](../source_pools_design.md) — Deterministic test data
- [Best Practices](best_practices.md) — Testing recommendations
- [Spark Engine Tutorial](../tutorials/spark_engine.md) — Spark-specific setup
