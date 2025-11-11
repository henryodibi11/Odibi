# ODIBI Framework - Phase 1 Test Results

**Date:** November 5, 2025  
**Tests Run:** 55  
**Status:** ✅ All Passed  
**Duration:** 0.35s

---

## Summary

All foundational components are **working and validated**:
- ✅ **Config validation** (25 tests) - Pydantic schemas catch errors
- ✅ **Context API** (12 tests) - Data passing works correctly
- ✅ **Function Registry** (18 tests) - Transform functions validated

---

## What We Proved

### 1. Config Validation (25 tests)

**What it does:** Validates YAML configs using Pydantic before execution

**Key capabilities tested:**

✅ **ReadConfig** - Validates data sources
- Accepts either `path` or `table` (not both required)
- Stores format, connection, and options
- Rejects configs missing both path and table

```python
# Valid config
ReadConfig(
    connection="local",
    format="csv",
    path="data/input.csv"
)

# Invalid - missing path/table (caught by validation!)
ReadConfig(connection="local", format="csv")  # ❌ ValueError
```

✅ **WriteConfig** - Validates data outputs
- Default mode is `OVERWRITE`
- Can set to `APPEND`
- Validates same as ReadConfig

✅ **NodeConfig** - Validates pipeline nodes
- **Requires at least one operation** (read/transform/write)
- Accepts dependencies via `depends_on`
- Validates cache flag
- Rejects empty nodes

```python
# Invalid - no operations
NodeConfig(name="empty")  # ❌ "must have at least one of: read, transform, write"

# Valid - has read operation
NodeConfig(
    name="load_data",
    read=ReadConfig(...)
)  # ✅
```

✅ **PipelineConfig** - Validates full pipelines
- **Rejects duplicate node names** (critical!)
- Validates all nodes
- Stores pipeline metadata

```python
# Invalid - duplicate names
PipelineConfig(
    pipeline="test",
    nodes=[
        NodeConfig(name="duplicate", read=...),
        NodeConfig(name="duplicate", read=...)  # ❌ Duplicate!
    ]
)
```

✅ **ProjectConfig** - Validates project settings
- Default engine is `pandas`
- Can override to `spark`
- Has sensible defaults for retry/logging/story
- Stores connections

**Why this matters:**
- Errors caught at **config load time**, not runtime
- Clear error messages show **exactly what's wrong**
- No need to run the pipeline to find config mistakes

---

### 2. Context API (12 tests)

**What it does:** Passes data between nodes (like variables between functions)

**Key capabilities tested:**

✅ **Register and retrieve DataFrames**
```python
ctx = PandasContext()
df = pd.DataFrame({"a": [1, 2, 3]})

ctx.register("my_data", df)
retrieved = ctx.get("my_data")  # Gets the same DataFrame
```

✅ **Error handling when DataFrame not found**
```python
ctx.get("missing")  
# ❌ KeyError: "DataFrame 'missing' not found in context. Available: df1, df2"
```
Shows **available DataFrames** in error message - critical for debugging!

✅ **Check if DataFrame exists**
```python
ctx.has("my_data")  # True
ctx.has("does_not_exist")  # False
```

✅ **List all registered names**
```python
ctx.list_names()  # ['my_data', 'reference', 'cleaned']
```

✅ **Clear all data**
```python
ctx.clear()  # Removes everything
```

✅ **Overwrite existing DataFrames**
```python
ctx.register("data", df1)
ctx.register("data", df2)  # Replaces df1 with df2
```

✅ **Type validation**
```python
ctx.register("invalid", {"not": "a dataframe"})
# ❌ TypeError: "Expected pandas.DataFrame, got dict"
```

✅ **Context isolation**
```python
ctx1 = PandasContext()
ctx2 = PandasContext()

ctx1.register("data", df1)
ctx2.register("data", df2)

# Each context has its own data (no sharing)
```

**Why this matters:**
- Nodes can pass data to each other **by name**
- Clear errors when dependencies missing
- Type-safe (can't accidentally register wrong type)
- Same API will work for Spark (SparkContext uses temp views under the hood)

**Example usage in a pipeline:**
```python
# Node 1: Load data
df = read_csv("input.csv")
context.register("raw_data", df)

# Node 2: Clean data
raw = context.get("raw_data")  # Gets data from Node 1
cleaned = raw[raw["value"] > 0]
context.register("cleaned_data", cleaned)

# Node 3: Save
final = context.get("cleaned_data")  # Gets data from Node 2
write_parquet(final, "output.parquet")
```

---

### 3. Function Registry (18 tests)

**What it does:** Manages transform functions with type-safe parameter validation

**Key capabilities tested:**

✅ **@transform decorator registers functions**
```python
@transform
def my_transform(context, param1: str, param2: int = 10):
    """My transformation."""
    data = context.get("input_table")
    # ... transform logic
    return result

# Function is now registered and can be called from YAML!
```

✅ **Decorated functions remain callable**
```python
@transform
def double(context, value: int):
    return value * 2

ctx = PandasContext()
result = double(ctx, 5)  # Returns 10
```

✅ **Parameter validation**
```python
@transform
def process(context, required_param: str, optional: int = 5):
    pass

# Valid - all required params provided
validate_function_params("process", {"required_param": "value"})  # ✅

# Invalid - missing required param
validate_function_params("process", {})  
# ❌ "Missing required parameters: required_param"

# Invalid - unexpected param
validate_function_params("process", {"required_param": "x", "unknown": "y"})
# ❌ "Unexpected parameters: unknown"
```

✅ **Context parameter ignored in validation**
```python
@transform
def func(context, data_param: str):  # 'context' injected by framework
    pass

# Don't need to provide 'context' in params (framework does it)
validate_function_params("func", {"data_param": "value"})  # ✅
```

✅ **Function metadata extraction**
```python
@transform
def example(context, param1: str, param2: int = 10):
    """Example function."""
    pass

info = FunctionRegistry.get_function_info("example")
# {
#   "name": "example",
#   "docstring": "Example function.",
#   "parameters": {
#     "param1": {"required": True, "default": None, "annotation": str},
#     "param2": {"required": False, "default": 10, "annotation": int}
#   }
# }
```

✅ **Error messages show available functions**
```python
get_registered_function("does_not_exist")
# ❌ "Transform function 'does_not_exist' not registered.
#     Available functions: my_transform, process, example"
```

✅ **Real-world usage pattern**
```python
@transform
def filter_by_threshold(context, source_table: str, threshold: float):
    """Filter data by threshold value."""
    df = context.get(source_table)
    return df[df["value"] > threshold]

# In YAML config:
# transform:
#   steps:
#     - function: filter_by_threshold
#       params:
#         source_table: raw_data
#         threshold: 10.0
```

**Why this matters:**
- Transform functions are **type-safe** (validated at config load)
- Clear errors: "Missing parameter X" instead of cryptic Python errors
- Functions work with **any engine** (Pandas/Spark) via Context API
- Self-documenting (docstrings + parameter info available)

---

## Warnings (Non-Critical)

Two warnings about field name shadowing:
```
Field name "validate" in "BaseConnectionConfig" shadows an attribute in parent "BaseModel"
Field name "validate" in "NodeConfig" shadows an attribute in parent "BaseModel"
```

**What it means:** Pydantic's BaseModel has a `validate()` method, and we have fields named `validate` in our configs.

**Impact:** None - the field names work correctly, just a naming collision warning.

**Fix:** Could rename to `validate_on_load` or `validation_mode` if needed.

---

## What This Enables

With these 3 components working, you can now:

1. **Write configs** that are validated before execution
2. **Pass data between nodes** using the Context API
3. **Create transform functions** with `@transform` that work in configs

**What's missing for end-to-end:**
- Dependency graph builder (orders nodes)
- Pipeline executor (runs nodes in order)
- Engine implementations (actually reads/writes data)
- Connections (accesses files/databases)

---

## Example: What You Can Do Now

```python
# Define a transform function
from odibi import transform
import pandas as pd

@transform
def clean_sales_data(context, source: str, min_amount: float = 0.0):
    """Remove invalid sales records."""
    df = context.get(source)
    cleaned = df[df["amount"] >= min_amount]
    return cleaned

# Create and validate a config
from odibi.config import NodeConfig, ReadConfig, TransformConfig

node = NodeConfig(
    name="clean_data",
    read=ReadConfig(
        connection="local",
        format="csv",
        path="sales.csv"
    ),
    transform=TransformConfig(
        steps=[
            {"function": "clean_sales_data", "params": {"source": "clean_data", "min_amount": 10.0}}
        ]
    )
)

# Validate transform parameters
from odibi.registry import validate_function_params
validate_function_params("clean_sales_data", {"source": "data", "min_amount": 5.0})
# ✅ Passes validation

# Use context to pass data
from odibi.context import PandasContext
ctx = PandasContext()
ctx.register("raw", pd.DataFrame({"amount": [5, 15, 25]}))
result = clean_sales_data(ctx, source="raw", min_amount=10.0)
# Returns DataFrame with 2 rows (15, 25)
```

---

## Next Steps

**Continue building:**
1. Dependency graph builder (validates dependencies, detects cycles)
2. Pipeline executor (runs nodes in topological order)
3. Engine implementations (Pandas engine for read/write/SQL)
4. Test the full pipeline flow

**Then we'll have a working MVP!**

---

**Test Coverage:** Excellent foundation  
**Confidence Level:** High - core APIs are solid  
**Ready for:** Building orchestration layer
