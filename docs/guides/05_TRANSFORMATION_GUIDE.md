# Transformation Writing Guide

**Learn to create your own custom operations for Odibi pipelines.**

---

## Table of Contents

1. [Quick Example](#quick-example)
2. [The @transformation Decorator](#the-transformation-decorator)
3. [Adding Explanations](#adding-explanations)
4. [Using Context](#using-context)
5. [Explanation Templates](#explanation-templates)
6. [Testing Your Transformations](#testing-your-transformations)
7. [Best Practices](#best-practices)

---

## Quick Example

Here's a complete custom transformation in **30 lines**:

```python
# my_transformations.py

from odibi import transformation
from odibi.transformations.templates import purpose_detail_result

@transformation("remove_outliers", category="cleaning", tags=["quality", "filter"])
def remove_outliers(df, column, threshold=3):
    """
    Remove outliers using standard deviation method.

    Args:
        df: Input DataFrame
        column: Column to check for outliers
        threshold: Number of standard deviations (default: 3)

    Returns:
        DataFrame with outliers removed
    """
    mean = df[column].mean()
    std = df[column].std()

    lower_bound = mean - (threshold * std)
    upper_bound = mean + (threshold * std)

    return df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]

@remove_outliers.explain
def explain(column, threshold=3, **context):
    plant = context.get('plant', 'the dataset')

    return purpose_detail_result(
        purpose=f"Remove statistical outliers from {plant} data",
        details=[
            f"Analyzes column: `{column}`",
            f"Uses ±{threshold} standard deviations as threshold",
            "Removes extreme values that may be errors or anomalies"
        ],
        result="Clean dataset with outliers removed, ready for analysis"
    )
```

**That's it!** Now use it in YAML:

```yaml
- name: clean_temperature_data
  transform:
    operation: remove_outliers
    column: temperature
    threshold: 3
```

---

## The @transformation Decorator

### Basic Usage

```python
from odibi import transformation

@transformation("my_operation")
def my_operation(df, param1, param2):
    """
    Your operation description here.
    Minimum 10 characters required!
    """
    # Your logic
    return transformed_df
```

### With Metadata

```python
@transformation(
    "my_operation",
    version="1.0.0",
    category="filtering",  # Group operations
    tags=["quality", "validation"]  # For discovery
)
def my_operation(df, ...):
    """..."""
    return df
```

**Categories:**
- `reshaping` - pivot, unpivot, melt, stack
- `filtering` - remove rows, select columns
- `aggregation` - group by, summarize
- `calculation` - derive new columns
- `combining` - join, concat, merge
- `cleaning` - remove nulls, deduplicate
- `validation` - check quality, flag issues

### Required: Docstring

```python
# ❌ Fails - no docstring
@transformation("bad")
def bad(df):
    return df

# ❌ Fails - docstring too short
@transformation("bad")
def bad(df):
    """Bad."""
    return df

# ✅ Good - meaningful docstring
@transformation("good")
def good(df):
    """Remove duplicate rows from DataFrame."""
    return df
```

---

## Adding Explanations

### Why Explanations Matter

Explanations appear in:
- ✅ Documentation stories (for stakeholders)
- ✅ Auto-generated docs
- ✅ Pipeline documentation

**Without explanation:**
> "Operation: deduplicate"

**With explanation:**
> "Removes duplicate temperature readings for NKC Plant Germ Dryer 1, ensuring each timestamp appears only once for accurate trend analysis."

### Basic Explanation

```python
@transformation("deduplicate")
def deduplicate(df, subset=None):
    """Remove duplicate rows."""
    return df.drop_duplicates(subset=subset)

@deduplicate.explain
def explain(subset=None, **context):
    details = ["Removes duplicate rows"]

    if subset:
        details.append(f"Based on columns: {', '.join(subset)}")
    else:
        details.append("Based on all columns")

    return purpose_detail_result(
        purpose="Remove duplicate records",
        details=details,
        result="Deduplicated dataset with unique records"
    )
```

### Context-Aware Explanation

```python
@my_transform.explain
def explain(threshold, **context):
    # Extract context
    plant = context.get('plant', 'Unknown')
    asset = context.get('asset', 'Unknown')
    pipeline = context.get('pipeline', 'Unknown')

    # Use in explanation
    return purpose_detail_result(
        purpose=f"Quality control for {plant} - {asset}",
        details=[
            f"Part of {pipeline} pipeline",
            f"Removes values below {threshold}",
            "Ensures data quality standards"
        ],
        result="Validated dataset meeting quality thresholds"
    )
```

**Context automatically includes:**
- `plant` - From YAML project config
- `asset` - From YAML project config
- `business_unit` - From YAML
- `project` - From YAML
- `pipeline` - Pipeline name
- `node` - Node name
- `operation` - Operation name

---

## Explanation Templates

### 1. Purpose-Detail-Result (Standard)

```python
from odibi.transformations.templates import purpose_detail_result

return purpose_detail_result(
    purpose="What this does in one sentence",
    details=[
        "Detail point 1",
        "Detail point 2",
        "Detail point 3"
    ],
    result="What you get at the end"
)
```

**Output:**
```
**Purpose:** What this does in one sentence

**Details:**
- Detail point 1
- Detail point 2
- Detail point 3

**Result:** What you get at the end
```

### 2. With Formula

```python
from odibi.transformations.templates import with_formula

return with_formula(
    purpose="Calculate thermal efficiency",
    details=[
        "Uses fuel consumption and heat output",
        "Industry-standard calculation"
    ],
    formula="efficiency = (heat_output / fuel_input) × 100",
    result="Efficiency percentage for performance tracking"
)
```

**Output includes:**
```
**Formula:**
```
efficiency = (heat_output / fuel_input) × 100
```
```

### 3. With Table

```python
from odibi.transformations.templates import table_explanation

table_data = [
    {"Input": "Temperature", "Output": "temp_celsius", "Transform": "F → C conversion"},
    {"Input": "Pressure", "Output": "pressure_bar", "Transform": "PSI → bar"}
]

return table_explanation(
    purpose="Standardize units",
    details=["Converts all measurements to metric"],
    table_data=table_data,
    result="Standardized measurements"
)
```

---

## Using Context

### Available Context Fields

```python
@my_op.explain
def explain(param1, **context):
    # Always available:
    node = context.get('node')           # "my_node_name"
    operation = context.get('operation') # "my_operation"
    pipeline = context.get('pipeline')   # "my_pipeline"

    # If provided in YAML:
    project = context.get('project')     # "My Project"
    plant = context.get('plant')         # "NKC Plant"
    asset = context.get('asset')         # "Germ Dryer 1"
    business_unit = context.get('business_unit')  # "Operations"

    # Use with defaults:
    plant = context.get('plant', 'Unknown Plant')
```

### Context Example

```python
@transformation("aggregate_metrics")
def aggregate_metrics(df, group_by, metrics):
    """Aggregate metrics by grouping columns."""
    return df.groupby(group_by)[metrics].mean()

@aggregate_metrics.explain
def explain(group_by, metrics, **context):
    plant = context.get('plant', 'Unknown')
    asset = context.get('asset', 'Unknown')

    if isinstance(group_by, str):
        group_by = [group_by]
    if isinstance(metrics, str):
        metrics = [metrics]

    return purpose_detail_result(
        purpose=f"Calculate average metrics for {plant} {asset}",
        details=[
            f"Groups by: {', '.join(group_by)}",
            f"Aggregates: {', '.join(metrics)}",
            "Uses mean() for averaging",
            "Produces one row per unique group combination"
        ],
        result=f"Aggregated metrics for {plant} performance analysis"
    )
```

---

## Testing Your Transformations

### Test Structure

```python
# tests/test_my_operations.py

import pandas as pd
import pytest
from odibi.transformations import get_registry

class TestMyOperation:
    @classmethod
    def setup_class(cls):
        """Import to register operation."""
        import my_transformations  # ← Registers it

    def test_basic_functionality(self):
        """Should perform basic operation."""
        df = pd.DataFrame({"value": [1, 2, 100, 3]})

        registry = get_registry()
        func = registry.get("remove_outliers")

        result = func(df, column="value", threshold=2)

        # Outlier (100) should be removed
        assert len(result) == 3
        assert 100 not in result["value"].values

    def test_explanation(self):
        """Should generate explanation."""
        registry = get_registry()
        func = registry.get("remove_outliers")

        explanation = func.get_explanation(
            column="temperature",
            threshold=3,
            plant="NKC",
            asset="Dryer 1"
        )

        assert "**Purpose:**" in explanation
        assert "NKC" in explanation
        assert "temperature" in explanation
```

### Run Your Tests

```bash
# Run your test file
pytest tests/test_my_operations.py -v

# Should see:
# ✅ test_basic_functionality PASSED
# ✅ test_explanation PASSED
```

---

## Best Practices

### 1. Keep It Simple

```python
# ❌ Complex
@transformation("complex_op")
def complex_op(df, param1, param2, param3, param4, param5):
    # 100 lines of logic
    ...

# ✅ Simple
@transformation("simple_op")
def simple_op(df, threshold):
    """Filter above threshold."""
    return df[df.value > threshold]
```

**Rule:** If logic is complex, use SQL operation instead!

### 2. Handle String and List Parameters

```python
@transformation("my_op")
def my_op(df, columns):
    # Accept both string and list
    if isinstance(columns, str):
        columns = [columns]

    return df[columns]
```

**Why?** YAML users might write:
```yaml
columns: ID            # String
columns: [ID, Name]    # List
```

### 3. Validate Inputs

```python
@transformation("safe_divide")
def safe_divide(df, numerator, denominator):
    """Divide two columns safely."""
    if numerator not in df.columns:
        raise ValueError(f"Column '{numerator}' not found in DataFrame")
    if denominator not in df.columns:
        raise ValueError(f"Column '{denominator}' not found in DataFrame")

    return df.assign(result=df[numerator] / df[denominator])
```

### 4. Write Helpful Docstrings

```python
@transformation("my_op")
def my_op(df, threshold, mode="strict"):
    """
    Filter records based on threshold value.

    Args:
        df: Input DataFrame
        threshold: Minimum value to keep
        mode: Filtering mode ('strict' or 'relaxed')

    Returns:
        Filtered DataFrame with rows >= threshold

    Example:
        Input:  | ID | Value |
                |----|-------|
                | A  | 100   |
                | B  | 50    |

        With threshold=75:
        Output: | ID | Value |
                |----|-------|
                | A  | 100   |
    """
    if mode == "strict":
        return df[df.value > threshold]
    else:
        return df[df.value >= threshold]
```

### 5. Use Explanation Templates

```python
# ✅ Good - uses template
from odibi.transformations.templates import purpose_detail_result

@my_op.explain
def explain(**kwargs):
    return purpose_detail_result(...)

# ❌ Bad - manual formatting
@my_op.explain
def explain(**kwargs):
    return "Purpose: ...\nDetails: ..."  # Inconsistent!
```

---

## Advanced Techniques

### Multi-DataFrame Operations

```python
@transformation("merge_with_lookup")
def merge_with_lookup(df, lookup_df, on, how="left"):
    """
    Merge DataFrame with a lookup table.

    Args:
        df: Main DataFrame
        lookup_df: Lookup DataFrame (from another node)
        on: Column to join on
        how: Join type

    Returns:
        Merged DataFrame
    """
    return df.merge(lookup_df, on=on, how=how)
```

**Use in YAML:**
```yaml
- name: load_main
  read:
    path: main_data.csv

- name: load_lookup
  read:
    path: lookup.csv

- name: merge_datasets
  depends_on: [load_main, load_lookup]
  transform:
    operation: merge_with_lookup
    lookup_df: load_lookup  # Reference other node!
    on: ID
```

### Conditional Logic

```python
@transformation("conditional_transform")
def conditional_transform(df, condition_column, threshold, operation):
    """
    Apply transformation conditionally.

    Args:
        df: Input DataFrame
        condition_column: Column to check
        threshold: Threshold value
        operation: 'scale' or 'flag'

    Returns:
        Transformed DataFrame
    """
    mask = df[condition_column] > threshold

    if operation == "scale":
        df.loc[mask, condition_column] = df.loc[mask, condition_column] * 1.1
    elif operation == "flag":
        df['quality_flag'] = mask.map({True: 'high', False: 'normal'})

    return df
```

### Stateful Operations (Advanced)

```python
@transformation("running_average")
def running_average(df, column, window=5):
    """
    Calculate running average.

    Args:
        df: Input DataFrame (must be sorted by time)
        column: Column to average
        window: Window size

    Returns:
        DataFrame with running_avg column
    """
    df = df.copy()
    df['running_avg'] = df[column].rolling(window=window).mean()
    return df
```

---

## Complete Example: Real-World Operation

```python
# efficiency_calculator.py

from odibi import transformation
from odibi.transformations.templates import with_formula
import pandas as pd

@transformation(
    "calculate_thermal_efficiency",
    version="1.0.0",
    category="calculation",
    tags=["energy", "efficiency", "manufacturing"]
)
def calculate_thermal_efficiency(
    df,
    fuel_column,
    output_column,
    target_column=None,
    flag_low_performance=True,
    low_threshold=0.6
):
    """
    Calculate thermal efficiency from fuel and output measurements.

    Thermal efficiency = (useful energy output / energy input) × 100

    Args:
        df: Input DataFrame with fuel and output columns
        fuel_column: Column containing fuel consumption (energy input)
        output_column: Column containing useful output (energy output)
        target_column: Optional target efficiency for comparison
        flag_low_performance: Whether to flag low-performing records
        low_threshold: Threshold for low performance (default: 0.6 = 60%)

    Returns:
        DataFrame with additional columns:
        - efficiency: Calculated thermal efficiency (0-1 range)
        - efficiency_pct: Efficiency as percentage
        - performance_flag: 'Low', 'Medium', or 'High' (if flagging enabled)
        - vs_target: Difference from target (if target provided)

    Example:
        Input:
        | Timestamp | Fuel | Output |
        |-----------|------|--------|
        | 10:00     | 100  | 75     |
        | 11:00     | 100  | 85     |

        Output (with target=0.8):
        | Timestamp | Fuel | Output | efficiency | efficiency_pct | performance_flag | vs_target |
        |-----------|------|--------|-----------|----------------|------------------|-----------|
        | 10:00     | 100  | 75     | 0.75      | 75.0           | Medium           | -0.05     |
        | 11:00     | 100  | 85     | 0.85      | 85.0           | High             | +0.05     |
    """
    # Validate inputs
    if fuel_column not in df.columns:
        raise ValueError(f"Column '{fuel_column}' not found")
    if output_column not in df.columns:
        raise ValueError(f"Column '{output_column}' not found")

    # Calculate efficiency
    df = df.copy()
    df['efficiency'] = df[output_column] / df[fuel_column]
    df['efficiency_pct'] = df['efficiency'] * 100

    # Flag performance if requested
    if flag_low_performance:
        df['performance_flag'] = pd.cut(
            df['efficiency'],
            bins=[0, low_threshold, 0.8, 1.0],
            labels=['Low', 'Medium', 'High']
        )

    # Compare to target if provided
    if target_column and target_column in df.columns:
        df['vs_target'] = df['efficiency'] - df[target_column]

    return df

@calculate_thermal_efficiency.explain
def explain(
    fuel_column,
    output_column,
    target_column=None,
    flag_low_performance=True,
    low_threshold=0.6,
    **context
):
    """Generate context-aware explanation."""
    plant = context.get('plant', 'facility')
    asset = context.get('asset', 'equipment')

    details = [
        f"Input energy: `{fuel_column}` column",
        f"Useful output: `{output_column}` column",
        "Calculates efficiency ratio and percentage"
    ]

    if flag_low_performance:
        details.append(f"Flags records below {low_threshold*100:.0f}% efficiency as low performance")

    if target_column:
        details.append(f"Compares against target values in `{target_column}`")

    result_desc = f"Efficiency metrics for {plant} {asset} performance monitoring"
    if flag_low_performance:
        result_desc += " with quality flags"

    return with_formula(
        purpose=f"Calculate thermal efficiency for {plant} {asset}",
        details=details,
        formula="efficiency = output / fuel",
        result=result_desc
    )
```

**Use in YAML:**
```yaml
- name: calculate_boiler_efficiency
  depends_on: [load_sensor_data]
  transform:
    operation: calculate_thermal_efficiency
    fuel_column: fuel_consumption_kwh
    output_column: heat_output_kwh
    target_column: efficiency_target
    flag_low_performance: true
    low_threshold: 0.65
```

---

## Explanation Quality Rules

### What Gets Validated

The `ExplanationLinter` checks:

**1. Minimum Length**
```python
# ❌ Too short
return "Filters data"

# ✅ Good length
return purpose_detail_result(
    purpose="Filter records based on quality criteria",
    details=["Removes invalid entries", "Keeps only verified data"],
    result="Clean, validated dataset"
)
```

**2. Required Sections**
```python
# ❌ Missing sections
return "Just text here"

# ✅ Has Purpose, Details, Result
return purpose_detail_result(
    purpose="...",  # ✅
    details=[...],  # ✅
    result="..."    # ✅
)
```

**3. No Lazy Phrases**
```python
# ❌ Generic
"Calculates stuff"
"Does things with data"
"Processes records"
"TODO: write this"

# ✅ Specific
"Calculates thermal efficiency from fuel and output measurements"
"Removes duplicate temperature readings based on timestamp"
"Filters records where quality_flag = 'GOOD'"
```

---

## Testing Checklist

For each new transformation, test:

- [ ] Basic functionality works
- [ ] Handles string and list parameters
- [ ] Validates inputs (rejects invalid data)
- [ ] Explanation generates without errors
- [ ] Explanation includes required sections
- [ ] Explanation uses context when provided
- [ ] Metadata registered correctly
- [ ] Edge cases handled (empty DataFrame, nulls, etc.)

**Example test suite:**

```python
class TestMyOperation:
    @classmethod
    def setup_class(cls):
        import my_operations  # Register

    def test_basic(self):
        """Basic functionality."""
        ...

    def test_with_string_param(self):
        """String parameter variant."""
        ...

    def test_with_list_param(self):
        """List parameter variant."""
        ...

    def test_empty_dataframe(self):
        """Handle empty DataFrame."""
        ...

    def test_explanation(self):
        """Generates valid explanation."""
        ...

    def test_explanation_with_context(self):
        """Uses context in explanation."""
        ...

    def test_metadata(self):
        """Has correct metadata."""
        ...
```

**Aim for 8-10 tests per operation.**

---

## Real-World Examples

### Example 1: Data Normalization

```python
@transformation("normalize_columns")
def normalize_columns(df, columns):
    """
    Normalize numeric columns to 0-1 range.

    Uses min-max normalization: (x - min) / (max - min)
    """
    if isinstance(columns, str):
        columns = [columns]

    df = df.copy()
    for col in columns:
        min_val = df[col].min()
        max_val = df[col].max()
        df[col] = (df[col] - min_val) / (max_val - min_val)

    return df

@normalize_columns.explain
def explain(columns, **context):
    if isinstance(columns, str):
        columns = [columns]

    return with_formula(
        purpose="Normalize values to 0-1 range",
        details=[
            f"Normalizes columns: {', '.join(columns)}",
            "Uses min-max normalization",
            "Preserves relative relationships"
        ],
        formula="normalized = (value - min) / (max - min)",
        result="Normalized data suitable for machine learning or comparison"
    )
```

### Example 2: Time-Based Filtering

```python
@transformation("filter_recent")
def filter_recent(df, timestamp_column, days=7):
    """
    Keep only recent records.

    Args:
        df: Input DataFrame
        timestamp_column: Column with timestamps
        days: Number of days to keep (default: 7)

    Returns:
        Filtered DataFrame with recent records
    """
    df = df.copy()
    df[timestamp_column] = pd.to_datetime(df[timestamp_column])

    cutoff = pd.Timestamp.now() - pd.Timedelta(days=days)
    return df[df[timestamp_column] >= cutoff]

@filter_recent.explain
def explain(timestamp_column, days=7, **context):
    return purpose_detail_result(
        purpose=f"Keep only records from last {days} days",
        details=[
            f"Filters based on `{timestamp_column}` column",
            f"Keeps records from last {days} days",
            "Older records are removed"
        ],
        result=f"Dataset with recent data (last {days} days)"
    )
```

### Example 3: Quality Scoring

```python
@transformation("calculate_quality_score")
def calculate_quality_score(df, metrics, weights=None):
    """
    Calculate weighted quality score.

    Args:
        df: Input DataFrame
        metrics: List of metric columns
        weights: Optional weights (equal if None)

    Returns:
        DataFrame with quality_score column
    """
    if isinstance(metrics, str):
        metrics = [metrics]

    if weights is None:
        weights = [1.0 / len(metrics)] * len(metrics)

    df = df.copy()
    df['quality_score'] = sum(
        df[metric] * weight
        for metric, weight in zip(metrics, weights)
    )

    return df

@calculate_quality_score.explain
def explain(metrics, weights=None, **context):
    if isinstance(metrics, str):
        metrics = [metrics]

    details = [f"Combines {len(metrics)} metrics into single score"]

    if weights:
        details.append("Uses weighted average:")
        for metric, weight in zip(metrics, weights):
            details.append(f"  - {metric}: {weight*100:.0f}% weight")
    else:
        details.append("Uses equal weighting for all metrics")

    return purpose_detail_result(
        purpose="Calculate composite quality score",
        details=details,
        result="Single quality_score column for ranking/filtering"
    )
```

---

## Packaging Your Transformations

### Option 1: Single File

```python
# my_company_ops.py

from odibi import transformation

@transformation("op1")
def op1(...): ...

@transformation("op2")
def op2(...): ...

# Use: import my_company_ops before running pipeline
```

### Option 2: Package

```
my_company_ops/
├── __init__.py          # Import all operations
├── quality_ops.py       # Quality-related operations
├── manufacturing_ops.py # Manufacturing-specific
└── reporting_ops.py     # Reporting operations
```

```python
# __init__.py
from .quality_ops import *
from .manufacturing_ops import *
from .reporting_ops import *
```

### Option 3: Plugin (Advanced)

Create installable package:

```
my-odibi-ops/
├── setup.py
├── my_odibi_ops/
│   ├── __init__.py
│   └── operations.py
└── tests/
    └── test_operations.py
```

```bash
pip install my-odibi-ops
```

---

## Common Mistakes

### Mistake 1: Modifying DataFrame In-Place

```python
# ❌ Bad - modifies original
@transformation("bad_op")
def bad_op(df, col):
    df[col] = df[col] * 2  # Modifies original!
    return df

# ✅ Good - creates copy
@transformation("good_op")
def good_op(df, col):
    df = df.copy()  # Safe!
    df[col] = df[col] * 2
    return df
```

### Mistake 2: No Error Handling

```python
# ❌ Bad - crashes on missing column
@transformation("bad_op")
def bad_op(df, col):
    return df[df[col] > 100]

# ✅ Good - validates first
@transformation("good_op")
def good_op(df, col):
    if col not in df.columns:
        raise ValueError(f"Column '{col}' not found. Available: {df.columns.tolist()}")
    return df[df[col] > 100]
```

### Mistake 3: Generic Explanations

```python
# ❌ Bad - too generic
@my_op.explain
def explain(**context):
    return "This operation processes data"

# ✅ Good - specific and contextual
@my_op.explain
def explain(threshold, **context):
    plant = context.get('plant', 'facility')
    return purpose_detail_result(
        purpose=f"Filter high-value records for {plant}",
        details=[f"Keeps only values above {threshold}"],
        result="High-value records for further analysis"
    )
```

---

## Next Steps

**You can now create custom transformations!**

Continue learning:
- **[Architecture Guide](04_ARCHITECTURE_GUIDE.md)** - Understand the system
- **[Troubleshooting](06_TROUBLESHOOTING.md)** - Debug issues
- **Look at existing operations** in `odibi/operations/` for inspiration

**Try it yourself:**
1. Create `my_operations.py`
2. Write a simple transformation
3. Add explanation
4. Write tests
5. Use it in a pipeline!

---

**Pro tip:** Look at `odibi/operations/unpivot.py` - it's only 75 lines and shows everything you need to know! 📖
