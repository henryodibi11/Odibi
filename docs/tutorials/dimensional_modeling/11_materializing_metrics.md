# Materializing Metrics Tutorial

In this tutorial, you'll learn how to pre-compute and persist metrics using the `Materializer` class. Materialization creates pre-aggregated tables for fast dashboard performance.

**What You'll Learn:**
- Why materialize metrics
- Defining materializations
- Executing materializations
- Scheduling with cron
- Incremental strategies (replace vs sum)

---

## Why Materialize?

Ad-hoc queries are powerful but slow for dashboards:

| Approach | Query Time | Use Case |
|----------|------------|----------|
| Ad-hoc query | 5-30 seconds | Exploratory analysis |
| Materialized table | 0.1-0.5 seconds | Production dashboards |

**Materialization** pre-computes metrics at a specific grain and saves them to a table. Dashboards query the pre-computed table instead of raw data.

---

## Semantic Config with Materializations

Let's extend our semantic config to include materializations:

### YAML Configuration

```yaml
# semantic_config.yaml
metrics:
  - name: revenue
    description: "Total revenue from completed orders"
    expr: "SUM(line_total)"
    source: fact_orders
    filters: ["status = 'completed'"]
  
  - name: order_count
    expr: "COUNT(*)"
    source: fact_orders
    filters: ["status = 'completed'"]
  
  - name: unique_customers
    expr: "COUNT(DISTINCT customer_sk)"
    source: fact_orders
    filters: ["status = 'completed'"]
  
  - name: avg_order_value
    expr: "AVG(line_total)"
    source: fact_orders
    filters: ["status = 'completed'"]

dimensions:
  - name: region
    source: dim_customer
    column: region
  
  - name: category
    source: dim_product
    column: category
  
  - name: month
    source: dim_date
    column: month_name
  
  - name: date_sk
    source: dim_date
    column: date_sk

materializations:
  - name: monthly_revenue_by_region
    metrics: [revenue, order_count]
    dimensions: [region, month]
    output: gold/agg_monthly_revenue_region
    schedule: "0 2 1 * *"  # 2am on 1st of month
  
  - name: daily_revenue
    metrics: [revenue, order_count, unique_customers]
    dimensions: [date_sk]
    output: gold/agg_daily_revenue
    schedule: "0 3 * * *"  # 3am daily
  
  - name: category_summary
    metrics: [revenue, order_count, avg_order_value]
    dimensions: [category]
    output: gold/agg_category_summary
```

---

## Step 1: Define a Materialization

A materialization specifies which metrics and dimensions to pre-compute:

### Python Code

```python
from odibi.semantics import MaterializationConfig

# Define a materialization
monthly_revenue = MaterializationConfig(
    name="monthly_revenue_by_region",
    metrics=["revenue", "order_count"],
    dimensions=["region", "month"],
    output="gold/agg_monthly_revenue_region",
    schedule="0 2 1 * *"
)
```

### Understanding the Definition

| Field | Value | Purpose |
|-------|-------|---------|
| `name` | `"monthly_revenue_by_region"` | Unique identifier |
| `metrics` | `["revenue", "order_count"]` | Which metrics to compute |
| `dimensions` | `["region", "month"]` | Grain (GROUP BY) |
| `output` | `"gold/agg_monthly_revenue_region"` | Output table path |
| `schedule` | `"0 2 1 * *"` | Cron schedule (optional) |

### What Gets Generated

The materialization creates a table with:
- One row per unique combination of `region` × `month`
- Columns for each metric (`revenue`, `order_count`)

**Output Table (12 rows - 4 regions × 3 months):**

| region | month | revenue | order_count |
|--------|-------|---------|-------------|
| North | January | 2,549.88 | 7 |
| North | February | 3,120.50 | 9 |
| North | March | 2,890.25 | 8 |
| South | January | 2,349.93 | 7 |
| South | February | 2,780.40 | 8 |
| South | March | 3,050.75 | 9 |
| East | January | 1,923.88 | 7 |
| East | February | 2,450.60 | 7 |
| East | March | 2,180.35 | 6 |
| West | January | 2,129.87 | 7 |
| West | February | 2,890.45 | 8 |
| West | March | 2,650.90 | 7 |

---

## Step 2: Execute a Materialization

Use the `Materializer` class to execute:

### Python Code

```python
from odibi.semantics import Materializer, parse_semantic_config
from odibi.context import EngineContext
from odibi.enums import EngineType
import yaml

# Load config
with open("semantic_config.yaml") as f:
    config = parse_semantic_config(yaml.safe_load(f))

# Create context
context = EngineContext(df=None, engine_type=EngineType.PANDAS)
context.register("fact_orders", fact_orders_df)
context.register("dim_customer", dim_customer_df)
context.register("dim_product", dim_product_df)
context.register("dim_date", dim_date_df)

# Create materializer
materializer = Materializer(config)

# Execute single materialization
result = materializer.execute("monthly_revenue_by_region", context)

# Check result
print(f"Name: {result.name}")
print(f"Output: {result.output}")
print(f"Row count: {result.row_count}")
print(f"Success: {result.success}")
print(f"Execution time: {result.elapsed_ms:.2f}ms")
```

### MaterializationResult

| Field | Type | Description |
|-------|------|-------------|
| `name` | str | Materialization name |
| `output` | str | Output table path |
| `row_count` | int | Number of rows generated |
| `elapsed_ms` | float | Execution time in ms |
| `success` | bool | Whether it succeeded |
| `error` | str | Error message (if failed) |
| `df` | DataFrame | The computed data |

---

## Step 3: Write the Output

Use a callback to write the materialized data:

### Python Code

```python
# Define write callback
def write_to_parquet(df, output_path):
    """Write DataFrame to Parquet."""
    full_path = f"warehouse/{output_path}.parquet"
    df.to_parquet(full_path, index=False)
    print(f"Wrote {len(df)} rows to {full_path}")

# Execute with write callback
result = materializer.execute(
    "monthly_revenue_by_region",
    context,
    write_callback=write_to_parquet
)
```

### Spark Example

```python
def write_to_delta(df, output_path):
    """Write Spark DataFrame to Delta Lake."""
    df.write.format("delta").mode("overwrite").save(f"/mnt/warehouse/{output_path}")

result = materializer.execute(
    "monthly_revenue_by_region",
    context,
    write_callback=write_to_delta
)
```

---

## Step 4: Understanding Schedules

The `schedule` field uses **cron syntax**:

```
┌───────── minute (0-59)
│ ┌─────── hour (0-23)
│ │ ┌───── day of month (1-31)
│ │ │ ┌─── month (1-12)
│ │ │ │ ┌─ day of week (0-6, Sun=0)
│ │ │ │ │
* * * * *
```

### Common Schedules

| Schedule | Cron | When |
|----------|------|------|
| Daily at 2am | `0 2 * * *` | Every day at 2:00 AM |
| Monthly at 2am | `0 2 1 * *` | 1st of month at 2:00 AM |
| Weekly Sunday | `0 3 * * 0` | Sunday at 3:00 AM |
| Hourly | `0 * * * *` | Every hour on the hour |

### Reading Schedules

```python
# Get schedule for a materialization
mat_config = materializer.get_materialization("monthly_revenue_by_region")
print(f"Schedule: {mat_config.schedule}")
# Output: Schedule: 0 2 1 * *

# List all materializations with schedules
for mat in materializer.list_materializations():
    print(f"{mat['name']}: {mat['schedule'] or 'No schedule'}")
```

---

## Step 5: Execute All Materializations

Execute all defined materializations at once:

### Python Code

```python
# Execute all materializations
results = materializer.execute_all(context, write_callback=write_to_parquet)

# Print summary
print("=" * 60)
print("Materialization Summary")
print("=" * 60)
for result in results:
    status = "SUCCESS" if result.success else f"FAILED: {result.error}"
    print(f"  {result.name}: {status}")
    if result.success:
        print(f"    Rows: {result.row_count}, Time: {result.elapsed_ms:.0f}ms")
```

### Output

```
============================================================
Materialization Summary
============================================================
  monthly_revenue_by_region: SUCCESS
    Rows: 12, Time: 45ms
  daily_revenue: SUCCESS
    Rows: 14, Time: 32ms
  category_summary: SUCCESS
    Rows: 2, Time: 18ms
```

---

## Step 6: Incremental Materialization

For large datasets, use incremental updates instead of full rebuilds.

### Replace Strategy

New data **replaces** existing rows for matching grain keys:

```yaml
materializations:
  - name: daily_revenue
    metrics: [revenue, order_count]
    dimensions: [date_sk]
    output: gold/agg_daily_revenue
    incremental:
      timestamp_column: load_timestamp
      merge_strategy: replace
```

### How Replace Works

**Existing Table:**

| date_sk | revenue | order_count |
|---------|---------|-------------|
| 20240115 | 1,439.96 | 3 |
| 20240116 | 589.95 | 3 |
| 20240117 | 749.98 | 2 |

**New Data Arrives (late order for Jan 15):**

| date_sk | revenue | order_count |
|---------|---------|-------------|
| 20240115 | 1,539.96 | 4 |

**After Replace Merge:**

| date_sk | revenue | order_count | Note |
|---------|---------|-------------|------|
| 20240115 | 1,539.96 | 4 | **Replaced** |
| 20240116 | 589.95 | 3 | Unchanged |
| 20240117 | 749.98 | 2 | Unchanged |

### Sum Strategy (Use with Caution)

New measure values **add to** existing values:

```yaml
materializations:
  - name: daily_order_count
    metrics: [order_count]  # Only COUNT metrics!
    dimensions: [date_sk]
    output: gold/agg_daily_count
    incremental:
      timestamp_column: created_at
      merge_strategy: sum
```

### How Sum Works

**Existing Table:**

| date_sk | order_count |
|---------|-------------|
| 20240115 | 3 |
| 20240116 | 3 |

**New Orders (2 new orders on Jan 15):**

| date_sk | order_count |
|---------|-------------|
| 20240115 | 2 |

**After Sum Merge:**

| date_sk | order_count | Note |
|---------|-------------|------|
| 20240115 | 5 | 3 + 2 = 5 |
| 20240116 | 3 | Unchanged |

### When NOT to Use Sum

**Never use sum for:**
- `AVG()` - Would become average of averages
- `COUNT(DISTINCT)` - Would overcount
- `MIN()` / `MAX()` - Would be wrong
- Data with corrections/updates

---

## Complete Python Example

```python
from odibi.semantics import Materializer, parse_semantic_config
from odibi.context import EngineContext
from odibi.enums import EngineType
import pandas as pd
import yaml

# ===========================================
# 1. Load config with materializations
# ===========================================
config_dict = {
    "metrics": [
        {"name": "revenue", "expr": "SUM(line_total)", "source": "fact_orders",
         "filters": ["status = 'completed'"]},
        {"name": "order_count", "expr": "COUNT(*)", "source": "fact_orders",
         "filters": ["status = 'completed'"]},
        {"name": "avg_order_value", "expr": "AVG(line_total)", "source": "fact_orders",
         "filters": ["status = 'completed'"]}
    ],
    "dimensions": [
        {"name": "region", "source": "dim_customer", "column": "region"},
        {"name": "category", "source": "dim_product", "column": "category"},
        {"name": "date_sk", "source": "dim_date", "column": "date_sk"}
    ],
    "materializations": [
        {
            "name": "daily_summary",
            "metrics": ["revenue", "order_count"],
            "dimensions": ["date_sk"],
            "output": "gold/agg_daily_summary",
            "schedule": "0 3 * * *"
        },
        {
            "name": "region_summary",
            "metrics": ["revenue", "order_count", "avg_order_value"],
            "dimensions": ["region"],
            "output": "gold/agg_region_summary"
        },
        {
            "name": "category_by_region",
            "metrics": ["revenue", "order_count"],
            "dimensions": ["category", "region"],
            "output": "gold/agg_category_region"
        }
    ]
}

config = parse_semantic_config(config_dict)

# ===========================================
# 2. Setup context with data
# ===========================================
context = EngineContext(df=None, engine_type=EngineType.PANDAS)
context.register("fact_orders", pd.read_parquet("warehouse/fact_orders"))
context.register("dim_customer", pd.read_parquet("warehouse/dim_customer"))
context.register("dim_product", pd.read_parquet("warehouse/dim_product"))
context.register("dim_date", pd.read_parquet("warehouse/dim_date"))

# ===========================================
# 3. Define write callback
# ===========================================
def write_output(df, output_path):
    full_path = f"warehouse/{output_path}.parquet"
    df.to_parquet(full_path, index=False)
    print(f"  → Wrote {len(df)} rows to {full_path}")

# ===========================================
# 4. Execute all materializations
# ===========================================
materializer = Materializer(config)

print("=" * 60)
print("Executing Materializations")
print("=" * 60)

results = materializer.execute_all(context, write_callback=write_output)

# ===========================================
# 5. Show results
# ===========================================
print()
print("=" * 60)
print("Results Summary")
print("=" * 60)

for result in results:
    status = "✓ SUCCESS" if result.success else f"✗ FAILED: {result.error}"
    print(f"\n{result.name}:")
    print(f"  Status: {status}")
    print(f"  Output: {result.output}")
    print(f"  Rows: {result.row_count}")
    print(f"  Time: {result.elapsed_ms:.0f}ms")
    
    # Show sample data
    if result.success and result.df is not None:
        print(f"  Sample data:")
        print(result.df.head(5).to_string(index=False))
```

### Output

```
============================================================
Executing Materializations
============================================================
  → Wrote 14 rows to warehouse/gold/agg_daily_summary.parquet
  → Wrote 4 rows to warehouse/gold/agg_region_summary.parquet
  → Wrote 8 rows to warehouse/gold/agg_category_region.parquet

============================================================
Results Summary
============================================================

daily_summary:
  Status: ✓ SUCCESS
  Output: gold/agg_daily_summary
  Rows: 14
  Time: 42ms
  Sample data:
   date_sk   revenue  order_count
  20240115  1439.96            3
  20240116   589.95            3
  20240117   749.98            2
  20240118   983.94            2
  20240119   269.98            2

region_summary:
  Status: ✓ SUCCESS
  Output: gold/agg_region_summary
  Rows: 4
  Time: 28ms
  Sample data:
  region   revenue  order_count  avg_order_value
   North  2549.88            7           364.27
   South  2349.93            7           335.70
    East  1923.88            7           274.84
    West  2129.87            7           304.27

category_by_region:
  Status: ✓ SUCCESS
  Output: gold/agg_category_region
  Rows: 8
  Time: 35ms
  Sample data:
     category  region   revenue  order_count
  Electronics   North  1549.94            4
  Electronics   South  1449.95            4
  Electronics    East  1323.91            4
  Electronics    West  1079.93            3
    Furniture   North   999.94            3
```

---

## What You Learned

In this tutorial, you learned:

- **Materialization** pre-computes metrics for fast dashboard queries
- **MaterializationConfig** specifies metrics, dimensions, output, and schedule
- **Materializer.execute()** runs a single materialization
- **Materializer.execute_all()** runs all configured materializations
- **Schedules** use cron syntax: `"0 2 * * *"` (daily at 2am)
- **Replace strategy** overwrites matching grain keys (recommended)
- **Sum strategy** adds to existing values (use with caution)

---

## Next Steps

Now let's put everything together in a complete semantic layer example.

**Next:** [Semantic Full Example](./12_semantic_full_example.md)

---

## Navigation

| Previous | Up | Next |
|----------|----|----|
| [Querying Metrics](./10_querying_metrics.md) | [Tutorials](../getting_started.md) | [Semantic Full Example](./12_semantic_full_example.md) |

---

## Reference

For complete documentation, see: [Materializing Reference](../../semantics/materialize.md)
