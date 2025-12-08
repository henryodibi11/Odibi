# Materializing Metrics

This guide covers how to pre-compute and persist metrics using the `Materializer` class.

## Overview

Materialization pre-computes aggregated metrics at a specific grain and persists them to an output table. This enables:

- **Faster query response**: Pre-computed aggregates vs. real-time calculation
- **Scheduled refresh**: Cron-based updates for dashboards
- **Incremental updates**: Merge new data without full recalculation

---

## MaterializationConfig

Define materializations in your semantic layer config:

```yaml
materializations:
  - name: monthly_revenue_by_region     # Unique identifier
    metrics: [revenue, order_count]     # Metrics to include
    dimensions: [region, month]         # Grain (GROUP BY)
    output: gold/agg_monthly_revenue    # Output table path
    schedule: "0 2 1 * *"               # Optional: cron schedule
    incremental:                        # Optional: incremental config
      timestamp_column: order_date
      merge_strategy: replace
```

### Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | str | Yes | - | Unique materialization identifier |
| `metrics` | list | Yes | - | Metrics to materialize |
| `dimensions` | list | Yes | - | Dimensions for grouping |
| `output` | str | Yes | - | Output table path |
| `schedule` | str | No | - | Cron schedule for refresh |
| `incremental` | dict | No | - | Incremental refresh config |

---

## Materializer Class

### Basic Usage

```python
from odibi.semantics import Materializer, parse_semantic_config
from odibi.context import EngineContext

# Load config
config = parse_semantic_config(yaml.safe_load(open("semantic_layer.yaml")))

# Create materializer
materializer = Materializer(config)

# Execute single materialization
result = materializer.execute("monthly_revenue_by_region", context)

print(result.name)        # 'monthly_revenue_by_region'
print(result.output)      # 'gold/agg_monthly_revenue'
print(result.row_count)   # Number of aggregated rows
print(result.elapsed_ms)  # Execution time
print(result.success)     # True if successful
print(result.error)       # Error message if failed
```

### Execute All Materializations

```python
# Execute all configured materializations
results = materializer.execute_all(context)

for result in results:
    status = "OK" if result.success else f"FAILED: {result.error}"
    print(f"  {result.name}: {status} ({result.row_count} rows)")
```

### Write Callback

Provide a callback to write the output:

```python
def write_delta(df, output_path):
    """Write DataFrame to Delta Lake."""
    df.write.format("delta").mode("overwrite").save(output_path)

# Execute with write callback
result = materializer.execute(
    "monthly_revenue_by_region",
    context,
    write_callback=write_delta
)
```

---

## Scheduling

Materializations can have cron schedules for automated refresh:

```yaml
materializations:
  - name: daily_revenue
    metrics: [revenue]
    dimensions: [date_sk]
    output: gold/agg_daily_revenue
    schedule: "0 2 * * *"  # 2am daily

  - name: monthly_summary
    metrics: [revenue, order_count]
    dimensions: [region, month]
    output: gold/agg_monthly_summary
    schedule: "0 3 1 * *"  # 3am on 1st of month
```

### Reading Schedules

```python
# Get schedule for a materialization
schedule = materializer.get_schedule("daily_revenue")
print(schedule)  # "0 2 * * *"

# List all materializations with schedules
for mat in materializer.list_materializations():
    print(f"{mat['name']}: {mat['schedule']}")
```

### Integration with Orchestrators

Use schedules with your orchestrator (Airflow, Dagster, etc.):

```python
# Airflow example
from airflow import DAG
from airflow.operators.python import PythonOperator

def run_materialization(name):
    materializer.execute(name, context, write_callback=write_delta)

for mat in materializer.list_materializations():
    if mat['schedule']:
        PythonOperator(
            task_id=f"materialize_{mat['name']}",
            python_callable=run_materialization,
            op_args=[mat['name']],
            schedule_interval=mat['schedule']
        )
```

---

## Incremental Materialization

Use `IncrementalMaterializer` for efficient updates:

```python
from odibi.semantics.materialize import IncrementalMaterializer

# Create incremental materializer
inc_materializer = IncrementalMaterializer(config)

# Load existing materialized data
existing_df = spark.read.format("delta").load("gold/agg_monthly_revenue")

# Get last processed timestamp
last_timestamp = existing_df.agg({"load_timestamp": "max"}).collect()[0][0]

# Execute incremental update
result = inc_materializer.execute_incremental(
    name="monthly_revenue_by_region",
    context=context,
    existing_df=existing_df,
    timestamp_column="order_date",
    since_timestamp=last_timestamp,
    merge_strategy="replace"
)
```

### Merge Strategies

#### Replace Strategy

New aggregates overwrite existing for matching grain keys:

```python
result = inc_materializer.execute_incremental(
    name="monthly_revenue_by_region",
    context=context,
    existing_df=existing_df,
    timestamp_column="order_date",
    since_timestamp=last_processed,
    merge_strategy="replace"
)
```

**Behavior:**
1. Filter source to `order_date > since_timestamp`
2. Aggregate new data at grain
3. Remove matching grain keys from existing
4. Union remaining existing + new aggregates

**Use case:** Late-arriving data, corrections, any non-additive metrics

#### Sum Strategy

Add new measure values to existing aggregates:

```python
result = inc_materializer.execute_incremental(
    name="daily_order_count",
    context=context,
    existing_df=existing_df,
    timestamp_column="created_at",
    since_timestamp=last_processed,
    merge_strategy="sum"
)
```

**Behavior:**
1. Filter source to `created_at > since_timestamp`
2. Aggregate new data at grain
3. Full outer join with existing on grain
4. Sum measure values

**Use case:** Purely additive metrics (counts, sums) where data is append-only

**Warning:** Don't use for AVG, DISTINCT counts, or ratios.

---

## Full Example

Complete materialization pipeline:

```yaml
# semantic_layer.yaml
semantic_layer:
  metrics:
    - name: revenue
      expr: "SUM(total_amount)"
      source: fact_orders
      filters: ["status = 'completed'"]
    
    - name: order_count
      expr: "COUNT(*)"
      source: fact_orders
    
    - name: unique_customers
      expr: "COUNT(DISTINCT customer_sk)"
      source: fact_orders

  dimensions:
    - name: region
      source: dim_customer
      column: region
    
    - name: month
      source: dim_date
      column: month_name
    
    - name: date_sk
      source: dim_date
      column: date_sk

  materializations:
    - name: daily_revenue
      metrics: [revenue, order_count]
      dimensions: [date_sk, region]
      output: gold/agg_daily_revenue
      schedule: "0 2 * * *"
    
    - name: monthly_summary
      metrics: [revenue, order_count, unique_customers]
      dimensions: [region, month]
      output: gold/agg_monthly_summary
      schedule: "0 3 1 * *"
```

```python
from odibi.semantics import Materializer, IncrementalMaterializer, parse_semantic_config
from odibi.context import EngineContext
import yaml

# Load config
with open("semantic_layer.yaml") as f:
    config = parse_semantic_config(yaml.safe_load(f)["semantic_layer"])

# Setup context
context = EngineContext(df=None, engine_type=EngineType.SPARK, spark=spark)
context.register("fact_orders", spark.table("silver.fact_orders"))
context.register("dim_customer", spark.table("gold.dim_customer"))
context.register("dim_date", spark.table("gold.dim_date"))

# Write callback
def write_to_delta(df, output_path):
    df.write.format("delta").mode("overwrite").save(f"/mnt/warehouse/{output_path}")

# Full refresh all materializations
materializer = Materializer(config)
results = materializer.execute_all(context, write_callback=write_to_delta)

# Print summary
for r in results:
    status = "SUCCESS" if r.success else f"FAILED: {r.error}"
    print(f"{r.name}: {status} - {r.row_count} rows in {r.elapsed_ms:.0f}ms")

# Incremental refresh for daily
inc_materializer = IncrementalMaterializer(config)
existing_daily = spark.read.format("delta").load("/mnt/warehouse/gold/agg_daily_revenue")
last_date = existing_daily.agg({"date_sk": "max"}).collect()[0][0]

result = inc_materializer.execute_incremental(
    name="daily_revenue",
    context=context,
    existing_df=existing_daily,
    timestamp_column="order_date",
    since_timestamp=last_date,
    merge_strategy="replace"
)

# Write incremental result
if result.success:
    write_to_delta(result.df, "gold/agg_daily_revenue")
    print(f"Updated daily_revenue: {result.row_count} rows")
```

---

## MaterializationResult

| Field | Type | Description |
|-------|------|-------------|
| `name` | str | Materialization name |
| `output` | str | Output table path |
| `row_count` | int | Number of aggregated rows |
| `elapsed_ms` | float | Execution time in milliseconds |
| `success` | bool | Whether execution succeeded |
| `error` | str | Error message if failed |

---

## Best Practices

### Grain Selection
- Choose grain based on query patterns
- Finer grain = more rows, but more flexibility
- Coarser grain = faster queries, less flexibility

### Scheduling
- Schedule based on source data freshness
- Daily aggregates: run after nightly ETL
- Monthly: run after month close

### Incremental Strategy
- Use `replace` for late-arriving data tolerance
- Use `sum` only for append-only sources
- Track `since_timestamp` in state store

### Performance
- Partition output by time dimension
- Use Delta Lake for efficient updates
- Monitor execution times

---

## See Also

- [Defining Metrics](./metrics.md) - Create metric definitions
- [Querying](./query.md) - Interactive metric queries
- [Aggregation Pattern](../patterns/aggregation.md) - Pattern-based aggregation
