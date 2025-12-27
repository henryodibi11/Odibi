# Introducing Odibi: Declarative Data Pipelines

*YAML in. Data warehouse out.*

---

## TL;DR

I built Odibi because I was tired of rewriting the same pipeline code for every project. It's an open-source framework that lets you define data pipelines declaratively-describe WHAT you want, let the tool handle HOW. Patterns for dimensions, facts, SCD2, aggregations, and more. Works with Spark, Pandas, or Polars.

---

## Why I Built This

Every data engineering project I worked on had the same patterns:

- Bronze layer ingestion
- Deduplication
- SCD2 history tracking
- Dimension table management
- Fact table surrogate key lookups
- Aggregations
- Data quality validation

And every project, I rewrote them from scratch.

Copy-paste from the last project. Tweak for the new schema. Debug the same edge cases. Repeat.

After doing this for the fifth time, I thought: *Why am I writing the same code over and over?*

So I stopped.

I started abstracting the patterns into reusable components. That became Odibi.

---

## What is Odibi?

Odibi is a declarative data pipeline framework.

**Declarative** means you describe what you want, not how to do it.

Instead of writing 200 lines of Python for SCD2:

```python
# Traditional approach
def apply_scd2(source_df, target_df, keys, track_cols, effective_col):
    # Match records
    matched = source_df.join(target_df, on=keys, how="left")
    
    # Find changes
    changed = matched.filter(/* complex comparison */)
    
    # Close old records
    closed = target_df.filter(/* is_current and has_changes */)
    closed = closed.withColumn("valid_to", /* new value */)
    closed = closed.withColumn("is_current", lit(False))
    
    # Insert new records
    new_records = changed.withColumn("valid_from", col(effective_col))
    new_records = new_records.withColumn("valid_to", lit(None))
    new_records = new_records.withColumn("is_current", lit(True))
    
    # Union everything
    result = unchanged.union(closed).union(new_records)
    
    # Handle first run
    # Handle nulls
    # Handle duplicates
    # ... 150 more lines
    
    return result
```

You write 10 lines of YAML:

```yaml
- name: dim_customer
  transformer: scd2
  params:
    target: silver.dim_customer
    keys: [customer_id]
    track_cols: [name, email, address]
    effective_time_col: updated_at
```

The framework handles the complexity. You focus on the business logic.

---

## Core Concepts

### Nodes

A **node** is a single step in your pipeline. It reads data, optionally transforms it, and writes the result.

```yaml
nodes:
  - name: bronze_orders
    read:
      connection: landing
      path: orders.csv
    write:
      connection: bronze
      path: orders
```

### Transformers

A **transformer** is a reusable operation. Odibi has 30+ built-in:

**Data Engineering Patterns:**
- `scd2` - Slowly changing dimensions type 2
- `merge` - Upsert/merge operations
- `deduplicate` - Remove duplicates

**Relational Operations:**
- `join` - Combine datasets
- `union` - Stack datasets
- `aggregate` - Group and summarize
- `pivot` / `unpivot` - Reshape data

**Data Quality:**
- `validate_and_flag` - Check rules, flag violations
- `filter_rows` - SQL-based filtering
- `fill_nulls` - Handle missing values

**Feature Engineering:**
- `derive_columns` - Create calculated columns
- `case_when` - Conditional logic
- `generate_surrogate_key` - Create unique keys

### Patterns

A **pattern** is a higher-level abstraction for common data modeling tasks:

```yaml
# Dimension pattern
pattern:
  type: dimension
  params:
    natural_key: customer_id
    surrogate_key: customer_sk
    scd_type: 2
    track_cols: [name, email]

# Fact pattern
pattern:
  type: fact
  params:
    grain: [order_id, product_sk]
    dimensions:
      - source_column: customer_id
        dimension_table: dim_customer
        surrogate_key: customer_sk
    orphan_handling: unknown

# Aggregation pattern
pattern:
  type: aggregation
  params:
    grain: [date_sk, product_sk]
    measures:
      - name: revenue
        expr: "SUM(amount)"
```

### Contracts

**Contracts** are data quality checks that run before loading:

```yaml
contracts:
  - type: not_null
    columns: [order_id, customer_id]
  
  - type: accepted_values
    column: status
    values: [pending, shipped, delivered]
  
  - type: row_count
    min: 1000
```

If a contract fails, the pipeline stops before bad data enters.

---

## Example: Complete Pipeline

Here's a simplified e-commerce pipeline:

```yaml
project: "ecommerce"
engine: "spark"

connections:
  bronze:
    type: delta
    catalog: spark_catalog
    schema_name: bronze
  
  silver:
    type: delta
    catalog: spark_catalog
    schema_name: silver
  
  gold:
    type: delta
    catalog: spark_catalog
    schema_name: gold

pipelines:
  - pipeline: build_warehouse
    nodes:
      # Silver: Clean customers
      - name: silver_customers
        read:
          connection: bronze
          table: customers
        transformer: deduplicate
        params:
          keys: [customer_id]
        write:
          connection: silver
          table: customers

      # Gold: Dimension
      - name: dim_customer
        depends_on: [silver_customers]
        pattern:
          type: dimension
          params:
            natural_key: customer_id
            surrogate_key: customer_sk
            scd_type: 2
            track_cols: [name, email, city]
        write:
          connection: gold
          table: dim_customer

      # Gold: Fact
      - name: fact_orders
        read:
          connection: silver
          table: orders
        pattern:
          type: fact
          params:
            grain: [order_id]
            dimensions:
              - source_column: customer_id
                dimension_table: dim_customer
                dimension_key: customer_id
                surrogate_key: customer_sk
            orphan_handling: unknown
        write:
          connection: gold
          table: fact_orders
```

Run it:

```bash
odibi run config.yaml
```

That's it. Bronze → Silver → Gold in one command.

---

## Multi-Engine Support

Odibi works with three engines:

| Engine | Best For | Scale |
|--------|----------|-------|
| **Pandas** | Development, small data | < 1GB |
| **Polars** | Medium data, fast single-machine | 1-10GB |
| **Spark** | Production, large data | 10GB+ |

Same YAML works with all three:

```yaml
# Development
engine: pandas

# Production
engine: spark
```

Switch engines without rewriting pipelines.

---

## What Odibi Handles For You

### SCD2 Complexity

- First run detection
- Change detection
- Row versioning
- Date management
- Duplicate handling

### Surrogate Key Lookups

- Join to dimension tables
- Handle nulls (unknown member)
- Orphan detection and routing

### Data Quality

- Pre-load validation
- Quarantine for failures
- Volume monitoring

### Operations

- Incremental loading
- Schema evolution
- Retry with backoff
- Logging and alerting

---

## What Odibi Doesn't Do

Odibi is not:

- **An orchestrator** - Use Airflow, Databricks Workflows, or Prefect to schedule
- **A data catalog** - Use Unity Catalog, Datahub, or Amundsen
- **A BI tool** - Use Power BI, Tableau, or Superset

Odibi does one thing: transform data through well-defined patterns.

---

## Getting Started

### Installation

```bash
pip install odibi
```

### Minimal Example

```yaml
# odibi.yaml
project: "my_project"
engine: "pandas"

connections:
  local:
    type: local
    base_path: "./data"

story:
  connection: local
  path: "_stories"

system:
  connection: local
  path: "_system"

pipelines:
  - pipeline: example
    nodes:
      - name: process_data
        read:
          connection: local
          path: input.csv
          format: csv
        transform:
          steps:
            - sql: "SELECT * FROM df WHERE amount > 0"
        write:
          connection: local
          path: output
          format: parquet
```

```bash
odibi run odibi.yaml
```

### Documentation

Full docs: [link to docs]

Includes:
- Configuration reference
- Pattern guides
- Transformer catalog
- Example projects

---

## Why Open Source?

I built Odibi to solve my own problems. But the problems aren't unique to me.

Every data engineer I know has rebuilt SCD2 from scratch. Every team has their own dimension pattern code. Every project reinvents the wheel.

By open-sourcing Odibi, I hope to:

1. **Save others time** - Don't rewrite what's already built
2. **Get feedback** - Find edge cases I haven't hit
3. **Build community** - Data engineering patterns should be shared, not siloed

---

## Contributing

Odibi is early. There are bugs. There are missing features. There are rough edges.

If you try it and find issues, I want to know.

Ways to contribute:
- **Report bugs** - GitHub issues
- **Improve docs** - Pull requests welcome
- **Add transformers** - Extend the catalog
- **Share feedback** - What's missing? What's confusing?

---

## What's Next

Over the coming weeks, I'll be sharing:

- Deep dives on each pattern
- Anti-patterns and common mistakes
- Production deployment guides
- Performance optimization

Follow along if you're interested.

---

## Links

- **GitHub:** [https://github.com/henryodibi11/odibi](https://github.com/henryodibi11/odibi)
- **Documentation:** [link to docs]
- **LinkedIn:** [Your LinkedIn URL]

---

*I'm Henry-a data engineer building in public. I created Odibi to solve problems I kept hitting. If it helps you too, that's a win.*
