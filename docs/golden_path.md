# Golden Path: From Zero to Production

Get a working Odibi pipeline in under 10 minutes. One path. No choices.

---

## Step 1: Install

```bash
pip install odibi
```

---

## Step 2: Create Your Project

**Option A: Use the CLI (recommended)**
```bash
odibi init my_project --template star-schema
cd my_project
```

**Option B: Clone the reference**
```bash
git clone https://github.com/henryodibi11/Odibi.git
cd Odibi/docs/examples/canonical/runnable
```

---

## Step 3: Run the Pipeline

```bash
odibi run odibi.yaml          # Option A
odibi run 04_fact_table.yaml  # Option B
```

This builds a complete star schema:

| Output | What It Is |
|--------|------------|
| `dim_customer` | Customer dimension with surrogate keys |
| `dim_product` | Product dimension with surrogate keys |
| `dim_date` | Generated date dimension (366 rows) |
| `fact_sales` | Sales fact with FK lookups to all dimensions |

---

## Step 4: See What Happened

```bash
# View the audit report (opens in browser)
odibi story last

# Or manually browse the output
ls data/gold/
```

---

## Step 5: Copy and Modify

The canonical pipeline is your template. Copy it:

```bash
cp 04_fact_table.yaml my_pipeline.yaml
```

Then modify:
1. Change `connections.source.base_path` to your data location
2. Update node names and paths to match your tables
3. Adjust patterns to fit your schema

**See the full breakdown:** [THE_REFERENCE.md](examples/canonical/THE_REFERENCE.md)

---

## The Config You Just Ran

```yaml
project: sales_star_schema

connections:
  source:
    type: local
    base_path: ../sample_data
  gold:
    type: local
    base_path: ./data/gold

story:
  connection: gold
  path: stories

system:
  connection: gold
  path: _system

pipelines:
  - pipeline: build_dimensions
    layer: gold
    nodes:
      - name: dim_customer
        read:
          connection: source
          format: csv
          path: customers.csv
          options:
            header: true
        pattern:
          type: dimension
          params:
            natural_key: customer_id
            surrogate_key: customer_sk
            scd_type: 1
            track_cols: [name, email, tier, city]
            unknown_member: true
        write:
          connection: gold
          format: parquet
          path: dim_customer
          mode: overwrite

      - name: dim_product
        # ... (similar pattern)

      - name: dim_date
        pattern:
          type: date_dimension
          params:
            start_date: "2025-01-01"
            end_date: "2025-12-31"
            unknown_member: true
        write:
          connection: gold
          format: parquet
          path: dim_date
          mode: overwrite

  - pipeline: build_facts
    layer: gold
    nodes:
      - name: fact_sales
        depends_on: [dim_customer, dim_product, dim_date]
        read:
          connection: source
          format: csv
          path: orders.csv
        pattern:
          type: fact
          params:
            grain: [order_id, line_item_id]
            dimensions:
              - source_column: customer_id
                dimension_table: dim_customer
                dimension_key: customer_id
                surrogate_key: customer_sk
            orphan_handling: unknown
            measures: [quantity, amount]
        write:
          connection: gold
          format: parquet
          path: fact_sales
          mode: overwrite
```

**Full version:** [04_fact_table.yaml](examples/canonical/runnable/04_fact_table.yaml)

---

## What's Next?

| Goal | Go To |
|------|-------|
| **Understand every line** | [THE_REFERENCE.md](examples/canonical/THE_REFERENCE.md) |
| **Add validation/contracts** | [YAML Schema](reference/yaml_schema.md#contractconfig) |
| **Scale to production (Spark)** | [Decision Guide](guides/decision_guide.md) |
| **Solve a specific problem** | [Playbook](playbook/README.md) |

---

## Production Upgrade

When you outgrow Pandas (files > 1GB), switch to Spark:

```yaml
engine: spark  # Add this at root level
```

That's it. Same config works on Databricks.

---

*Questions? [Open an issue](https://github.com/henryodibi11/Odibi/issues).*
