# FK Validation

The FK Validation module declares and validates referential integrity between fact and dimension tables.

## How It Fits Into Odibi

FK Validation is a **Python API module** that complements the `fact` pattern. While the `fact` pattern handles basic orphan handling (unknown member assignment), the FK validation module provides:

- **Detailed orphan reporting** with sample values
- **Multiple validation strategies** (error, warn, filter)
- **Relationship registry** for documenting your data model
- **Lineage generation** from relationships

**It's typically used for:**
1. Post-pipeline validation/auditing
2. Custom fact loading with advanced orphan handling
3. Documenting relationships for data governance

---

## Quick Start

### 1. Define Relationships

```python
from odibi.validation.fk import RelationshipConfig, RelationshipRegistry

relationships = [
    RelationshipConfig(
        name="orders_to_customers",
        fact="fact_orders",
        dimension="dim_customer",
        fact_key="customer_sk",
        dimension_key="customer_sk",
        on_violation="error"
    ),
    RelationshipConfig(
        name="orders_to_products",
        fact="fact_orders",
        dimension="dim_product",
        fact_key="product_sk",
        dimension_key="product_sk",
        on_violation="warn"
    )
]

registry = RelationshipRegistry(relationships=relationships)
```

### 2. Validate a Fact Table

```python
from odibi.validation.fk import FKValidator
from odibi.context import EngineContext

# Setup context with dimension tables
context = EngineContext(df=None, engine_type=EngineType.SPARK, spark=spark)
context.register("dim_customer", spark.table("warehouse.dim_customer"))
context.register("dim_product", spark.table("warehouse.dim_product"))

# Load fact table
fact_df = spark.table("warehouse.fact_orders")

# Validate
validator = FKValidator(registry)
report = validator.validate_fact(fact_df, "fact_orders", context)

# Check results
if report.all_valid:
    print("All FK relationships valid!")
else:
    print(f"Found {len(report.orphan_records)} orphan records")
    for result in report.results:
        if not result.valid:
            print(f"  {result.relationship_name}: {result.orphan_count} orphans")
```

---

## YAML Configuration (Optional)

You can define relationships in YAML and load them:

```yaml
# relationships.yaml
relationships:
  - name: orders_to_customers
    fact: fact_orders
    dimension: dim_customer
    fact_key: customer_sk
    dimension_key: customer_sk
    nullable: false
    on_violation: error

  - name: orders_to_products
    fact: fact_orders
    dimension: dim_product
    fact_key: product_sk
    dimension_key: product_sk
    nullable: false
    on_violation: error

  - name: orders_to_dates
    fact: fact_orders
    dimension: dim_date
    fact_key: order_date_sk
    dimension_key: date_sk
    nullable: true  # Pending orders may not have date
    on_violation: warn
```

```python
from odibi.validation.fk import parse_relationships_config
import yaml

with open("relationships.yaml") as f:
    config = yaml.safe_load(f)

registry = parse_relationships_config(config)
```

---

## RelationshipConfig

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | str | Yes | - | Unique relationship identifier |
| `fact` | str | Yes | - | Fact table name |
| `dimension` | str | Yes | - | Dimension table name |
| `fact_key` | str | Yes | - | FK column in fact table |
| `dimension_key` | str | Yes | - | PK/SK column in dimension |
| `nullable` | bool | No | false | Whether nulls are allowed in fact_key |
| `on_violation` | str | No | "error" | Action on violation: "error", "warn", "quarantine" |

---

## Validation Results

### FKValidationResult (per relationship)

| Field | Type | Description |
|-------|------|-------------|
| `relationship_name` | str | Relationship identifier |
| `valid` | bool | Whether validation passed |
| `total_rows` | int | Total rows in fact table |
| `orphan_count` | int | Number of orphan records |
| `null_count` | int | Number of null FK values |
| `orphan_values` | list | Sample orphan values (up to 100) |
| `elapsed_ms` | float | Validation time |

### FKValidationReport (for entire fact table)

| Field | Type | Description |
|-------|------|-------------|
| `fact_table` | str | Fact table name |
| `all_valid` | bool | True if all relationships valid |
| `total_relationships` | int | Number of relationships checked |
| `valid_relationships` | int | Number that passed |
| `results` | List[FKValidationResult] | Individual results |
| `orphan_records` | List[OrphanRecord] | All orphan records |

---

## validate_fk_on_load

Convenience function for pipeline integration:

```python
from odibi.validation.fk import validate_fk_on_load, RelationshipConfig

relationships = [
    RelationshipConfig(
        name="orders_to_customers",
        fact="fact_orders",
        dimension="dim_customer",
        fact_key="customer_sk",
        dimension_key="customer_sk"
    )
]

# Error on violation (default) - raises ValueError
validated_df = validate_fk_on_load(
    fact_df=fact_df,
    relationships=relationships,
    context=context,
    on_failure="error"
)

# Warn on violation - logs warning, returns original
validated_df = validate_fk_on_load(
    fact_df=fact_df,
    relationships=relationships,
    context=context,
    on_failure="warn"
)

# Filter orphans - removes orphan rows
validated_df = validate_fk_on_load(
    fact_df=fact_df,
    relationships=relationships,
    context=context,
    on_failure="filter"
)
```

---

## Integration with Fact Pattern

The `fact` pattern already handles basic orphan handling. Use FK validation for additional auditing:

```yaml
# odibi.yaml - Build fact with orphan handling
pipelines:
  - pipeline: build_facts
    nodes:
      - name: dim_customer
        read:
          connection: warehouse
          path: dim_customer

      - name: fact_orders
        depends_on: [dim_customer]
        read:
          connection: staging
          path: orders
        transformer: fact
        params:
          grain: [order_id]
          dimensions:
            - source_column: customer_id
              dimension_table: dim_customer
              dimension_key: customer_id
              surrogate_key: customer_sk
          orphan_handling: unknown  # Assigns SK=0 to orphans
        write:
          connection: warehouse
          path: fact_orders
```

Then run FK validation as a post-pipeline check:

```python
# Post-pipeline audit
from odibi.validation.fk import FKValidator, RelationshipRegistry, RelationshipConfig

# Define expected relationships
registry = RelationshipRegistry(relationships=[
    RelationshipConfig(
        name="verify_customer_fk",
        fact="fact_orders",
        dimension="dim_customer",
        fact_key="customer_sk",
        dimension_key="customer_sk"
    )
])

validator = FKValidator(registry)
report = validator.validate_fact(
    spark.table("warehouse.fact_orders"),
    "fact_orders",
    context
)

# Report findings
if not report.all_valid:
    print(f"WARNING: {report.orphan_records} orphan records found")
    # Log to monitoring, send alert, etc.
```

---

## Lineage Generation

Generate lineage graph from relationships:

```python
registry = RelationshipRegistry(relationships=[
    RelationshipConfig(
        name="orders_to_customers",
        fact="fact_orders",
        dimension="dim_customer",
        fact_key="customer_sk",
        dimension_key="customer_sk"
    ),
    RelationshipConfig(
        name="orders_to_products",
        fact="fact_orders",
        dimension="dim_product",
        fact_key="product_sk",
        dimension_key="product_sk"
    ),
    RelationshipConfig(
        name="line_items_to_orders",
        fact="fact_line_items",
        dimension="fact_orders",
        fact_key="order_sk",
        dimension_key="order_sk"
    )
])

lineage = registry.generate_lineage()
# {
#     'fact_orders': ['dim_customer', 'dim_product'],
#     'fact_line_items': ['fact_orders']
# }
```

---

## Full Example

Complete FK validation workflow:

```python
from odibi.validation.fk import (
    RelationshipConfig,
    RelationshipRegistry,
    FKValidator,
    get_orphan_records
)
from odibi.context import EngineContext
from odibi.enums import EngineType

# Define relationships
relationships = [
    RelationshipConfig(
        name="orders_to_customers",
        fact="fact_orders",
        dimension="dim_customer",
        fact_key="customer_sk",
        dimension_key="customer_sk",
        nullable=False,
        on_violation="error"
    ),
    RelationshipConfig(
        name="orders_to_products",
        fact="fact_orders",
        dimension="dim_product",
        fact_key="product_sk",
        dimension_key="product_sk",
        nullable=False,
        on_violation="warn"
    )
]

registry = RelationshipRegistry(relationships=relationships)
validator = FKValidator(registry)

# Setup context
context = EngineContext(df=None, engine_type=EngineType.SPARK, spark=spark)
context.register("dim_customer", spark.table("warehouse.dim_customer"))
context.register("dim_product", spark.table("warehouse.dim_product"))

# Load and validate
fact_df = spark.table("warehouse.fact_orders")
report = validator.validate_fact(fact_df, "fact_orders", context)

# Report
print(f"Validation {'PASSED' if report.all_valid else 'FAILED'}")
print(f"Checked {report.total_relationships} relationships")

for result in report.results:
    status = "PASS" if result.valid else "FAIL"
    print(f"  {result.relationship_name}: {status}")
    if not result.valid:
        print(f"    Orphans: {result.orphan_count}")
        print(f"    Sample values: {result.orphan_values[:5]}")

# Generate lineage
lineage = registry.generate_lineage()
print(f"Lineage: {lineage}")
```

---

---

## Pipeline Integration Patterns

### Pattern 1: Pre-Load Validation

Validate FK relationships **before** writing to the warehouse:

```python
from odibi.validation.fk import validate_fk_on_load, RelationshipConfig

@transform
def validate_and_load_orders(context, current):
    """Validate FKs before writing to warehouse."""
    relationships = [
        RelationshipConfig(
            name="orders_to_customers",
            fact="orders",
            dimension="dim_customer",
            fact_key="customer_id",
            dimension_key="customer_id"
        )
    ]
    
    validated_df = validate_fk_on_load(
        fact_df=current,
        relationships=relationships,
        context=context,
        on_failure="filter"  # Remove orphans
    )
    return validated_df
```

YAML configuration:
```yaml
nodes:
  - name: dim_customer
    read:
      connection: warehouse
      path: dim_customer

  - name: validated_orders
    depends_on: [dim_customer]
    read:
      connection: staging
      path: orders
    transform:
      steps:
        - function: validate_and_load_orders
    write:
      connection: warehouse
      path: fact_orders
```

### Pattern 2: Post-Pipeline Audit Job

Run FK validation as a separate audit pipeline:

```yaml
pipelines:
  - pipeline: audit_referential_integrity
    description: "Nightly FK validation audit"
    nodes:
      - name: load_dimensions
        read:
          connection: warehouse
          tables:
            - dim_customer
            - dim_product
            - dim_date

      - name: validate_fact_orders
        depends_on: [load_dimensions]
        read:
          connection: warehouse
          path: fact_orders
        transform:
          steps:
            - function: run_fk_audit
              params:
                relationships_file: "config/fk_relationships.yaml"
```

### Pattern 3: Integrated with Data Quality Gate

Use FK validation as a quality gate:

```yaml
nodes:
  - name: fact_orders
    read:
      connection: staging
      path: orders
    transformer: fact
    params:
      grain: [order_id]
      dimensions:
        - source_column: customer_id
          dimension_table: dim_customer
          dimension_key: customer_id
          surrogate_key: customer_sk
    gate:
      - type: custom
        function: fk_validation_gate
        params:
          max_orphan_percent: 0.1  # Fail if > 0.1% orphans
    write:
      connection: warehouse
      path: fact_orders
```

---

## See Also

- [Fact Pattern](../patterns/fact.md) - Build fact tables with orphan handling
- [Dimension Pattern](../patterns/dimension.md) - Build dimensions with unknown member
- [Patterns Overview](../patterns/README.md) - All available patterns
