# FK Validation

The FK Validation module declares and validates referential integrity between fact and dimension tables.

## Features

- **Declarative relationships** in YAML
- **Validate FK constraints** on fact table load
- **Detect orphan records** with detailed reporting
- **Generate lineage** from relationships
- **Integration with FactPattern**
- **Multiple violation handlers** (error, warn, filter)

---

## Quick Start

```yaml
relationships:
  - name: orders_to_customers
    fact: fact_orders
    dimension: dim_customer
    fact_key: customer_sk
    dimension_key: customer_sk
    on_violation: error

  - name: orders_to_products
    fact: fact_orders
    dimension: dim_product
    fact_key: product_sk
    dimension_key: product_sk
    on_violation: warn
```

```python
from odibi.validation.fk import (
    RelationshipRegistry,
    FKValidator,
    validate_fk_on_load
)

# Load relationships
registry = RelationshipRegistry(relationships=[...])

# Validate
validator = FKValidator(registry)
report = validator.validate_fact(fact_df, "fact_orders", context)

if not report.all_valid:
    print(f"Orphan records found: {report.orphan_records}")
```

---

## RelationshipConfig

Define a foreign key relationship:

```yaml
relationships:
  - name: orders_to_customers     # Unique identifier
    fact: fact_orders             # Fact table name
    dimension: dim_customer       # Dimension table name
    fact_key: customer_sk         # FK column in fact table
    dimension_key: customer_sk    # PK/SK column in dimension
    nullable: false               # Allow nulls in fact_key?
    on_violation: error           # Action: "error", "warn", "quarantine"
```

### Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | str | Yes | - | Unique relationship identifier |
| `fact` | str | Yes | - | Fact table name |
| `dimension` | str | Yes | - | Dimension table name |
| `fact_key` | str | Yes | - | FK column in fact table |
| `dimension_key` | str | Yes | - | PK/SK column in dimension |
| `nullable` | bool | No | false | Whether nulls are allowed in fact_key |
| `on_violation` | str | No | "error" | Action on violation |

### Violation Actions

| Action | Behavior |
|--------|----------|
| `error` | Raise ValueError, halt pipeline |
| `warn` | Log warning, continue processing |
| `quarantine` | Route orphans to quarantine table |

---

## RelationshipRegistry

Container for relationship definitions:

```python
from odibi.validation.fk import RelationshipRegistry, RelationshipConfig

# Create registry
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
    )
])

# Query relationships
rel = registry.get_relationship("orders_to_customers")
fact_rels = registry.get_fact_relationships("fact_orders")
dim_rels = registry.get_dimension_relationships("dim_customer")

# Generate lineage
lineage = registry.generate_lineage()
# {'fact_orders': ['dim_customer', 'dim_product']}
```

---

## FKValidator

Validate FK relationships:

```python
from odibi.validation.fk import FKValidator, RelationshipRegistry
from odibi.context import EngineContext

# Setup
registry = RelationshipRegistry(relationships=[...])
validator = FKValidator(registry)

# Validate single relationship
result = validator.validate_relationship(
    fact_df,
    registry.get_relationship("orders_to_customers"),
    context
)

print(result.relationship_name)  # 'orders_to_customers'
print(result.valid)              # True/False
print(result.total_rows)         # Total fact rows
print(result.orphan_count)       # Orphan count
print(result.null_count)         # Null FK count
print(result.orphan_values)      # Sample orphan values (up to 100)
print(result.elapsed_ms)         # Validation time

# Validate all relationships for a fact table
report = validator.validate_fact(fact_df, "fact_orders", context)

print(report.fact_table)         # 'fact_orders'
print(report.all_valid)          # True if all relationships valid
print(report.total_relationships)  # Number of relationships checked
print(report.valid_relationships)  # Number that passed
print(report.results)            # List of FKValidationResult
print(report.orphan_records)     # List of OrphanRecord
```

---

## FKValidationResult

Result of validating a single relationship:

| Field | Type | Description |
|-------|------|-------------|
| `relationship_name` | str | Relationship identifier |
| `valid` | bool | Whether validation passed |
| `total_rows` | int | Total rows in fact table |
| `orphan_count` | int | Number of orphan records |
| `null_count` | int | Number of null FK values |
| `orphan_values` | list | Sample orphan values (up to 100) |
| `elapsed_ms` | float | Validation time |
| `error` | str | Error message if failed |

---

## FKValidationReport

Complete report for a fact table:

| Field | Type | Description |
|-------|------|-------------|
| `fact_table` | str | Fact table name |
| `all_valid` | bool | True if all relationships valid |
| `total_relationships` | int | Number of relationships checked |
| `valid_relationships` | int | Number that passed |
| `results` | List[FKValidationResult] | Individual results |
| `orphan_records` | List[OrphanRecord] | All orphan records |
| `elapsed_ms` | float | Total validation time |

---

## validate_fk_on_load

Convenience function for use in pipelines:

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

# Error on violation (default)
validated_df = validate_fk_on_load(
    fact_df=fact_df,
    relationships=relationships,
    context=context,
    on_failure="error"
)

# Warn on violation
validated_df = validate_fk_on_load(
    fact_df=fact_df,
    relationships=relationships,
    context=context,
    on_failure="warn"
)

# Filter orphans
validated_df = validate_fk_on_load(
    fact_df=fact_df,
    relationships=relationships,
    context=context,
    on_failure="filter"
)
# Returns only rows with valid FK references
```

### on_failure Options

| Option | Behavior |
|--------|----------|
| `error` | Raise ValueError with orphan details |
| `warn` | Log warning, return original DataFrame |
| `filter` | Remove orphan rows, return filtered DataFrame |

---

## get_orphan_records

Extract orphan records for analysis:

```python
from odibi.validation.fk import get_orphan_records
from odibi.enums import EngineType

orphans_df = get_orphan_records(
    fact_df=fact_df,
    relationship=relationship_config,
    dim_df=dim_df,
    engine_type=EngineType.SPARK
)

# Returns DataFrame containing only orphan rows
orphans_df.show()
```

---

## YAML Configuration

Full relationship configuration in YAML:

```yaml
# relationships.yaml
relationships:
  # Customer dimension
  - name: orders_to_customers
    fact: fact_orders
    dimension: dim_customer
    fact_key: customer_sk
    dimension_key: customer_sk
    nullable: false
    on_violation: error

  # Product dimension
  - name: orders_to_products
    fact: fact_orders
    dimension: dim_product
    fact_key: product_sk
    dimension_key: product_sk
    nullable: false
    on_violation: error

  # Date dimension (nullable for pending orders)
  - name: orders_to_dates
    fact: fact_orders
    dimension: dim_date
    fact_key: order_date_sk
    dimension_key: date_sk
    nullable: true
    on_violation: warn

  # Ship date (optional)
  - name: orders_to_ship_dates
    fact: fact_orders
    dimension: dim_date
    fact_key: ship_date_sk
    dimension_key: date_sk
    nullable: true
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

## Integration with FactPattern

Use FK validation in your fact table pipeline:

```python
from odibi.patterns.fact import FactPattern
from odibi.validation.fk import validate_fk_on_load, RelationshipConfig

# Define pattern
pattern = FactPattern(params={
    "grain": ["order_id"],
    "dimensions": [...],
    "orphan_handling": "unknown"  # Pattern handles during lookup
})

# Execute pattern
result_df = pattern.execute(context)

# Additional FK validation post-pattern
relationships = [
    RelationshipConfig(
        name="verify_customer_sk",
        fact="fact_orders",
        dimension="dim_customer",
        fact_key="customer_sk",
        dimension_key="customer_sk"
    )
]

# Validate (will detect if pattern's unknown member wasn't used)
validated_df = validate_fk_on_load(
    fact_df=result_df,
    relationships=relationships,
    context=context,
    on_failure="warn"
)
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
        ...
    ),
    RelationshipConfig(
        name="orders_to_products",
        fact="fact_orders",
        dimension="dim_product",
        ...
    ),
    RelationshipConfig(
        name="line_items_to_orders",
        fact="fact_line_items",
        dimension="fact_orders",
        ...
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

Complete FK validation pipeline:

```python
from odibi.validation.fk import (
    RelationshipConfig,
    RelationshipRegistry,
    FKValidator,
    validate_fk_on_load,
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
    ),
    RelationshipConfig(
        name="orders_to_dates",
        fact="fact_orders",
        dimension="dim_date",
        fact_key="date_sk",
        dimension_key="date_sk",
        nullable=True,
        on_violation="warn"
    )
]

# Create registry and validator
registry = RelationshipRegistry(relationships=relationships)
validator = FKValidator(registry)

# Setup context with dimension tables
context = EngineContext(df=None, engine_type=EngineType.SPARK, spark=spark)
context.register("dim_customer", spark.table("gold.dim_customer"))
context.register("dim_product", spark.table("gold.dim_product"))
context.register("dim_date", spark.table("gold.dim_date"))

# Load fact table
fact_df = spark.table("silver.fact_orders")

# Validate all relationships
report = validator.validate_fact(fact_df, "fact_orders", context)

# Report summary
print(f"Validation {'PASSED' if report.all_valid else 'FAILED'}")
print(f"Checked {report.total_relationships} relationships")
print(f"Valid: {report.valid_relationships}")
print(f"Total time: {report.elapsed_ms:.0f}ms")

# Detail per relationship
for result in report.results:
    status = "PASS" if result.valid else "FAIL"
    print(f"  {result.relationship_name}: {status}")
    if not result.valid:
        print(f"    Orphans: {result.orphan_count}")
        print(f"    Nulls: {result.null_count}")
        if result.orphan_values:
            print(f"    Sample values: {result.orphan_values[:5]}")

# Extract orphans for analysis
if report.orphan_records:
    for rel in relationships:
        orphans = get_orphan_records(
            fact_df,
            rel,
            context.get(rel.dimension),
            context.engine_type
        )
        if orphans.count() > 0:
            print(f"\nOrphans for {rel.name}:")
            orphans.show(5)

# Generate lineage
lineage = registry.generate_lineage()
print(f"\nLineage: {lineage}")
```

---

## See Also

- [Fact Pattern](../patterns/fact.md) - Build fact tables with orphan handling
- [Dimension Pattern](../patterns/dimension.md) - Build dimensions with unknown member
- [Data Patterns](../patterns/README.md) - Overview of patterns
