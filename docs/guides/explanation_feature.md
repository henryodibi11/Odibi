# Explanation Feature Guide

The `explanation` field allows you to document **business logic, data mappings, and transformation rationale** directly in your pipeline definitions. This documentation is rendered in **Data Story** HTML reports, making pipelines self-documenting for stakeholders.

## Why Use Explanations?

- **Bridge technical and business context**: Explain *why* a transformation happens, not just *what* it does
- **Stakeholder communication**: Business analysts can understand pipelines without reading code
- **Audit trail**: Document data lineage decisions and business rules
- **Self-service pipelines**: Enable non-engineers to understand and trust data transformations

## Basic Usage (Inline)

Use the `explanation` field for short explanations directly in YAML:

```yaml
nodes:
  - name: fact_sales
    explanation: |
      ## Sales Fact Table

      Joins orders with dimension tables to create the analytical fact grain.

      | Source Column | Lookup | Result |
      |--------------|--------|--------|
      | customer_id | dim_customer | customer_sk |
      | product_id | dim_product | product_sk |

      **Business Rule:** Orphan orders (missing customer/product) get `unknown_member_sk = -1`
    read:
      connection: bronze
      path: orders
    transform:
      steps:
        - function: lookup
          params:
            lookup_source: dim_customer
            on: customer_id
```

## External Files (explanation_file)

For longer documentation, reference an external Markdown file to keep YAML clean:

```yaml
nodes:
  - name: fact_sales
    explanation_file: docs/fact_sales.md  # Relative to this YAML file
    read:
      connection: bronze
      path: orders
```

**docs/fact_sales.md:**
```markdown
## Sales Fact Table

This fact table represents individual sales transactions at the order-line grain.

### Data Sources

| Source | Description |
|--------|-------------|
| orders | Raw order events from POS system |
| dim_customer | Customer dimension (SCD2) |
| dim_product | Product dimension |

### Business Rules

1. **Orphan Handling**: Orders without matching customers/products receive `unknown_member_sk = -1`
2. **Currency**: All amounts converted to USD at transaction-date rate
3. **Returns**: Negative quantities indicate returns, linked via `original_order_id`

### Data Quality

- Orders older than 90 days are archived (not in this table)
- Duplicate order IDs are deduped by latest timestamp

### Stakeholder Notes

Contact: data-team@company.com  
SLA: Refreshed daily by 6am UTC
```

## Markdown Features

Explanations support full GitHub-flavored Markdown:

### Tables

```yaml
explanation: |
  | Column | Type | Description |
  |--------|------|-------------|
  | order_id | string | Unique order identifier |
  | amount | decimal | Order total in USD |
```

### Code Blocks

```yaml
explanation: |
  The join logic follows this pattern:

  ```sql
  SELECT o.*, c.customer_sk
  FROM orders o
  LEFT JOIN dim_customer c ON o.customer_id = c.customer_id
  ```
```

### Headers and Lists

```yaml
explanation: |
  ## Data Flow

  1. Raw events from Kafka
  2. Deduplication by event_id
  3. Enrichment with customer attributes

  ### Edge Cases

  - Late-arriving events: Merged into existing records
  - Schema changes: Handled via schema evolution
```

### Callouts

```yaml
explanation: |
  > **⚠️ Important**: This table contains PII. Access is restricted.

  > **📝 Note**: Amounts are in cents, divide by 100 for dollars.
```

## Mutual Exclusivity

You cannot use both `explanation` and `explanation_file` on the same node:

```yaml
# ❌ Invalid - will raise validation error
nodes:
  - name: bad_example
    explanation: "Inline explanation"
    explanation_file: docs/explanation.md

# ✅ Valid - use one or the other
nodes:
  - name: inline_example
    explanation: "Short inline explanation"

  - name: file_example
    explanation_file: docs/detailed_explanation.md
```

## Path Resolution

`explanation_file` paths are resolved **relative to the YAML file** containing the node, not the project root:

```
project/
├── project.yaml
├── pipelines/
│   ├── sales/
│   │   ├── pipeline.yaml        # explanation_file: docs/fact_sales.md
│   │   └── docs/
│   │       └── fact_sales.md    # ← Resolved here
```

## Viewing in Data Stories

After running your pipeline, view explanations in the Data Story:

```bash
odibi story last                    # Open most recent story
odibi story last --node fact_sales  # View specific node
```

The explanation appears in the **Node Details** section with full Markdown rendering.

## Best Practices

### Do Document

- **Business rules**: "Orders under $10 are excluded from commissions"
- **Data mappings**: Which source columns map to which targets
- **Edge cases**: How nulls, duplicates, and late-arriving data are handled
- **Stakeholder context**: Who uses this data and for what

### Don't Document

- **Technical implementation**: That belongs in code comments
- **Temporary notes**: Use TODO comments instead
- **Sensitive information**: Explanations are visible in reports

### Organization Tips

1. **Use headers** to structure long explanations
2. **Use tables** for column mappings and lookups
3. **Use external files** for explanations over 20 lines
4. **Keep inline explanations** under 10 lines

## Integration with Explanation Linter

Odibi includes an explanation linter that identifies:
- Nodes missing explanations
- TODO placeholders in explanations
- Incomplete documentation

```bash
odibi validate --check-explanations
```

## See Also

- [Data Story Guide](../reference/story.md) - Understanding story reports
- [YAML Schema Reference](../reference/yaml_schema.md) - Full NodeConfig documentation
- [Best Practices](best_practices.md) - Pipeline documentation standards
