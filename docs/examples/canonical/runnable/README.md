# Runnable Canonical Examples

Ready-to-run versions of the canonical examples. Each config uses sample data from `../sample_data/`.

## Quick Start

```bash
cd docs/examples/canonical/runnable

# Run any example
odibi run 01_hello_world.yaml
odibi run 03_scd2_dimension.yaml
odibi run 04_fact_table.yaml
```

## Examples

| Example | Description | Patterns Used |
|---------|-------------|---------------|
| **01_hello_world.yaml** | CSV → Parquet ingestion | Basic read/write |
| **03_scd2_dimension.yaml** | Customer dimension with history tracking | `dimension` (SCD2) |
| **04_fact_table.yaml** | Complete star schema with FK lookups | `dimension`, `date_dimension`, `fact` |

## Sample Data

Located in `../sample_data/`:

| File | Rows | Description |
|------|------|-------------|
| `customers.csv` | 5 | Customer records (customer_id 1-5) |
| `products.csv` | 5 | Product catalog (P100-P500) |
| `orders.csv` | 6 | Order transactions (includes orphan customer_id=999) |

## Expected Outputs

### 01_hello_world.yaml

Creates `./data/bronze/customers/` with 5 customer records in Parquet format.

### 03_scd2_dimension.yaml

Creates `./data/silver/dim_customer/` with:
- 6 rows (5 customers + 1 unknown member row with customer_sk=0)
- SCD2 columns: `valid_from`, `valid_to`, `is_current`

### 04_fact_table.yaml

Creates star schema in `./data/gold/`:

```
./data/gold/
├── dim_customer/     # 6 rows (5 customers + unknown member)
├── dim_product/      # 6 rows (5 products + unknown member)
├── dim_date/         # 366 rows (2025 dates + unknown member)
├── fact_sales/       # 6 rows with surrogate keys
├── stories/          # Pipeline metadata
└── _system/          # System tables
```

**Key Demonstrations:**

| Feature | Example |
|---------|---------|
| Dimension pattern | `dim_customer`, `dim_product` with surrogate keys |
| Date dimension | Generated `dim_date` with 19 pre-calculated columns |
| Fact pattern | FK lookups from dimensions |
| Orphan handling | `customer_id=999` → `customer_sk=0` |
| Unknown member | SK=0 rows created in all dimensions |

**Orphan Handling:** Order O005 has `customer_id=999` (doesn't exist in customers.csv). The fact pattern maps it to `customer_sk=0` (unknown member).

**Note on Date Lookups:** The date dimension `full_date` column uses Python `date` objects. If your source data has date strings, add a transform to convert them before the fact pattern, or use a string-based dimension key.

## Validation

```bash
# Validate config syntax before running
odibi validate 04_fact_table.yaml

# Run with verbose output
odibi run 04_fact_table.yaml --verbose
```

## Cleanup

```bash
# Remove generated data
rm -rf ./data
```

## Troubleshooting

**"Dimension table not found in context"**
- Ensure dimension nodes run before the fact node
- Check `depends_on` is set correctly

**"No module named 'deltalake'"**
- Install delta support: `pip install deltalake`
- Or change `format: delta` to `format: parquet`
