# Date Dimension Pattern

The `DateDimensionPattern` generates a complete date dimension table with pre-calculated attributes useful for BI/reporting.

## Features

- **Date range generation** from start_date to end_date
- **Fiscal calendar support** with configurable fiscal year start month
- **19 pre-calculated columns** for flexible analysis
- **Unknown member row** (date_sk=0) for orphan FK handling
- **Works with both Spark and Pandas**

## Quick Start

```yaml
nodes:
  - name: dim_date
    pattern:
      type: date_dimension
      params:
        start_date: "2020-01-01"
        end_date: "2030-12-31"
        fiscal_year_start_month: 7  # July fiscal year
        unknown_member: true
    write:
      target: gold.dim_date
      mode: overwrite
```

---

## Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `start_date` | str | Yes | - | Start date in YYYY-MM-DD format |
| `end_date` | str | Yes | - | End date in YYYY-MM-DD format |
| `fiscal_year_start_month` | int | No | 1 | Month when fiscal year starts (1-12) |
| `unknown_member` | bool | No | false | Add unknown date row with date_sk=0 |

---

## Generated Columns

The pattern generates 19 columns automatically:

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `date_sk` | int | Surrogate key (YYYYMMDD format) | 20240115 |
| `full_date` | date | The actual date | 2024-01-15 |
| `day_of_week` | str | Day name | Monday |
| `day_of_week_num` | int | Day number (1=Monday, 7=Sunday) | 1 |
| `day_of_month` | int | Day of month (1-31) | 15 |
| `day_of_year` | int | Day of year (1-366) | 15 |
| `is_weekend` | bool | Weekend flag | false |
| `week_of_year` | int | ISO week number (1-53) | 3 |
| `month` | int | Month number (1-12) | 1 |
| `month_name` | str | Month name | January |
| `quarter` | int | Calendar quarter (1-4) | 1 |
| `quarter_name` | str | Quarter name | Q1 |
| `year` | int | Calendar year | 2024 |
| `fiscal_year` | int | Fiscal year | 2024 |
| `fiscal_quarter` | int | Fiscal quarter (1-4) | 3 |
| `is_month_start` | bool | First day of month | false |
| `is_month_end` | bool | Last day of month | false |
| `is_year_start` | bool | First day of year | false |
| `is_year_end` | bool | Last day of year | false |

---

## Fiscal Calendar Configuration

Configure fiscal year start month for companies with non-calendar fiscal years:

```yaml
pattern:
  type: date_dimension
  params:
    start_date: "2020-01-01"
    end_date: "2030-12-31"
    fiscal_year_start_month: 7  # July 1st = FY start
```

**Fiscal Year Calculation:**
- If `fiscal_year_start_month = 7` (July)
- July 2024 → FY 2025
- June 2024 → FY 2024

**Fiscal Quarter Calculation:**
- Fiscal Q1: July, August, September
- Fiscal Q2: October, November, December
- Fiscal Q3: January, February, March
- Fiscal Q4: April, May, June

---

## Unknown Member Row

Enable `unknown_member: true` to add a special row for orphan FK handling:

```yaml
pattern:
  type: date_dimension
  params:
    start_date: "2020-01-01"
    end_date: "2030-12-31"
    unknown_member: true
```

**Unknown Member Values:**

| Column | Value |
|--------|-------|
| date_sk | 0 |
| full_date | 1900-01-01 |
| day_of_week | Unknown |
| day_of_week_num | 0 |
| month_name | Unknown |
| quarter_name | Unknown |
| year | 0 |
| All other numerics | 0 |
| All booleans | false |

---

## Full YAML Example

Complete date dimension for a data warehouse:

```yaml
project:
  name: date_dimension_pipeline

connections:
  warehouse:
    type: delta
    path: /mnt/warehouse

nodes:
  - name: dim_date
    description: "Standard date dimension with fiscal calendar"
    pattern:
      type: date_dimension
      params:
        start_date: "2015-01-01"
        end_date: "2035-12-31"
        fiscal_year_start_month: 10  # October fiscal year (common for retail)
        unknown_member: true
    write:
      connection: warehouse
      path: dim_date
      mode: overwrite
      partition_by: [year]  # Optional: partition by year for performance
```

---

## Python API

```python
from odibi.patterns.date_dimension import DateDimensionPattern
from odibi.context import EngineContext
from odibi.enums import EngineType

# Create pattern instance
pattern = DateDimensionPattern(params={
    "start_date": "2020-01-01",
    "end_date": "2030-12-31",
    "fiscal_year_start_month": 7,
    "unknown_member": True
})

# Validate configuration
pattern.validate()

# Execute pattern (no source data needed - generates dates)
context = EngineContext(df=None, engine_type=EngineType.SPARK, spark=spark)
result_df = pattern.execute(context)

# Result: DataFrame with 4018 rows (11 years * 365.25 days + unknown member)
result_df.show()
```

**Output:**
```
+--------+----------+-----------+---------------+------------+-----------+----------+------------+-----+----------+-------+------------+----+-----------+--------------+--------------+------------+--------------+------------+
|date_sk |full_date |day_of_week|day_of_week_num|day_of_month|day_of_year|is_weekend|week_of_year|month|month_name|quarter|quarter_name|year|fiscal_year|fiscal_quarter|is_month_start|is_month_end|is_year_start|is_year_end|
+--------+----------+-----------+---------------+------------+-----------+----------+------------+-----+----------+-------+------------+----+-----------+--------------+--------------+------------+--------------+------------+
|       0|1900-01-01|    Unknown|              0|           0|          0|     false|           0|    0|   Unknown|      0|     Unknown|   0|          0|             0|         false|       false|         false|       false|
|20200101|2020-01-01|  Wednesday|              3|           1|          1|     false|           1|    1|   January|      1|          Q1|2020|       2020|             3|          true|       false|          true|       false|
|20200102|2020-01-02|   Thursday|              4|           2|          2|     false|           1|    1|   January|      1|          Q1|2020|       2020|             3|         false|       false|         false|       false|
+--------+----------+-----------+---------------+------------+-----------+----------+------------+-----+----------+-------+------------+----+-----------+--------------+--------------+------------+--------------+------------+
```

---

## Common Use Cases

### Retail Calendar (October FY)
```yaml
fiscal_year_start_month: 10
```

### Government Calendar (October FY)
```yaml
fiscal_year_start_month: 10
```

### Education Calendar (July FY)
```yaml
fiscal_year_start_month: 7
```

### Standard Calendar Year
```yaml
fiscal_year_start_month: 1  # Default
```

---

## Performance Tips

1. **Generate once, use forever**: Date dimensions are static - generate with a wide range (e.g., 2015-2035)
2. **Partition by year**: For very large date ranges, partition the output
3. **Unknown member**: Always enable for FK integrity

---

## See Also

- [Dimension Pattern](./dimension.md) - Build regular dimensions
- [Fact Pattern](./fact.md) - Build fact tables with SK lookups
