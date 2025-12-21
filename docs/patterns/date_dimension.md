# Date Dimension Pattern

The `date_dimension` pattern generates a complete date dimension table with pre-calculated attributes useful for BI/reporting.

## Integration with Odibi YAML

The date dimension pattern is unique - it **generates** data rather than transforming it. No `read:` block is needed.

```yaml
project: my_warehouse
engine: spark

connections:
  warehouse:
    type: delta
    path: /mnt/warehouse

story:
  connection: warehouse
  path: stories

pipelines:
  - pipeline: build_date_dimension
    nodes:
      - name: dim_date
        # No read block - pattern generates data
        transformer: date_dimension
        params:
          start_date: "2020-01-01"
          end_date: "2030-12-31"
          fiscal_year_start_month: 7  # July fiscal year
          unknown_member: true
        write:
          connection: warehouse
          path: dim_date
          format: delta
          mode: overwrite
```

---

## Features

- **Date range generation** from start_date to end_date
- **Fiscal calendar support** with configurable fiscal year start month
- **19 pre-calculated columns** for flexible analysis
- **Unknown member row** (date_sk=0) for orphan FK handling
- **Works with both Spark and Pandas**

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
nodes:
  - name: dim_date
    transformer: date_dimension
    params:
      start_date: "2020-01-01"
      end_date: "2030-12-31"
      fiscal_year_start_month: 7  # July 1st = FY start
    write:
      connection: warehouse
      path: dim_date
      mode: overwrite
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

| Column | Value |
|--------|-------|
| date_sk | 0 |
| full_date | 1900-01-01 |
| day_of_week | Unknown |
| day_of_week_num | 0 |
| month_name | Unknown |
| quarter_name | Unknown |
| year | 0 |

---

## Full YAML Example

Complete date dimension in a warehouse pipeline:

```yaml
project: sales_warehouse
engine: spark

connections:
  warehouse:
    type: delta
    path: /mnt/warehouse

story:
  connection: warehouse
  path: stories

system:
  connection: warehouse
  path: _system_catalog

pipelines:
  - pipeline: build_reference_dimensions
    description: "Build date and other reference dimensions"
    nodes:
      - name: dim_date
        description: "Standard date dimension with fiscal calendar"
        transformer: date_dimension
        params:
          start_date: "2015-01-01"
          end_date: "2035-12-31"
          fiscal_year_start_month: 10  # October fiscal year (retail)
          unknown_member: true
        write:
          connection: warehouse
          path: dim_date
          format: delta
          mode: overwrite
          partition_by: [year]  # Optional: partition by year
```

---

## Common Fiscal Year Configurations

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

---

## Complete Configuration Reference

### All Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `start_date` | str | Yes | - | Start date in YYYY-MM-DD format |
| `end_date` | str | Yes | - | End date in YYYY-MM-DD format |
| `fiscal_year_start_month` | int | No | 1 | Month when fiscal year starts (1-12) |
| `unknown_member` | bool | No | false | Add unknown date row with date_sk=0 |
| `date_format` | str | No | "%Y-%m-%d" | Format string for date parsing |
| `week_start_day` | int | No | 1 | First day of week (1=Monday, 7=Sunday) |
| `include_holidays` | bool | No | false | Generate is_holiday column (requires holiday_country) |
| `holiday_country` | str | No | "US" | Country code for holiday calendar |

### Advanced Configuration Example

```yaml
nodes:
  - name: dim_date
    transformer: date_dimension
    params:
      # Required
      start_date: "2015-01-01"
      end_date: "2035-12-31"
      
      # Fiscal calendar
      fiscal_year_start_month: 7      # July fiscal year
      
      # Unknown member
      unknown_member: true            # Add SK=0 row for orphans
      
      # Week configuration
      week_start_day: 1               # Monday (ISO standard)
      
      # Holiday support (if holidays package installed)
      include_holidays: true
      holiday_country: "US"
    write:
      connection: warehouse
      path: dim_date
      format: delta
      mode: overwrite
```

### Output Column Customization

To customize output columns, add a SQL step after generation:

```yaml
nodes:
  - name: dim_date_raw
    transformer: date_dimension
    params:
      start_date: "2020-01-01"
      end_date: "2030-12-31"
      fiscal_year_start_month: 10
      unknown_member: true

  - name: dim_date
    depends_on: [dim_date_raw]
    transform:
      steps:
        - sql: |
            SELECT 
              date_sk,
              full_date,
              day_of_week,
              month_name,
              quarter_name,
              year,
              fiscal_year,
              fiscal_quarter,
              is_weekend,
              -- Custom columns
              CONCAT(year, '-', LPAD(month, 2, '0')) AS year_month,
              CASE WHEN month IN (11, 12) THEN true ELSE false END AS is_holiday_season
            FROM dim_date_raw
    write:
      connection: warehouse
      path: dim_date
```

---

## See Also

- [Dimension Pattern](./dimension.md) - Build regular dimensions
- [Fact Pattern](./fact.md) - Build fact tables with SK lookups
- [YAML Schema Reference](../reference/yaml_schema.md) - Full configuration reference
