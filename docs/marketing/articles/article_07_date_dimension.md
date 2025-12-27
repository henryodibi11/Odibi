# Building a Date Dimension from Scratch

*The one dimension every data warehouse needs*

---

## TL;DR

Every data warehouse needs a date dimension. It enables filtering by month, quarter, fiscal year, day of week, and more-without complex date functions in every query. This article shows how to generate a complete date dimension with Odibi, including fiscal calendars, holiday flags, and the critical unknown member row.

---

## Why Every Warehouse Needs a Date Dimension

You could join fact tables directly on dates:

```sql
SELECT 
  EXTRACT(MONTH FROM order_date) as month,
  SUM(amount)
FROM fact_orders
GROUP BY EXTRACT(MONTH FROM order_date)
```

But this approach has problems:

1. **Repetitive**: Every query repeats date extraction logic
2. **Inconsistent**: Different analysts calculate "fiscal quarter" differently
3. **Slow**: Date functions run on every row
4. **Limited**: Can't filter on "is_holiday" or "is_business_day"

A date dimension solves all of this:

```sql
SELECT 
  d.month_name,
  d.fiscal_quarter,
  SUM(f.amount)
FROM fact_orders f
JOIN dim_date d ON f.order_date_sk = d.date_sk
WHERE d.is_business_day = true
GROUP BY d.month_name, d.fiscal_quarter
```

Pre-calculate once, query forever.

---

## What Columns to Include

A complete date dimension has these categories:

### Core Date Fields

| Column | Example | Description |
|--------|---------|-------------|
| date_sk | 20180315 | Surrogate key (YYYYMMDD) |
| full_date | 2018-03-15 | The actual date |
| day_of_week | Thursday | Day name |
| day_of_week_num | 4 | 1=Sunday, 7=Saturday |
| day_of_month | 15 | 1-31 |
| day_of_year | 74 | 1-366 |

### Week Fields

| Column | Example | Description |
|--------|---------|-------------|
| week_of_year | 11 | ISO week number |
| week_start_date | 2018-03-12 | Monday of that week |
| week_end_date | 2018-03-18 | Sunday of that week |

### Month Fields

| Column | Example | Description |
|--------|---------|-------------|
| month | 3 | 1-12 |
| month_name | March | Full name |
| month_short | Mar | Abbreviated |
| month_start_date | 2018-03-01 | First of month |
| month_end_date | 2018-03-31 | Last of month |

### Quarter Fields

| Column | Example | Description |
|--------|---------|-------------|
| quarter | 1 | 1-4 |
| quarter_name | Q1 | Label |
| quarter_start_date | 2018-01-01 | First of quarter |
| quarter_end_date | 2018-03-31 | Last of quarter |

### Year Fields

| Column | Example | Description |
|--------|---------|-------------|
| year | 2018 | Calendar year |
| year_month | 2018-03 | For sorting |
| year_quarter | 2018-Q1 | For sorting |

### Fiscal Fields

| Column | Example | Description |
|--------|---------|-------------|
| fiscal_year | 2018 | Based on fiscal calendar |
| fiscal_quarter | FQ3 | Fiscal quarter |
| fiscal_month | 9 | Month within fiscal year |
| fiscal_week | 37 | Week within fiscal year |

### Flags

| Column | Example | Description |
|--------|---------|-------------|
| is_weekend | true | Saturday or Sunday |
| is_weekday | false | Monday-Friday |
| is_holiday | false | Company holiday |
| is_month_start | false | First of month |
| is_month_end | false | Last of month |
| is_quarter_start | false | First of quarter |
| is_quarter_end | true | Last of quarter |
| is_year_start | false | January 1 |
| is_year_end | false | December 31 |

---

## Generating with Odibi

Odibi has a built-in pattern for date dimensions:

```yaml
pipelines:
  - pipeline: gold_dimensions
    layer: gold
    description: "Gold layer dimensions"
    
    nodes:
      - name: dim_date
        description: "Date dimension spanning 2015-2030"
        
        pattern:
          type: date_dimension
          params:
            start_date: "2015-01-01"
            end_date: "2030-12-31"
            date_key_format: "yyyyMMdd"  # Creates 20180315 format
            fiscal_year_start_month: 7    # Fiscal year starts in July
            unknown_member: true          # Adds date_sk=0 row
        
        write:
          connection: gold
          path: dim_date
          format: delta
```

This generates:
- 5,844 rows (16 years of dates)
- 20+ columns
- Unknown member row with `date_sk=0`

---

## Generated Columns

Running the pattern generates these columns:

```
date_sk              INTEGER    (20180315)
full_date            DATE       (2018-03-15)
day_of_week          STRING     (Thursday)
day_of_week_num      INTEGER    (4)
day_of_month         INTEGER    (15)
day_of_year          INTEGER    (74)
is_weekend           BOOLEAN    (false)
week_of_year         INTEGER    (11)
month                INTEGER    (3)
month_name           STRING     (March)
quarter              INTEGER    (1)
quarter_name         STRING     (Q1)
year                 INTEGER    (2018)
year_month           STRING     (2018-03)
fiscal_year          INTEGER    (2018)
fiscal_quarter       STRING     (FQ3)
is_month_start       BOOLEAN    (false)
is_month_end         BOOLEAN    (false)
is_quarter_start     BOOLEAN    (false)
is_quarter_end       BOOLEAN    (false)
is_year_start        BOOLEAN    (false)
is_year_end          BOOLEAN    (false)
```

---

## Fiscal Year Handling

Many organizations don't follow the calendar year. 

If your fiscal year starts in July:
- July 2018 = Fiscal Month 1 of FY2019
- June 2019 = Fiscal Month 12 of FY2019

Configure this with `fiscal_year_start_month`:

```yaml
pattern:
  type: date_dimension
  params:
    fiscal_year_start_month: 7  # July
```

The pattern calculates:
- `fiscal_year`: The fiscal year (e.g., 2019 for July 2018)
- `fiscal_quarter`: FQ1-FQ4 based on fiscal months
- `fiscal_month`: 1-12 within fiscal year
- `fiscal_week`: 1-52 within fiscal year

---

## The Unknown Member Row

What happens when a fact record has a null date? Or a date that falls outside your dimension range?

The **unknown member** pattern adds a special row:

| date_sk | full_date | day_of_week | month_name | ... |
|---------|-----------|-------------|------------|-----|
| 0 | NULL | Unknown | Unknown | ... |

Configure it:

```yaml
pattern:
  type: date_dimension
  params:
    unknown_member: true
```

When building fact tables, orphan date lookups map to `date_sk=0`:

```yaml
# In fact table configuration
dimensions:
  - source_column: order_date
    dimension_table: dim_date
    dimension_key: full_date
    surrogate_key: date_sk
# Orphans get date_sk=0 instead of NULL or failure
```

This ensures:
- All facts have valid foreign keys
- Queries always return results
- You can track "unknown date" separately

---

## Adding Holidays

The base pattern doesn't include holidays (they're company-specific). Add them after generation:

```yaml
- name: dim_date
  description: "Date dimension with holidays"
  
  pattern:
    type: date_dimension
    params:
      start_date: "2015-01-01"
      end_date: "2030-12-31"
      unknown_member: true
  
  # Add holiday flags
  transform:
    steps:
      - function: derive_columns
        params:
          columns:
            is_us_holiday: |
              CASE
                -- New Year's Day
                WHEN month = 1 AND day_of_month = 1 THEN true
                -- Independence Day
                WHEN month = 7 AND day_of_month = 4 THEN true
                -- Thanksgiving (4th Thursday of November)
                WHEN month = 11 AND day_of_week = 'Thursday' 
                     AND day_of_month BETWEEN 22 AND 28 THEN true
                -- Christmas
                WHEN month = 12 AND day_of_month = 25 THEN true
                ELSE false
              END
            
            is_business_day: |
              CASE
                WHEN is_weekend = true THEN false
                WHEN is_us_holiday = true THEN false
                ELSE true
              END
  
  write:
    connection: gold
    path: dim_date
    format: delta
```

For complex holiday logic (Easter, floating holidays), you might generate a holiday table separately and join it.

---

## Date Key Format Choices

The `date_key_format` parameter controls how `date_sk` is generated:

| Format | Example | Pros | Cons |
|--------|---------|------|------|
| yyyyMMdd | 20180315 | Human readable, sortable | Large integers |
| yyyy-MM-dd | 2018-03-15 | Very readable | String, slower joins |
| Sequential | 1, 2, 3... | Compact | Not readable |

Most warehouses use `yyyyMMdd` because:
- You can read it: "20180315 = March 15, 2018"
- It sorts correctly numerically
- It's widely understood

---

## Common Queries

Once you have a date dimension, these queries become trivial:

### Sales by Day of Week

```sql
SELECT 
  d.day_of_week,
  SUM(f.amount) as total_sales
FROM fact_orders f
JOIN dim_date d ON f.order_date_sk = d.date_sk
GROUP BY d.day_of_week, d.day_of_week_num
ORDER BY d.day_of_week_num
```

### Month-over-Month Growth

```sql
SELECT 
  d.year_month,
  SUM(f.amount) as monthly_sales,
  LAG(SUM(f.amount)) OVER (ORDER BY d.year_month) as prev_month,
  ROUND(100.0 * (SUM(f.amount) - LAG(SUM(f.amount)) OVER (ORDER BY d.year_month)) 
        / LAG(SUM(f.amount)) OVER (ORDER BY d.year_month), 2) as growth_pct
FROM fact_orders f
JOIN dim_date d ON f.order_date_sk = d.date_sk
GROUP BY d.year_month
ORDER BY d.year_month
```

### Fiscal Year Comparison

```sql
SELECT 
  d.fiscal_year,
  d.fiscal_quarter,
  SUM(f.amount) as sales
FROM fact_orders f
JOIN dim_date d ON f.order_date_sk = d.date_sk
GROUP BY d.fiscal_year, d.fiscal_quarter
ORDER BY d.fiscal_year, d.fiscal_quarter
```

### Business Days Only

```sql
SELECT 
  COUNT(*) as orders,
  AVG(f.amount) as avg_order_value
FROM fact_orders f
JOIN dim_date d ON f.order_date_sk = d.date_sk
WHERE d.is_business_day = true
  AND d.year = 2018
```

### Weekend vs Weekday

```sql
SELECT 
  CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END as day_type,
  COUNT(*) as order_count,
  SUM(f.amount) as total_sales
FROM fact_orders f
JOIN dim_date d ON f.order_date_sk = d.date_sk
GROUP BY d.is_weekend
```

---

## Complete Configuration

Here's the full date dimension configuration:

```yaml
pipelines:
  - pipeline: gold_dimensions
    layer: gold
    description: "Gold layer dimension tables"
    
    nodes:
      - name: dim_date
        description: "Date dimension 2015-2030 with fiscal calendar and holidays"
        
        pattern:
          type: date_dimension
          params:
            start_date: "2015-01-01"
            end_date: "2030-12-31"
            date_key_format: "yyyyMMdd"
            fiscal_year_start_month: 7
            unknown_member: true
        
        transform:
          steps:
            # Add holiday flags (customize for your company)
            - function: derive_columns
              params:
                columns:
                  is_holiday: |
                    CASE
                      WHEN month = 1 AND day_of_month = 1 THEN true
                      WHEN month = 7 AND day_of_month = 4 THEN true
                      WHEN month = 12 AND day_of_month = 25 THEN true
                      ELSE false
                    END
                  
                  is_business_day: |
                    CASE
                      WHEN is_weekend = true THEN false
                      WHEN month = 1 AND day_of_month = 1 THEN false
                      WHEN month = 7 AND day_of_month = 4 THEN false
                      WHEN month = 12 AND day_of_month = 25 THEN false
                      ELSE true
                    END
                  
                  # Relative date flags (useful for dashboards)
                  is_current_month: |
                    CASE 
                      WHEN year = YEAR(CURRENT_DATE()) 
                       AND month = MONTH(CURRENT_DATE()) 
                      THEN true ELSE false 
                    END
                  
                  is_previous_month: |
                    CASE 
                      WHEN full_date >= DATE_TRUNC('month', ADD_MONTHS(CURRENT_DATE(), -1))
                       AND full_date < DATE_TRUNC('month', CURRENT_DATE())
                      THEN true ELSE false 
                    END
        
        write:
          connection: gold
          path: dim_date
          format: delta
          mode: overwrite
```

---

## Verification

After running, verify your date dimension:

```python
from deltalake import DeltaTable
import pandas as pd

dt = DeltaTable("./gold/dim_date")
df = dt.to_pandas()

print(f"Total rows: {len(df)}")
print(f"Date range: {df['full_date'].min()} to {df['full_date'].max()}")
print(f"Has unknown member: {(df['date_sk'] == 0).any()}")
print(f"\nSample:")
print(df.head(10))
```

Expected output:

```
Total rows: 5845
Date range: 2015-01-01 to 2030-12-31
Has unknown member: True

Sample:
   date_sk  full_date day_of_week  month month_name  ...
0        0       None     Unknown      0    Unknown  ...
1 20150101 2015-01-01    Thursday      1    January  ...
2 20150102 2015-01-02      Friday      1    January  ...
```

---

## Key Takeaways

1. **Pre-calculate, don't recalculate** - Date logic belongs in the dimension, not queries
2. **Fiscal calendars matter** - Configure for your organization's fiscal year
3. **Unknown member is essential** - Handles null and out-of-range dates
4. **Add business context** - Holidays, business days, relative flags
5. **Generate once, use forever** - 10+ years of dates is tiny (<10K rows)

---

## Next Steps

With the date dimension complete, we'll build the remaining dimensions and fact tables:

1. Customer, Product, Seller dimensions
2. Order facts with surrogate key lookups
3. Complete star schema assembly

Next article: **Fact Table Patterns: Lookups, Orphans, and Measures**.

---

## Connect

- **LinkedIn:** [Your LinkedIn URL]
- **GitHub:** [Odibi Repository URL]
- **Medium:** Follow for more articles in this series
