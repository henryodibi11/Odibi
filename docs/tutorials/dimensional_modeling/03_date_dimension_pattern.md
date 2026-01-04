# Date Dimension Pattern Tutorial

In this tutorial, you'll learn how to use Odibi's `date_dimension` pattern to generate a complete date dimension table with pre-calculated attributes for reporting and analytics.

**What You'll Learn:**
- Why you need a date dimension
- How the pattern generates dates automatically
- Understanding all 19 generated columns
- Fiscal calendar configuration
- Unknown date handling

---

## Why Do You Need a Date Dimension?

Consider this question: *"What were our sales on Tuesdays in January?"*

Your raw order data looks like this:

| order_id | order_date | amount |
|----------|------------|--------|
| ORD001 | 2024-01-15 | 1,299.99 |
| ORD002 | 2024-01-16 | 249.99 |
| ORD003 | 2024-01-23 | 599.99 |

**Problem:** The date `2024-01-15` doesn't tell you it's a Tuesday. You'd need to calculate that in every query.

**Without a date dimension:**
```sql
-- Complex, repeated logic in every query
SELECT 
    DATENAME(weekday, order_date) AS day_of_week,
    SUM(amount) AS total
FROM orders
WHERE MONTH(order_date) = 1
  AND DATENAME(weekday, order_date) = 'Tuesday'
GROUP BY DATENAME(weekday, order_date);
```

**With a date dimension:**
```sql
-- Simple join, pre-calculated attributes
SELECT 
    d.day_of_week,
    SUM(o.amount) AS total
FROM fact_orders o
JOIN dim_date d ON o.date_sk = d.date_sk
WHERE d.month = 1
  AND d.day_of_week = 'Tuesday'
GROUP BY d.day_of_week;
```

A date dimension also provides:
- **Fiscal calendar** attributes (fiscal year, fiscal quarter)
- **Holiday flags** (is_holiday, holiday_name)
- **Business day calculations** (is_weekend, is_month_start)
- **Consistent naming** ("January" not "1", "Q1" not "1")

---

## The Date Dimension Pattern

Unlike other patterns that transform source data, the `date_dimension` pattern **generates** data. You don't need a `read:` block—just configure the date range and options.

### Basic YAML Configuration

```yaml
project: date_dimension_tutorial
engine: pandas

connections:
  warehouse:
    type: file
    path: ./warehouse

story:
  connection: warehouse
  path: stories

pipelines:
  - pipeline: build_date_dimension
    nodes:
      - name: dim_date
        # No read block needed - pattern generates data
        pattern:
          type: date_dimension
          params:
            start_date: "2024-01-15"
            end_date: "2024-01-28"
            fiscal_year_start_month: 7
            unknown_member: true
        
        write:
          connection: warehouse
          path: dim_date
          format: parquet
          mode: overwrite
```

---

## Step 1: Generate a Small Date Range

Let's generate 14 days (January 15-28, 2024) to see exactly what columns are created.

### Output: dim_date (15 rows including unknown)

Here are the first 10 rows showing all 19 columns:

| date_sk | full_date | day_of_week | day_of_week_num | day_of_month | day_of_year | is_weekend | week_of_year | month | month_name | quarter | quarter_name | year | fiscal_year | fiscal_quarter | is_month_start | is_month_end | is_year_start | is_year_end |
|---------|-----------|-------------|-----------------|--------------|-------------|------------|--------------|-------|------------|---------|--------------|------|-------------|----------------|----------------|--------------|---------------|-------------|
| 0 | 1900-01-01 | Unknown | 0 | 0 | 0 | false | 0 | 0 | Unknown | 0 | Unknown | 0 | 0 | 0 | false | false | false | false |
| 20240115 | 2024-01-15 | Monday | 1 | 15 | 15 | false | 3 | 1 | January | 1 | Q1 | 2024 | 2024 | 3 | false | false | false | false |
| 20240116 | 2024-01-16 | Tuesday | 2 | 16 | 16 | false | 3 | 1 | January | 1 | Q1 | 2024 | 2024 | 3 | false | false | false | false |
| 20240117 | 2024-01-17 | Wednesday | 3 | 17 | 17 | false | 3 | 1 | January | 1 | Q1 | 2024 | 2024 | 3 | false | false | false | false |
| 20240118 | 2024-01-18 | Thursday | 4 | 18 | 18 | false | 3 | 1 | January | 1 | Q1 | 2024 | 2024 | 3 | false | false | false | false |
| 20240119 | 2024-01-19 | Friday | 5 | 19 | 19 | false | 3 | 1 | January | 1 | Q1 | 2024 | 2024 | 3 | false | false | false | false |
| 20240120 | 2024-01-20 | Saturday | 6 | 20 | 20 | **true** | 3 | 1 | January | 1 | Q1 | 2024 | 2024 | 3 | false | false | false | false |
| 20240121 | 2024-01-21 | Sunday | 7 | 21 | 21 | **true** | 3 | 1 | January | 1 | Q1 | 2024 | 2024 | 3 | false | false | false | false |
| 20240122 | 2024-01-22 | Monday | 1 | 22 | 22 | false | 4 | 1 | January | 1 | Q1 | 2024 | 2024 | 3 | false | false | false | false |
| 20240123 | 2024-01-23 | Tuesday | 2 | 23 | 23 | false | 4 | 1 | January | 1 | Q1 | 2024 | 2024 | 3 | false | false | false | false |

### Remaining rows (24-28):

| date_sk | full_date | day_of_week | day_of_week_num | is_weekend | week_of_year | fiscal_year | fiscal_quarter |
|---------|-----------|-------------|-----------------|------------|--------------|-------------|----------------|
| 20240124 | 2024-01-24 | Wednesday | 3 | false | 4 | 2024 | 3 |
| 20240125 | 2024-01-25 | Thursday | 4 | false | 4 | 2024 | 3 |
| 20240126 | 2024-01-26 | Friday | 5 | false | 4 | 2024 | 3 |
| 20240127 | 2024-01-27 | Saturday | 6 | true | 4 | 2024 | 3 |
| 20240128 | 2024-01-28 | Sunday | 7 | true | 4 | 2024 | 3 |

---

## Step 2: Understanding the 19 Columns

### Surrogate Key

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `date_sk` | int | Surrogate key in YYYYMMDD format | 20240115 |

The `date_sk` uses YYYYMMDD format, which:
- Is human-readable (you can see the date in the key)
- Sorts chronologically
- Is efficient for range queries

### Date Columns

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `full_date` | date | The actual date | 2024-01-15 |

### Day Attributes

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `day_of_week` | string | Day name | Monday |
| `day_of_week_num` | int | Day number (1=Monday, 7=Sunday) | 1 |
| `day_of_month` | int | Day of month (1-31) | 15 |
| `day_of_year` | int | Day of year (1-366) | 15 |
| `is_weekend` | bool | Weekend flag | false |

### Week Attributes

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `week_of_year` | int | ISO week number (1-53) | 3 |

### Month Attributes

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `month` | int | Month number (1-12) | 1 |
| `month_name` | string | Month name | January |
| `is_month_start` | bool | First day of month | false |
| `is_month_end` | bool | Last day of month | false |

### Quarter Attributes

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `quarter` | int | Calendar quarter (1-4) | 1 |
| `quarter_name` | string | Quarter name | Q1 |

### Year Attributes

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `year` | int | Calendar year | 2024 |
| `is_year_start` | bool | First day of year | false |
| `is_year_end` | bool | Last day of year | false |

### Fiscal Calendar

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `fiscal_year` | int | Fiscal year | 2024 |
| `fiscal_quarter` | int | Fiscal quarter (1-4) | 3 |

---

## Step 3: Understanding Fiscal Calendars

Many organizations don't use January-December as their fiscal year. Common alternatives:

| Industry | Fiscal Year Start | Example |
|----------|-------------------|---------|
| US Government | October 1 | FY2024 = Oct 2023 - Sep 2024 |
| Retail | February 1 | FY2024 = Feb 2024 - Jan 2025 |
| Education | July 1 | FY2024 = Jul 2023 - Jun 2024 |

### How Fiscal Year Calculation Works

With `fiscal_year_start_month: 7` (July):

| Calendar Date | Calendar Year | Fiscal Year | Why |
|---------------|---------------|-------------|-----|
| June 15, 2024 | 2024 | 2024 | Before fiscal year start |
| July 1, 2024 | 2024 | **2025** | Fiscal year starts |
| December 31, 2024 | 2024 | 2025 | Still FY2025 |

**Formula:** If current month >= fiscal_year_start_month, add 1 to calendar year.

### Fiscal Quarters

With `fiscal_year_start_month: 7`:

| Fiscal Quarter | Months |
|----------------|--------|
| Q1 | July, August, September |
| Q2 | October, November, December |
| Q3 | January, February, March |
| Q4 | April, May, June |

### Example Output with July Fiscal Year

January 2024 dates would show:

| full_date | year | fiscal_year | quarter | fiscal_quarter |
|-----------|------|-------------|---------|----------------|
| 2024-01-15 | 2024 | 2024 | 1 | **3** |
| 2024-01-16 | 2024 | 2024 | 1 | **3** |

Note: January is calendar Q1 but fiscal Q3 (since the fiscal year started in July).

---

## Step 4: The Unknown Date Row

When `unknown_member: true`, a special row is added with `date_sk = 0`:

| Column | Value |
|--------|-------|
| date_sk | 0 |
| full_date | 1900-01-01 |
| day_of_week | Unknown |
| day_of_week_num | 0 |
| day_of_month | 0 |
| day_of_year | 0 |
| is_weekend | false |
| week_of_year | 0 |
| month | 0 |
| month_name | Unknown |
| quarter | 0 |
| quarter_name | Unknown |
| year | 0 |
| fiscal_year | 0 |
| fiscal_quarter | 0 |
| is_month_start | false |
| is_month_end | false |
| is_year_start | false |
| is_year_end | false |

**Use case:** When fact records have NULL or invalid dates, they can be assigned to date_sk = 0 rather than being dropped.

---

## Complete Runnable Example

Here's a complete YAML file for a production-ready date dimension spanning 10 years:

```yaml
# File: odibi_date_dimension_tutorial.yaml
project: date_dimension_tutorial
engine: pandas

connections:
  warehouse:
    type: file
    path: ./warehouse

story:
  connection: warehouse
  path: stories

pipelines:
  - pipeline: build_date_dimension
    description: "Generate date dimension for 10-year range"
    nodes:
      - name: dim_date
        description: "Standard date dimension with fiscal calendar"
        pattern:
          type: date_dimension
          params:
            # 10-year range for typical warehouse
            start_date: "2020-01-01"
            end_date: "2030-12-31"
            
            # July fiscal year (education/government style)
            fiscal_year_start_month: 7
            
            # Add unknown member for orphan handling
            unknown_member: true
        
        write:
          connection: warehouse
          path: dim_date
          format: parquet
          mode: overwrite
```

This generates 4,018 rows (4,017 days + 1 unknown member).

---

## Common Fiscal Year Configurations

### Standard Calendar Year (Default)

```yaml
params:
  start_date: "2020-01-01"
  end_date: "2030-12-31"
  fiscal_year_start_month: 1  # Default
```

### US Government/Retail (October)

```yaml
params:
  start_date: "2020-01-01"
  end_date: "2030-12-31"
  fiscal_year_start_month: 10
```

### Education (July)

```yaml
params:
  start_date: "2020-01-01"
  end_date: "2030-12-31"
  fiscal_year_start_month: 7
```

### UK Tax Year (April)

```yaml
params:
  start_date: "2020-01-01"
  end_date: "2030-12-31"
  fiscal_year_start_month: 4
```

---

## Python API Alternative

```python
from odibi.patterns.date_dimension import DateDimensionPattern
from odibi.context import EngineContext
from odibi.enums import EngineType

# Create pattern
pattern = DateDimensionPattern(params={
    "start_date": "2024-01-15",
    "end_date": "2024-01-28",
    "fiscal_year_start_month": 7,
    "unknown_member": True
})

# Validate configuration
pattern.validate()

# Execute pattern (no input df needed - generates data)
context = EngineContext(df=None, engine_type=EngineType.PANDAS)
result_df = pattern.execute(context)

# View results
print(f"Generated {len(result_df)} date rows")
print("\nColumns:", result_df.columns.tolist())
print("\nSample data:")
print(result_df.head(10).to_string())
```

---

## Querying the Date Dimension

### "What day of the week had the most sales?"

```sql
SELECT 
    d.day_of_week,
    SUM(f.line_total) AS total_sales,
    COUNT(*) AS order_count
FROM fact_orders f
JOIN dim_date d ON f.date_sk = d.date_sk
GROUP BY d.day_of_week
ORDER BY total_sales DESC;
```

### "Show monthly sales trend"

```sql
SELECT 
    d.year,
    d.month_name,
    SUM(f.line_total) AS monthly_sales
FROM fact_orders f
JOIN dim_date d ON f.date_sk = d.date_sk
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;
```

### "Compare weekday vs weekend sales"

```sql
SELECT 
    CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END AS period,
    SUM(f.line_total) AS total_sales,
    AVG(f.line_total) AS avg_order
FROM fact_orders f
JOIN dim_date d ON f.date_sk = d.date_sk
GROUP BY d.is_weekend;
```

### "Fiscal year performance"

```sql
SELECT 
    d.fiscal_year,
    d.fiscal_quarter,
    SUM(f.line_total) AS quarterly_sales
FROM fact_orders f
JOIN dim_date d ON f.date_sk = d.date_sk
GROUP BY d.fiscal_year, d.fiscal_quarter
ORDER BY d.fiscal_year, d.fiscal_quarter;
```

---

## What You Learned

In this tutorial, you learned:

- **Date dimensions** pre-calculate date attributes for easier querying
- The pattern **generates** data—no source file needed
- **19 columns** are created automatically covering day, week, month, quarter, year, and fiscal calendar
- **Surrogate keys** use YYYYMMDD format (e.g., 20240115)
- **Fiscal calendars** can start on any month—the pattern calculates fiscal year and quarter automatically
- **Unknown member** (date_sk = 0) handles NULL or invalid dates in fact tables

---

## Next Steps

Now that you have customer and date dimensions, let's build a fact table that links them together.

**Next:** [Fact Pattern Tutorial](./04_fact_pattern.md)

---

## Navigation

| Previous | Up | Next |
|----------|----|----|
| [Dimension Pattern](./02_dimension_pattern.md) | [Tutorials](../getting_started.md) | [Fact Pattern](./04_fact_pattern.md) |

---

## Reference

For complete parameter documentation, see: [Date Dimension Pattern Reference](../../patterns/date_dimension.md)
