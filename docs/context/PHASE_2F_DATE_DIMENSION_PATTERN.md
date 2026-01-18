# PHASE 2F: DATE DIMENSION PATTERN ANALYSIS

## Overview
The `DateDimensionPattern` in the Odibi framework is designed to generate a comprehensive date dimension table with pre-calculated attributes. These attributes are invaluable for Business Intelligence (BI) and reporting, supporting a wide range of analytical queries.

---

## Configuration Parameters

### Required Parameters:
1. **`start_date`** (str)
   - The start date for the date dimension in `YYYY-MM-DD` format.
   - **Required**: Yes.
   - **Example**: `"2024-01-01"`.

2. **`end_date`** (str)
   - The end date for the date dimension in `YYYY-MM-DD` format.
   - **Required**: Yes.
   - **Example**: `"2024-12-31"`.

---

### Optional Parameters:
1. **`date_key_format`** (str)
   - Format for the surrogate key (`date_sk`).
   - **Default**: `"yyyyMMdd"`.

2. **`fiscal_year_start_month`** (int)
   - Specifies the starting month of the fiscal year.
   - **Default**: `1` (January). Range: 1–12.

3. **`unknown_member`** (bool)
   - Adds an unknown date row (`date_sk=0`) to handle null or missing dates.
   - **Default**: False.

---

## Generated Columns
1. **Surrogate Key**
   - `date_sk`: Integer key (default is in `YYYYMMDD` format).

2. **Date Attributes**
   - `full_date`: Actual date.
   - `day_of_week`: Day name.
   - `day_of_week_num`: Day number (Monday = 1, Sunday = 7).
   - `day_of_month`: Day of the month (1–31).
   - `day_of_year`: Day of the year (1–366).
   - `is_weekend`: Boolean indicator for weekends.

3. **Calendar Periods**
   - `week_of_year`: Week of the year (ISO standard, 1–53).
   - `month`: Month number (1–12).
   - `month_name`: Month name.
   - `quarter`: Quarter of the year (1–4).
   - `quarter_name`: Quarter designation (`Q1`, `Q2`, etc.).
   - `year`: Calendar year.
   - `fiscal_year`: Fiscal year based on the specified fiscal start month.
   - `fiscal_quarter`: Fiscal quarter (1–4).

4. **Boundary Indicators**
   - `is_month_start`: First day of the month.
   - `is_month_end`: Last day of the month.
   - `is_year_start`: First day of the year.
   - `is_year_end`: Last day of the year.

---

## Key Methods

### 1. `validate(self) -> None`
- Ensures the presence of valid `start_date` and `end_date`.
- Verifies that:
   - Dates are in `YYYY-MM-DD` format.
   - `start_date` is before or equal to `end_date`.
   - `fiscal_year_start_month` is an integer between 1 and 12.

### 2. `execute(self, context: EngineContext) -> Any`
- Generates the Date Dimension table using either Spark or Pandas:
   1. **Pandas**:
      - Uses `pandas.date_range` to generate rows between `start_date` and `end_date`.
      - Populates all supported date attributes.
   2. **Spark**:
      - Uses distributed processing to efficiently generate the date dimension.

---

## Workflow

1. **Validation**:
   - Validates `start_date`, `end_date`, and `fiscal_year_start_month` for integrity.

2. **Data Generation**:
   - Creates records for each date between `start_date` and `end_date`, pre-calculating attribute fields.

3. **Integration**:
   - Supports generating the dimension using Pandas or Spark. It chooses the appropriate implementation based on the `engine_type` in the `EngineContext`.

---

## Example YAML Configuration

```yaml
pattern:
  type: date_dimension
  params:
    start_date: "2024-01-01"
    end_date: "2024-12-31"
    date_key_format: "yyyyMMdd"
    fiscal_year_start_month: 4
    unknown_member: true
```

---

## Observations and Gotchas

1. **Date Range Validations**:
   - If `start_date` is after `end_date`, validation fails immediately.
   - Dates must follow the `YYYY-MM-DD` format.

2. **Unknown Date Member**:
   - Setting `unknown_member=True` generates a placeholder row with `date_sk=0` for missing dates.

3. **Fiscal Year Handling**:
   - The fiscal year calculations adjust based on the starting month. For example:
     - Given a fiscal start month of `4` (April):
       - Fiscal Quarters:
         - Jan–Mar = Q4 of the previous year.
         - Apr–Jun = Q1 of the current year.

---

## Summary

The `DateDimensionPattern` provides a simple yet powerful solution for generating date dimensions essential for BI and analytics. Its flexibility in configuration and comprehensive attribute generation make it a valuable asset in any data warehouse or ETL process.

---

## Next Steps
The analysis and documentation for the Patterns phase (PHASE 2) have now been completed as planned. The next step is to proceed with documenting Transformers.
