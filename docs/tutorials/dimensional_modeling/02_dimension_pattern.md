# Dimension Pattern Tutorial

In this tutorial, you'll learn how to use Odibi's `dimension` pattern to build dimension tables with automatic surrogate key generation and SCD (Slowly Changing Dimension) support.

**What You'll Learn:**
- How surrogate keys are generated
- SCD Type 0 (static) - never update
- SCD Type 1 (overwrite) - update in place
- SCD Type 2 (history) - track all changes
- Unknown member handling

---

## Source Data

We'll start with this customer data (12 rows):

**Source Data (customers.csv) - 12 rows:**

| customer_id | name | email | region | city | state |
|-------------|------|-------|--------|------|-------|
| C001 | Alice Johnson | alice@example.com | North | Chicago | IL |
| C002 | Bob Smith | bob@example.com | South | Houston | TX |
| C003 | Carol White | carol@example.com | North | Detroit | MI |
| C004 | David Brown | david@example.com | East | New York | NY |
| C005 | Emma Davis | emma@example.com | West | Seattle | WA |
| C006 | Frank Miller | frank@example.com | South | Miami | FL |
| C007 | Grace Lee | grace@example.com | East | Boston | MA |
| C008 | Henry Wilson | henry@example.com | West | Portland | OR |
| C009 | Ivy Chen | ivy@example.com | North | Minneapolis | MN |
| C010 | Jack Taylor | jack@example.com | South | Dallas | TX |
| C011 | Karen Martinez | karen@example.com | East | Philadelphia | PA |
| C012 | Leo Anderson | leo@example.com | West | Denver | CO |

---

## Step 1: SCD Type 0 - Static Dimensions

**When to use:** Reference data that never changes (country codes, fixed lookups).

SCD Type 0 creates surrogate keys but never updates existing records. New records are inserted, but changes to existing records are ignored.

### YAML Configuration

```yaml
project: dimension_tutorial
engine: pandas

connections:
  source:
    type: local
    path: ./data
  warehouse:
    type: local
    path: ./warehouse

story:
  connection: warehouse
  path: stories

pipelines:
  - pipeline: build_dim_customer_scd0
    nodes:
      - name: dim_customer
        read:
          connection: source
          path: customers.csv
          format: csv

        pattern:
          type: dimension
          params:
            natural_key: customer_id
            surrogate_key: customer_sk
            scd_type: 0
            unknown_member: true

        write:
          connection: warehouse
          path: dim_customer
          format: parquet
          mode: overwrite
```

### Output: dim_customer (13 rows)

After running with `scd_type: 0`, here's the dimension table with generated surrogate keys:

| customer_sk | customer_id | name | email | region | city | state | load_timestamp |
|-------------|-------------|------|-------|--------|------|-------|----------------|
| 0 | -1 | Unknown | Unknown | Unknown | Unknown | Unknown | 1900-01-01 00:00:00 |
| 1 | C001 | Alice Johnson | alice@example.com | North | Chicago | IL | 2024-01-15 10:00:00 |
| 2 | C002 | Bob Smith | bob@example.com | South | Houston | TX | 2024-01-15 10:00:00 |
| 3 | C003 | Carol White | carol@example.com | North | Detroit | MI | 2024-01-15 10:00:00 |
| 4 | C004 | David Brown | david@example.com | East | New York | NY | 2024-01-15 10:00:00 |
| 5 | C005 | Emma Davis | emma@example.com | West | Seattle | WA | 2024-01-15 10:00:00 |
| 6 | C006 | Frank Miller | frank@example.com | South | Miami | FL | 2024-01-15 10:00:00 |
| 7 | C007 | Grace Lee | grace@example.com | East | Boston | MA | 2024-01-15 10:00:00 |
| 8 | C008 | Henry Wilson | henry@example.com | West | Portland | OR | 2024-01-15 10:00:00 |
| 9 | C009 | Ivy Chen | ivy@example.com | North | Minneapolis | MN | 2024-01-15 10:00:00 |
| 10 | C010 | Jack Taylor | jack@example.com | South | Dallas | TX | 2024-01-15 10:00:00 |
| 11 | C011 | Karen Martinez | karen@example.com | East | Philadelphia | PA | 2024-01-15 10:00:00 |
| 12 | C012 | Leo Anderson | leo@example.com | West | Denver | CO | 2024-01-15 10:00:00 |

**Key observations:**
- **Row 0** is the unknown member (customer_sk = 0, customer_id = -1)
- Surrogate keys are sequential integers starting at 1
- Each source row gets a unique SK

---

## Step 2: SCD Type 1 - Overwrite Updates

**When to use:** Attributes where you only need the current value (email, phone, preferences).

SCD Type 1 updates existing records in place when changes are detected. No history is preserved.

### The Update Scenario

Three customers changed their email addresses:

**Updated Source Data (customers_updated.csv) - 3 changes highlighted:**

| customer_id | name | email | region | city | state |
|-------------|------|-------|--------|------|-------|
| C001 | Alice Johnson | **alice.johnson@newmail.com** | North | Chicago | IL |
| C002 | Bob Smith | bob@example.com | South | Houston | TX |
| C003 | Carol White | carol@example.com | North | Detroit | MI |
| C004 | David Brown | **david.b@corporate.com** | East | New York | NY |
| C005 | Emma Davis | emma@example.com | West | Seattle | WA |
| C006 | Frank Miller | frank@example.com | South | Miami | FL |
| C007 | Grace Lee | **grace.lee@gmail.com** | East | Boston | MA |
| C008 | Henry Wilson | henry@example.com | West | Portland | OR |
| C009 | Ivy Chen | ivy@example.com | North | Minneapolis | MN |
| C010 | Jack Taylor | jack@example.com | South | Dallas | TX |
| C011 | Karen Martinez | karen@example.com | East | Philadelphia | PA |
| C012 | Leo Anderson | leo@example.com | West | Denver | CO |

### YAML Configuration

```yaml
project: dimension_tutorial
engine: pandas

connections:
  source:
    type: local
    path: ./data
  warehouse:
    type: local
    path: ./warehouse

story:
  connection: warehouse
  path: stories

pipelines:
  - pipeline: build_dim_customer_scd1
    nodes:
      - name: dim_customer
        read:
          connection: source
          path: customers_updated.csv
          format: csv

        pattern:
          type: dimension
          params:
            natural_key: customer_id
            surrogate_key: customer_sk
            scd_type: 1
            track_cols:
              - name
              - email
              - region
              - city
              - state
            target: warehouse.dim_customer
            unknown_member: true
            audit:
              load_timestamp: true

        write:
          connection: warehouse
          path: dim_customer
          format: parquet
          mode: overwrite
```

### Before vs After Comparison

**BEFORE (original load):**

| customer_sk | customer_id | email | load_timestamp |
|-------------|-------------|-------|----------------|
| 0 | -1 | Unknown | 1900-01-01 00:00:00 |
| 1 | C001 | alice@example.com | 2024-01-15 10:00:00 |
| 4 | C004 | david@example.com | 2024-01-15 10:00:00 |
| 7 | C007 | grace@example.com | 2024-01-15 10:00:00 |
| ... | ... | ... | ... |

**AFTER (SCD1 update):**

| customer_sk | customer_id | email | load_timestamp |
|-------------|-------------|-------|----------------|
| 0 | -1 | Unknown | 1900-01-01 00:00:00 |
| 1 | C001 | **alice.johnson@newmail.com** | **2024-01-20 14:30:00** |
| 4 | C004 | **david.b@corporate.com** | **2024-01-20 14:30:00** |
| 7 | C007 | **grace.lee@gmail.com** | **2024-01-20 14:30:00** |
| ... | ... | ... | ... |

**Key observations:**
- **Same surrogate keys** - C001 is still customer_sk = 1
- **Values updated in place** - old emails are gone
- **Timestamp updated** - shows when the record was last modified
- **No history preserved** - we can't see the old email addresses

### Complete SCD1 Output (13 rows)

| customer_sk | customer_id | name | email | region | city | state | load_timestamp |
|-------------|-------------|------|-------|--------|------|-------|----------------|
| 0 | -1 | Unknown | Unknown | Unknown | Unknown | Unknown | 1900-01-01 00:00:00 |
| 1 | C001 | Alice Johnson | alice.johnson@newmail.com | North | Chicago | IL | 2024-01-20 14:30:00 |
| 2 | C002 | Bob Smith | bob@example.com | South | Houston | TX | 2024-01-15 10:00:00 |
| 3 | C003 | Carol White | carol@example.com | North | Detroit | MI | 2024-01-15 10:00:00 |
| 4 | C004 | David Brown | david.b@corporate.com | East | New York | NY | 2024-01-20 14:30:00 |
| 5 | C005 | Emma Davis | emma@example.com | West | Seattle | WA | 2024-01-15 10:00:00 |
| 6 | C006 | Frank Miller | frank@example.com | South | Miami | FL | 2024-01-15 10:00:00 |
| 7 | C007 | Grace Lee | grace.lee@gmail.com | East | Boston | MA | 2024-01-20 14:30:00 |
| 8 | C008 | Henry Wilson | henry@example.com | West | Portland | OR | 2024-01-15 10:00:00 |
| 9 | C009 | Ivy Chen | ivy@example.com | North | Minneapolis | MN | 2024-01-15 10:00:00 |
| 10 | C010 | Jack Taylor | jack@example.com | South | Dallas | TX | 2024-01-15 10:00:00 |
| 11 | C011 | Karen Martinez | karen@example.com | East | Philadelphia | PA | 2024-01-15 10:00:00 |
| 12 | C012 | Leo Anderson | leo@example.com | West | Denver | CO | 2024-01-15 10:00:00 |

---

## Step 3: SCD Type 2 - Full History Tracking

**When to use:** Attributes where historical accuracy matters (address for shipping analysis, tier for billing history).

SCD Type 2 preserves full history by creating a new row for each change. Old versions are closed with a `valid_to` date.

### The History Scenario

Same three customers changed their emails. With SCD2, we keep both versions:

### YAML Configuration

```yaml
project: dimension_tutorial
engine: pandas

connections:
  source:
    type: local
    path: ./data
  warehouse:
    type: local
    path: ./warehouse

story:
  connection: warehouse
  path: stories

pipelines:
  - pipeline: build_dim_customer_scd2
    nodes:
      - name: dim_customer
        read:
          connection: source
          path: customers_updated.csv
          format: csv

        pattern:
          type: dimension
          params:
            natural_key: customer_id
            surrogate_key: customer_sk
            scd_type: 2
            track_cols:
              - name
              - email
              - region
              - city
              - state
            target: warehouse.dim_customer
            valid_from_col: valid_from
            valid_to_col: valid_to
            is_current_col: is_current
            unknown_member: true
            audit:
              load_timestamp: true
              source_system: "crm"

        write:
          connection: warehouse
          path: dim_customer
          format: parquet
          mode: overwrite
```

### Output: Full History (16 rows)

| customer_sk | customer_id | name | email | region | valid_from | valid_to | is_current |
|-------------|-------------|------|-------|--------|------------|----------|------------|
| 0 | -1 | Unknown | Unknown | Unknown | 1900-01-01 | NULL | true |
| 1 | C001 | Alice Johnson | alice@example.com | North | 2024-01-15 | 2024-01-20 | **false** |
| 2 | C002 | Bob Smith | bob@example.com | South | 2024-01-15 | NULL | true |
| 3 | C003 | Carol White | carol@example.com | North | 2024-01-15 | NULL | true |
| 4 | C004 | David Brown | david@example.com | East | 2024-01-15 | 2024-01-20 | **false** |
| 5 | C005 | Emma Davis | emma@example.com | West | 2024-01-15 | NULL | true |
| 6 | C006 | Frank Miller | frank@example.com | South | 2024-01-15 | NULL | true |
| 7 | C007 | Grace Lee | grace@example.com | East | 2024-01-15 | 2024-01-20 | **false** |
| 8 | C008 | Henry Wilson | henry@example.com | West | 2024-01-15 | NULL | true |
| 9 | C009 | Ivy Chen | ivy@example.com | North | 2024-01-15 | NULL | true |
| 10 | C010 | Jack Taylor | jack@example.com | South | 2024-01-15 | NULL | true |
| 11 | C011 | Karen Martinez | karen@example.com | East | 2024-01-15 | NULL | true |
| 12 | C012 | Leo Anderson | leo@example.com | West | 2024-01-15 | NULL | true |
| **13** | C001 | Alice Johnson | **alice.johnson@newmail.com** | North | 2024-01-20 | NULL | **true** |
| **14** | C004 | David Brown | **david.b@corporate.com** | East | 2024-01-20 | NULL | **true** |
| **15** | C007 | Grace Lee | **grace.lee@gmail.com** | East | 2024-01-20 | NULL | **true** |

**Key observations:**
- **New surrogate keys** for new versions (13, 14, 15)
- **Old versions marked closed** (is_current = false, valid_to = 2024-01-20)
- **New versions marked current** (is_current = true, valid_to = NULL)
- **Full history preserved** - we can query data as of any point in time

### How to Query SCD2

**Current view (most common):**
```sql
SELECT * FROM dim_customer WHERE is_current = true;
```

**Point-in-time query (as of January 17):**
```sql
SELECT * FROM dim_customer
WHERE '2024-01-17' >= valid_from
  AND ('2024-01-17' < valid_to OR valid_to IS NULL);
```

**Customer C001's email history:**
```sql
SELECT customer_sk, email, valid_from, valid_to
FROM dim_customer
WHERE customer_id = 'C001'
ORDER BY valid_from;
```

| customer_sk | email | valid_from | valid_to |
|-------------|-------|------------|----------|
| 1 | alice@example.com | 2024-01-15 | 2024-01-20 |
| 13 | alice.johnson@newmail.com | 2024-01-20 | NULL |

---

## Understanding the Unknown Member

The unknown member row (customer_sk = 0) is automatically created when `unknown_member: true`:

| customer_sk | customer_id | name | email | all other columns |
|-------------|-------------|------|-------|-------------------|
| 0 | -1 | Unknown | Unknown | Unknown |

**Why it matters:**

When building fact tables, orders might reference a customer_id that doesn't exist in the dimension (data quality issue, late-arriving data, etc.). Instead of:
- Failing the pipeline (strict but inflexible)
- Losing the order data (dangerous)

We assign those orphan records to customer_sk = 0. This:
- Preserves all fact data
- Makes orphans easily identifiable
- Allows later cleanup/investigation

---

## Complete Runnable Example

Here's a complete YAML file you can run:

```yaml
# File: odibi_dimension_tutorial.yaml
project: dimension_tutorial
engine: pandas

connections:
  source:
    type: local
    path: ./examples/tutorials/dimensional_modeling/data
  warehouse:
    type: local
    path: ./warehouse

story:
  connection: warehouse
  path: stories

pipelines:
  # Initial load with SCD2
  - pipeline: initial_load
    description: "First load of customer dimension"
    nodes:
      - name: dim_customer
        description: "Customer dimension with SCD2 history tracking"
        read:
          connection: source
          path: customers.csv
          format: csv

        pattern:
          type: dimension
          params:
            natural_key: customer_id
            surrogate_key: customer_sk
            scd_type: 2
            track_cols:
              - name
              - email
              - region
              - city
              - state
            target: warehouse.dim_customer
            valid_from_col: valid_from
            valid_to_col: valid_to
            is_current_col: is_current
            unknown_member: true
            audit:
              load_timestamp: true
              source_system: "crm"

        write:
          connection: warehouse
          path: dim_customer
          format: parquet
          mode: overwrite

  # Incremental update with changes
  - pipeline: incremental_update
    description: "Process updates to customer dimension"
    nodes:
      - name: dim_customer
        description: "Update customer dimension with new email addresses"
        read:
          connection: source
          path: customers_updated.csv
          format: csv

        pattern:
          type: dimension
          params:
            natural_key: customer_id
            surrogate_key: customer_sk
            scd_type: 2
            track_cols:
              - name
              - email
              - region
              - city
              - state
            target: warehouse.dim_customer
            valid_from_col: valid_from
            valid_to_col: valid_to
            is_current_col: is_current
            unknown_member: true
            audit:
              load_timestamp: true
              source_system: "crm"

        write:
          connection: warehouse
          path: dim_customer
          format: parquet
          mode: overwrite
```

---

## Python API Alternative

If you prefer Python over YAML:

```python
from odibi.patterns.dimension import DimensionPattern
from odibi.context import EngineContext
from odibi.enums import EngineType
import pandas as pd

# Load source data
source_df = pd.read_csv("examples/tutorials/dimensional_modeling/data/customers.csv")

# Create pattern
pattern = DimensionPattern(params={
    "natural_key": "customer_id",
    "surrogate_key": "customer_sk",
    "scd_type": 2,
    "track_cols": ["name", "email", "region", "city", "state"],
    "unknown_member": True,
    "audit": {
        "load_timestamp": True,
        "source_system": "crm"
    }
})

# Validate configuration
pattern.validate()

# Execute pattern
context = EngineContext(df=source_df, engine_type=EngineType.PANDAS)
result_df = pattern.execute(context)

# View results
print(f"Generated {len(result_df)} dimension rows")
print(result_df.head(15))
```

---

## What You Learned

In this tutorial, you learned:

- **SCD Type 0** creates surrogate keys but never updates existing records
- **SCD Type 1** updates records in place, losing history but keeping current data
- **SCD Type 2** creates new rows for changes, preserving full history with valid_from/valid_to dates
- **Surrogate keys** are auto-generated integers, sequential starting from 1
- **Unknown member** (SK=0) provides a default for orphan FK handling
- **track_cols** defines which columns trigger a new version in SCD1/SCD2
- **Audit columns** (load_timestamp, source_system) track when/where data came from

---

## Next Steps

Now that you can build customer dimensions, let's create a date dimension that's automatically generated.

**Next:** [Date Dimension Pattern Tutorial](./03_date_dimension_pattern.md)

---

## Navigation

| Previous | Up | Next |
|----------|----|----|
| [Introduction](./01_introduction.md) | [Tutorials](../getting_started.md) | [Date Dimension](./03_date_dimension_pattern.md) |

---

## Reference

For complete parameter documentation, see: [Dimension Pattern Reference](../../patterns/dimension.md)
