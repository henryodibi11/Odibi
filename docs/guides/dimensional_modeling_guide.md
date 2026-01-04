# Dimensional Modeling Guide for ODIBI

A practical reference for building data warehouses with ODIBI.

---

## Table of Contents

1. [The Problem We're Solving](#the-problem-were-solving)
2. [Facts and Dimensions](#facts-and-dimensions)
3. [The Star Schema](#the-star-schema)
4. [Natural Keys vs Surrogate Keys](#natural-keys-vs-surrogate-keys)
5. [Bronze → Silver → Gold Flow](#bronze--silver--gold-flow)
6. [Where Do IDs Come From?](#where-do-ids-come-from)
7. [Lookup Tables vs Dimension Tables](#lookup-tables-vs-dimension-tables)
8. [Slowly Changing Dimensions (SCD)](#slowly-changing-dimensions-scd)
9. [The Date Dimension](#the-date-dimension)
10. [Aggregations](#aggregations)
11. [Common Mistakes to Avoid](#common-mistakes-to-avoid)
12. [ODIBI Current State vs Target](#odibi-current-state-vs-target)

---

## The Problem We're Solving

### Raw Data is Hard to Query

Your source systems store data for operations, not analysis:

```
orders.csv
| customer_email    | product_name | quantity | price | timestamp           |
|-------------------|--------------|----------|-------|---------------------|
| john@mail.com     | Latte        | 2        | 11.00 | 2024-01-15 09:15:00 |
```

To answer "What was revenue by product category for Q4 weekends?", you need:
- Product category (not in orders)
- Whether it's a weekend (calculated from timestamp)
- Q4 filter (calculated from timestamp)

**Dimensional modeling pre-calculates and organizes this context.**

---

## Facts and Dimensions

### Fact Table = What Happened

Events, transactions, measurements. Numbers you can add/count/average.

| Contains | Example |
|----------|---------|
| **Measures** (numbers) | `quantity`, `price`, `total_amount` |
| **Foreign keys** (pointers to dimensions) | `customer_sk`, `product_sk`, `date_sk` |
| **Degenerate dimensions** (IDs with no table) | `order_id`, `invoice_number` |

**Key insight:** Facts are usually **append-only**. Once it happened, it happened.

### Dimension Table = Context About What Happened

Who, what, when, where, why. Descriptive attributes.

| Contains | Example |
|----------|---------|
| **Primary key** | `customer_sk` |
| **Natural key** | `customer_id` (from source system) |
| **Descriptive attributes** | `name`, `email`, `city`, `segment` |
| **Hierarchies** | `city` → `state` → `country` |

**Key insight:** Dimensions **change over time**. A customer might move cities.

---

## The Star Schema

```
                    dim_customer
                         │
                         │ customer_sk
                         │
dim_product ─────── fact_orders ─────── dim_date
         product_sk      │        date_sk
                         │
                    dim_region
                         │
                    region_sk
```

**Why this shape?**

1. **Storage efficiency** — Store "John Smith, Premium, NYC" once, reference by ID
2. **Query speed** — Filter small dimension tables first, then join to facts
3. **Flexibility** — Add new attributes to dimensions, all reports get them
4. **Single source of truth** — Customer's segment defined in ONE place

---

## Natural Keys vs Surrogate Keys

| Type | What It Is | Example | Who Creates It |
|------|------------|---------|----------------|
| **Natural Key** | Business identifier | `customer_id`, `email`, `product_sku` | Source system or you (via hash) |
| **Surrogate Key** | Warehouse-generated integer | `customer_sk = 1001` | Data warehouse (auto-increment) |

### Why Use Surrogate Keys?

1. **Faster JOINs** — Integers are faster than strings
2. **SCD2 support** — One `customer_id` can have multiple `customer_sk` values (history)
3. **Survives source changes** — If source system changes IDs, yours don't
4. **Unknown member** — `customer_sk = 0` for orphan records

### The "Unknown" Member

What if an order references `customer_id = 999` but that customer doesn't exist?

**Solution:** Create a special row in every dimension:

| customer_sk | customer_id | name | city |
|-------------|-------------|------|------|
| **0** | **-1** | **Unknown** | **Unknown** |
| 1001 | 47 | John | NYC |

Orphan facts JOIN to `customer_sk = 0` — you never lose data.

---

## Bronze → Silver → Gold Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                           BRONZE                                     │
│   Raw data, as-is from source. Messy, duplicates, nulls, no IDs.    │
│   Example: raw_orders.csv, raw_customers.json                        │
└─────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                           SILVER                                     │
│   Cleaned, validated, deduplicated. NATURAL KEYS assigned.          │
│   Example: clean_orders, clean_customers                             │
│   Keys: customer_id (hash or from source), product_id, order_id     │
└─────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                            GOLD                                      │
│   Dimensional model. SURROGATE KEYS, SCD history, aggregates.       │
│   Example: dim_customer, dim_product, fact_orders, agg_daily_sales  │
│   Keys: customer_sk, product_sk, date_sk (integers)                 │
└─────────────────────────────────────────────────────────────────────┘
```

### Key Point: Silver Uses Natural Keys, Gold Uses Surrogate Keys

| Layer | Keys Used |
|-------|-----------|
| Bronze | Whatever source provides (emails, names, raw IDs) |
| Silver | **Natural keys** — business identifiers (customer_id, product_id) |
| Gold | **Surrogate keys** — warehouse-generated integers (customer_sk) |

---

## Where Do IDs Come From?

### Scenario A: Source System Has IDs (Common)

Your CRM already has `customer_id = 47`. Just pass it through.

```
Source → Bronze → Silver → Gold
  47       47       47      + customer_sk = 1001
```

### Scenario B: Source System Has NO IDs (Your Data)

You only have `email` and `product_name`. You must generate IDs.

**Option 1: Hash (Recommended)**

```sql
SELECT MD5(LOWER(TRIM(email))) as customer_id, email, name
FROM clean_customers
```

- Same email = same ID, forever
- Deterministic, stateless, simple
- Ugly IDs but who cares

**Option 2: Persistent Lookup Table**

- Store `email → customer_id` mapping
- On each run, only assign new IDs to new emails
- Nice sequential numbers (1, 2, 3...)
- More complex, requires state

**⚠️ NEVER use RANK() or ROW_NUMBER() alone — IDs will shift when data changes!**

---

## Lookup Tables vs Dimension Tables

**These are NOT the same thing.**

| Lookup Table | Dimension Table |
|--------------|-----------------|
| Silver layer | Gold layer |
| Maps `email → customer_id` | Has `customer_sk, customer_id, name, city, ...` |
| Just a helper for ID generation | Official, versioned, surrogate-keyed entity |
| No history | SCD2 history tracking |
| Simple key-value | Rich with attributes |

### The Flow

```
Bronze: raw_customers (email, name, city)
    │
    ▼
Silver: customer_lookup (email → customer_id via hash)
    │
    ▼
Silver: clean_customers (customer_id, email, name, city)
    │
    ▼
Gold: dim_customer (customer_SK, customer_id, name, city, valid_from, valid_to, is_current)
```

---

## Slowly Changing Dimensions (SCD)

What happens when a customer moves from NYC to LA?

### SCD Type 0: Keep Original

Never update. Always shows original value.

**Use case:** Birth date, original signup date

### SCD Type 1: Overwrite

Update in place. Lose history.

| customer_sk | customer_id | city |
|-------------|-------------|------|
| 1001 | 47 | LA | ← Was NYC, now LA, history lost

**Use case:** Correcting typos, current-state-only reporting

### SCD Type 2: Track History (Most Common)

Create new row, close old row.

| customer_sk | customer_id | city | valid_from | valid_to | is_current |
|-------------|-------------|------|------------|----------|------------|
| 1001 | 47 | NYC | 2020-01-01 | 2023-03-15 | **false** |
| 1002 | 47 | LA | 2023-03-15 | NULL | **true** |

**Use case:** Historical analysis, "What was their city when they ordered?"

### Looking Up Surrogate Keys for Facts

When building fact tables, filter by `is_current = true`:

```sql
SELECT 
  o.order_id,
  dc.customer_sk
FROM clean_orders o
LEFT JOIN dim_customer dc 
  ON o.customer_id = dc.customer_id 
  AND dc.is_current = true  -- Only match current version!
```

---

## The Date Dimension

Every business question involves time. Pre-calculate all date attributes.

| Column | Example | Why Useful |
|--------|---------|------------|
| date_sk | 20240115 | Primary key |
| full_date | 2024-01-15 | Actual date |
| day_of_week | Monday | Filter by weekday |
| is_weekend | false | Weekend analysis |
| month | 1 | Monthly aggregation |
| month_name | January | Display-friendly |
| quarter | 1 | Quarterly reporting |
| year | 2024 | Annual comparison |
| is_holiday | false | Holiday impact |

### Why Pre-Calculate?

**Without date dimension:**
```sql
-- Calculates for every row
WHERE DAYOFWEEK(order_date) IN (1, 7)
```

**With date dimension:**
```sql
-- Pre-calculated, indexed, fast
WHERE d.is_weekend = true
```

---

## Aggregations

### Why Pre-Aggregate?

**Without aggregates:**
```sql
-- Scans 1 BILLION rows every time
SELECT SUM(revenue) FROM fact_sales WHERE month = 'January'
```

**With aggregates:**
```sql
-- Scans 31 rows
SELECT SUM(daily_revenue) FROM agg_daily_sales WHERE month = 'January'
```

### The Grain Concept

**Grain = what one row represents**

| Table | Grain |
|-------|-------|
| fact_sales | One order line item |
| agg_daily_sales | All sales for a day + product + region |
| agg_monthly_sales | All sales for a month + product + region |

**Rule:** You can roll UP (daily → monthly) but not drill DOWN (monthly → daily) without the source.

---

## When to Use Natural Keys vs Surrogate Keys (Practical Decision)

The theory says "always use surrogate keys in gold." Reality is more nuanced.

### Use Natural Keys When:

- **Source system IDs are stable** — e.g., `P_ID` from opsvisdata, `LID` from reference tables
- **You're a solo DE and simplicity matters** — fewer moving parts = fewer bugs
- **No multi-source integration with conflicting IDs** — one source per entity
- **No need for unknown member (SK=0) handling** — your FKs always resolve

**Example:** OEE project uses `P_ID`, `LID`, `F_ID` as natural keys because they're system-generated and never change.

### Use Surrogate Keys When:

- **Natural keys might change or get recycled** — e.g., product codes reassigned
- **Joining on composite keys (3+ columns) is unwieldy** — `JOIN ON a=a AND b=b AND c=c AND d=d`
- **Integrating same entity from multiple sources** — same `customer_id` means different things in CRM vs ERP
- **Need unknown member rows for orphan FK handling** — SK=0 catches missing references

---

## When to Use `scd2` Function vs `DimensionPattern`

Both handle SCD Type 2, but they serve different purposes.

| Scenario | Use | Why |
|----------|-----|-----|
| Need custom SQL transforms before SCD | `scd2` function | Pattern doesn't support mid-pipeline SQL |
| All-in-one with surrogate keys + unknown member | `DimensionPattern` | Handles everything declaratively |
| Natural keys sufficient, no surrogates needed | `scd2` function | Lighter weight, less overhead |
| Building classic Kimball star schema from scratch | `DimensionPattern` | Full feature set |

### `scd2` Function Example (Custom SQL + SCD)

```yaml
transform:
  steps:
    - sql_file: "sql/cleaned_vw_dim_plantprocess.sql"  # Custom transform first
    - function: scd2
      params:
        connection: goat_prod
        path: "OEE/silver/cleaned_vw_dim_plantprocess"
        keys: [P_ID]
        track_cols: [PlantCode, Region, Site, Department]
        effective_time_col: _extracted_at
```

### `DimensionPattern` Example (All-in-One)

```yaml
pattern:
  type: dimension
  params:
    natural_key: customer_id
    surrogate_key: customer_sk
    scd_type: 2
    track_cols: [name, email, address]
    target: warehouse.dim_customer
    unknown_member: true
    audit:
      load_timestamp: true
      source_system: "crm"
```

---

## Common Mistakes to Avoid

### ❌ Using RANK() to Generate IDs

```sql
-- WRONG: IDs will shift when new data arrives!
SELECT DENSE_RANK() OVER (ORDER BY email) as customer_id
```

**Fix:** Use hash or persistent lookup table.

### ❌ Building Lookups from Bronze (Dirty Data)

```sql
-- WRONG: Bronze has duplicates, nulls, deleted records
SELECT DISTINCT email FROM raw_customers
```

**Fix:** Build lookups from Silver (cleaned data).

### ❌ Joining Fact to Dimension Without is_current Filter

```sql
-- WRONG: May get multiple matches (historical + current)
SELECT * FROM fact_orders f
JOIN dim_customer dc ON f.customer_id = dc.customer_id
```

**Fix:** Add `AND dc.is_current = true`.

### ❌ Confusing Lookup Tables with Dimension Tables

- Lookup = Silver, just maps keys
- Dimension = Gold, has surrogate keys + history + attributes

### ❌ Not Having an Unknown Member

If a fact references a customer that doesn't exist, the JOIN fails and you lose data.

**Fix:** Every dimension should have `SK = 0, name = 'Unknown'`.

---

## ODIBI Current State vs Target

### What ODIBI Has Now

| Pattern | What It Does | Limitation |
|---------|--------------|------------|
| `FactPattern` | Dedup + pass through | No SK lookup, no orphan handling |
| `SCD2Pattern` | Track history | No auto surrogate key |
| `MergePattern` | Upsert logic | — |
| `SnapshotPattern` | Point-in-time capture | — |
| `generate_surrogate_key` | Hash-based key | Not integrated into patterns |

### What We're Adding

| Pattern | What It Will Do |
|---------|-----------------|
| `DimensionPattern` | Auto SK + SCD + unknown member + audit columns |
| `DateDimensionPattern` | Generate complete date dimension |
| `Enhanced FactPattern` | Auto SK lookups + orphan handling + grain validation |
| `AggregationPattern` | Declarative GROUP BY + time rollups |

### Target: Declarative Dimensional Modeling

```yaml
# This should just work
- name: dim_customer
  pattern:
    type: dimension
    params:
      natural_key: customer_id
      surrogate_key: customer_sk
      scd_type: 2
      unknown_member: true

- name: fact_orders
  pattern:
    type: fact
    params:
      grain: [order_id]
      dimensions:
        - source_column: customer_id
          dimension_table: dim_customer
          surrogate_key: customer_sk
      orphan_handling: unknown
```

---

## Quick Reference: The Mental Model

```
┌─────────────────────────────────────────────────────────────────────┐
│                        BUSINESS QUESTION                             │
│   "What was revenue by product category for Q4 weekends?"           │
└─────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         GOLD LAYER                                   │
│   fact_orders ←→ dim_product ←→ dim_date                            │
│   Surrogate keys, SCD2 history, pre-aggregated                      │
└─────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        SILVER LAYER                                  │
│   clean_orders, clean_customers, customer_lookup                    │
│   Natural keys (hash or source), validated, deduplicated            │
└─────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        BRONZE LAYER                                  │
│   raw_orders.csv, raw_customers.json                                │
│   As-is from source, messy, no IDs if source doesn't have them     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Glossary

| Term | Definition |
|------|------------|
| **Fact** | Table of events/transactions with measures |
| **Dimension** | Table of context (who, what, when, where) |
| **Grain** | What one row represents |
| **Natural Key** | Business identifier (customer_id) |
| **Surrogate Key** | Warehouse-generated integer (customer_sk) |
| **SCD** | Slowly Changing Dimension |
| **Star Schema** | Fact in center, dimensions around it |
| **Lookup Table** | Helper table to map values to IDs |
| **Unknown Member** | Row with SK=0 for orphan handling |
| **Aggregate** | Pre-calculated summary table |
