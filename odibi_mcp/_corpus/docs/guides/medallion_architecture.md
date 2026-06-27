# Medallion Architecture Guide

A beginner-friendly guide to understanding Bronze, Silver, and Gold data layers.

---

## What is Medallion Architecture?

Medallion Architecture organizes your data into three layers, like refining raw ore into polished gold:

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│   Source Systems          Bronze           Silver           Gold   │
│   ──────────────          ──────           ──────           ────   │
│                                                                     │
│   [SQL Server] ──────►  [Raw Copy]  ───►  [Cleaned]  ───►  [Facts] │
│   [API]        ──────►  [Raw Copy]  ───►  [Cleaned]  ───►  [Dims]  │
│   [Files]      ──────►  [Raw Copy]  ───►  [Cleaned]  ───►  [KPIs]  │
│                                                                     │
│   "Just land it"      "Fix it"         "Make it                    │
│                                          business-ready"           │
└─────────────────────────────────────────────────────────────────────┘
```

**Think of it like cooking:**
- **Bronze** = Raw ingredients from the store (as-is, untouched)
- **Silver** = Ingredients washed, chopped, and prepped (cleaned, standardized)
- **Gold** = The finished dish ready to serve (combined, calculated, business-ready)

---

## Layer 1: Bronze (Raw Data)

### Purpose
Land data exactly as it comes from the source. No transformations. Just copy it.

### What Happens Here
| Operation | Example | Why |
|-----------|---------|-----|
| Raw ingestion | Copy SQL table to Delta | Preserve original data |
| Add metadata | `_extracted_at` timestamp | Track when data arrived |
| Schema preservation | Keep all columns, even unused | Don't lose anything |

### What Does NOT Happen Here
❌ No cleaning  
❌ No transformations  
❌ No joins  
❌ No filtering (except maybe date ranges for incremental loads)

### Example Bronze Node
```yaml
- name: bronze_sales_orders
  read:
    connection: erp_database
    table: dbo.SalesOrders
  write:
    connection: datalake
    path: bronze/sales_orders
    format: delta
```

### The Golden Rule of Bronze
> "If the source system has garbage data, Bronze has garbage data. That's okay."

---

## Layer 2: Silver (Cleaned & Conformed)

### Purpose
Clean and standardize **ONE source** at a time. Make it trustworthy.

### The Key Question
> "Could this node run if only ONE source system existed?"
> 
> If YES → Silver ✓  
> If NO → Probably Gold

!!! note "Reference Tables Are Allowed in Silver"
    The One-Source Test refers to **business source systems**, not reference/lookup data.
    
    **Silver CAN join with:**
    
    - Reference/lookup tables (code mappings, static lists)
    - Dimension lookups for enrichment (product_code → product_name)
    - Self-joins within the same source
    
    **Silver should NOT join:**
    
    - Multiple business source systems (SAP + Salesforce → use Gold)

### What Happens Here

#### 1. Data Cleaning
Fixing problems in the source data.

| Operation | Example | Category |
|-----------|---------|----------|
| Deduplication | `ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC)` | Cleaning |
| Remove bad characters | `REPLACE(name, '"', '')` | Cleaning |
| Fix typos | `REPLACE(status, 'Actve', 'Active')` | Cleaning |
| Handle nulls | `COALESCE(middle_name, '')` | Cleaning |
| Trim whitespace | `TRIM(customer_name)` | Cleaning |

```sql
-- Example: Cleaning product codes
CASE
    WHEN LEFT(REPLACE(product_code, '"', ''), 1) = 'X'
    THEN SUBSTRING(REPLACE(product_code, '"', ''), 2)
    ELSE REPLACE(product_code, '"', '')
END AS product_code
```

#### 2. Type Casting & Standardization
Making data types consistent.

| Operation | Example | Category |
|-----------|---------|----------|
| Cast types | `CAST(date_string AS DATE)` | Standardization |
| Parse timestamps | `to_timestamp(date_col)` | Standardization |
| Unit conversion | `hours * 60 AS duration_minutes` | Standardization |
| Standardize casing | `UPPER(country_code)` | Standardization |

```sql
-- Example: Standardizing dates
to_timestamp(order_date) AS order_date,
DATEDIFF(to_date(order_date), to_date('2020-01-01')) + 1 AS date_id
```

#### 3. Conforming to Standard Schema
Mapping source-specific values to enterprise-standard values.

| Operation | Example | Category |
|-----------|---------|----------|
| Code mapping | `'M1' → 'Machine 1'` | Conforming |
| Category standardization | `'Sched%' → 'Scheduled'` | Conforming |
| Rename columns | `cust_id AS customer_id` | Conforming |
| Add source context | `'West Region' AS region_name` | Conforming |

```sql
-- Example: Mapping source codes to standard names
CASE
    WHEN machine_code = 'M1' THEN 'Machine 1'
    WHEN machine_code = 'M2' THEN 'Machine 2'
    WHEN machine_code = 'M3' THEN 'Machine 3'
END AS machine_name,

CASE
    WHEN category LIKE '%Sched%' THEN 'Scheduled'
    WHEN category LIKE '%Maint%' THEN 'Maintenance'
    WHEN category LIKE '%Breakdown%' THEN 'Unplanned'
    ELSE 'Other'
END AS downtime_category
```

#### 4. Enrichment via Lookups
Adding dimension attributes from reference tables.

| Operation | Example | Category |
|-----------|---------|----------|
| Join to calendar | Get `date_id` from date dimension | Enrichment |
| Join to location | Get `location_id` from location dimension | Enrichment |
| Join to reason codes | Get `reason_id` from reason lookup | Enrichment |
| Join to product master | Get `product_name` from product dim | Enrichment |

```sql
-- Example: Enriching with dimension lookups
SELECT
    e.event_id,
    e.event_date,
    e.duration_minutes,
    r.reason_id,           -- From reason code lookup
    l.location_id          -- From location dimension
FROM events e
LEFT JOIN reason_codes r
    ON r.category = e.category 
LEFT JOIN dim_location l
    ON e.site_code = l.site_code
```

#### 5. Soft Delete Detection
Tracking records that exist in Bronze but no longer exist in the source.

| Operation | Example | Category |
|-----------|---------|----------|
| Compare snapshots | Find missing keys | Delete Detection |
| Flag deleted records | `_is_deleted = true` | Delete Detection |
| Filter active records | `WHERE _is_deleted = false` | Delete Detection |

### What Does NOT Happen Here
❌ Combining data from multiple source systems  
❌ Business calculations (like KPIs, ratios)  
❌ Aggregations for reporting  
❌ Creating facts that span multiple sources

### Example Silver Node
```yaml
- name: cleaned_warehouse_events
  inputs:
    input_name: $bronze.warehouse_event_log
  depends_on:
    - cleaned_reason_codes     # Lookup table
    - cleaned_dim_location     # Dimension table
  transformer: deduplicate
  params:
    keys: [event_id]
    order_by: "_extracted_at DESC"
  transform:
    steps:
      - sql: |
          SELECT
              to_timestamp(event_date) AS event_date,
              DATEDIFF(to_date(event_date), '2020-01-01') + 1 AS date_id,
              'Warehouse A' AS location_name,
              CASE WHEN machine = 'M1' THEN 'Machine 1' ... END AS machine_name,
              duration_hours * 60 AS duration_minutes
          FROM df
      - function: detect_deletes
        params:
          mode: sql_compare
          keys: [event_id]
```

### The Golden Rule of Silver
> "One source in, one cleaned version out. The output should be the **best possible version** of that single source."

---

## Layer 3: Gold (Business-Ready)

### Purpose
Combine cleaned Silver data into **business-meaningful** outputs.

### The Key Question
> "Does this require data from MULTIPLE Silver sources?"
> 
> If YES → Gold ✓  
> If NO → Probably Silver

### What Happens Here

#### 1. Combining Multiple Sources (UNION)
Merging the same type of data from different systems.

| Operation | Example | Category |
|-----------|---------|----------|
| Union facts | Combine events from System A + System B + System C | Combining |
| Reconciliation | UNION (not UNION ALL) to dedupe across sources | Combining |
| Cross-system dedup | Same event recorded in multiple systems | Combining |

```sql
-- Example: Combining events from all sources
SELECT date_id, location_id, duration_minutes, notes
FROM cleaned_system_a_events

UNION ALL

SELECT date_id, location_id, duration_minutes, notes
FROM cleaned_system_b_events

UNION ALL

SELECT date_id, location_id, duration_minutes, notes
FROM cleaned_system_c_events
```

#### 2. Business Calculations
Applying business definitions and formulas.

| Operation | Example | Category |
|-----------|---------|----------|
| Define metrics | `total_output = COALESCE(revised_qty, original_qty)` | Business Rule |
| Calculate KPIs | `efficiency = actual_output / expected_output * 100` | Business Rule |
| Apply business logic | "If negative, treat as zero" | Business Rule |
| Default values | "Use default reason if null and duration < 10 min" | Business Rule |

```sql
-- Example: Business definition of Total Output
COALESCE(
    CASE
        WHEN COALESCE(revised_quantity, original_quantity) <= 0 THEN 0
        ELSE COALESCE(revised_quantity, original_quantity)
    END, 
0) AS total_output
```

This is Gold because it answers: *"What does 'output' MEAN to the business?"*

#### 3. Cross-Fact Joins
Joining multiple fact tables together.

| Operation | Example | Category |
|-----------|---------|----------|
| Join facts | Production + Downtime + Quality → Efficiency | Cross-Fact |
| Build wide tables | Denormalized reporting tables | Cross-Fact |
| Calculate ratios | Downtime / Available Hours | Cross-Fact |

```sql
-- Example: Joining facts for efficiency calculation
SELECT 
    c.date_id,
    c.location_id,
    p.total_output,
    d.downtime_minutes,
    q.defect_count,
    -- Efficiency uses multiple facts
    (p.total_output / p.target_output) * 100 AS efficiency_pct
FROM calendar_scaffold c
LEFT JOIN combined_production p 
    ON c.date_id = p.date_id AND c.location_id = p.location_id
LEFT JOIN combined_downtime d 
    ON c.date_id = d.date_id AND c.location_id = d.location_id
LEFT JOIN combined_quality q 
    ON c.date_id = q.date_id AND c.location_id = q.location_id
```

#### 4. Aggregations for Reporting
Pre-computing summaries for dashboards and reports.

| Operation | Example | Category |
|-----------|---------|----------|
| Daily rollups | SUM(production) GROUP BY date, location | Aggregation |
| Weekly summaries | AVG(efficiency) by week | Aggregation |
| YTD calculations | Running totals | Aggregation |

#### 5. Derived Dimensions
Creating dimensions that don't exist in source systems.

| Operation | Example | Category |
|-----------|---------|----------|
| Date spine | Calendar × Locations for all combinations | Derived Dim |
| Distinct lists | All locations with any activity | Derived Dim |

```sql
-- Example: Create all Date × Location combinations
SELECT *
FROM dim_calendar
CROSS JOIN distinct_locations
```

### Example Gold Node
```yaml
- name: fact_daily_efficiency
  description: "Daily efficiency metrics by location"
  depends_on:
    - combined_production   # Multiple sources unioned
    - combined_downtime     # Multiple sources unioned
    - combined_quality      # Multiple sources unioned
    - calendar_scaffold     # Date × Location scaffold
  transform:
    steps:
      - sql: |
          SELECT
              c.date_id,
              c.location_id,
              COALESCE(p.total_output, 0) AS output_units,
              COALESCE(d.downtime_minutes, 0) AS downtime_min,
              COALESCE(q.defect_count, 0) AS defects,
              -- Efficiency Calculation (Business Formula)
              CASE 
                  WHEN p.target_output > 0 
                  THEN (p.total_output / p.target_output) * 100
                  ELSE 0 
              END AS efficiency_pct
          FROM calendar_scaffold c
          LEFT JOIN combined_production p 
              ON c.date_id = p.date_id AND c.location_id = p.location_id
          LEFT JOIN combined_downtime d 
              ON c.date_id = d.date_id AND c.location_id = d.location_id
          LEFT JOIN combined_quality q 
              ON c.date_id = q.date_id AND c.location_id = q.location_id
  write:
    connection: gold
    table: fact_daily_efficiency
    format: delta
```

### The Golden Rule of Gold
> "This is what the business sees. Every row and column should have business meaning."

---

## Quick Reference: Where Does This Belong?

### By Operation Type

| Operation | Bronze | Silver | Gold |
|-----------|:------:|:------:|:----:|
| Raw ingestion | ✓ | | |
| Add `_extracted_at` | ✓ | | |
| Deduplication | | ✓ | |
| Remove bad characters | | ✓ | |
| Fix typos | | ✓ | |
| Type casting | | ✓ | |
| Unit conversion | | ✓ | |
| Map codes to standard names | | ✓ | |
| Join to dimension/lookup tables | | ✓ | |
| Soft delete detection | | ✓ | |
| UNION multiple sources | | | ✓ |
| Business calculations | | | ✓ |
| Cross-fact joins | | | ✓ |
| Aggregations for reporting | | | ✓ |
| KPI definitions | | | ✓ |

### By Question

| Question | Layer |
|----------|-------|
| "How do I get data from the source?" | Bronze |
| "How do I fix this source's data quality issues?" | Silver |
| "How do I standardize this source to our schema?" | Silver |
| "How do I look up IDs from a dimension table?" | Silver |
| "How do I combine data from System A + B + C?" | Gold |
| "What does 'Total Output' mean to the business?" | Gold |
| "How do I calculate efficiency?" | Gold |
| "What should the dashboard show?" | Gold |

### The One-Source Test

```
┌─────────────────────────────────────────────────────────────┐
│                                                             │
│   "Could this node work with only ONE source system?"       │
│                                                             │
│   YES ──────────────────────────────► SILVER                │
│    │                                                        │
│    │   Examples:                                            │
│    │   • Cleaning System A data                             │
│    │   • Joining System B data to calendar dimension        │
│    │   • Mapping System C codes to standard categories      │
│    │                                                        │
│   NO ───────────────────────────────► GOLD                  │
│    │                                                        │
│    │   Examples:                                            │
│    │   • Combining all event sources                        │
│    │   • Calculating efficiency from production + downtime  │
│    │   • Creating unified fact tables                       │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## Common Mistakes

### ❌ Mistake 1: Business Logic in Silver
```yaml
# WRONG - Business calculation in Silver
- name: cleaned_production
  transform:
    steps:
      - sql: |
          SELECT 
              *,
              (actual / target) * 100 AS efficiency  -- Business formula!
          FROM df
```

**Why it's wrong:** Efficiency is a business definition. Silver should just clean the data.

**Fix:** Move the efficiency calculation to Gold.

### ❌ Mistake 2: Raw Data in Silver
```yaml
# WRONG - No Bronze layer, reading directly from source
- name: cleaned_orders
  read:
    connection: erp
    table: dbo.Orders  # Reading directly from source!
  transform:
    steps:
      - sql: SELECT * FROM df WHERE status != 'DELETED'
```

**Why it's wrong:** If the source changes, you lose the original data.

**Fix:** Add a Bronze layer that preserves the raw data first.

### ❌ Mistake 3: Combining Sources in Silver
```yaml
# WRONG - UNION in Silver
- name: cleaned_all_events
  depends_on:
    - cleaned_system_a_events
    - cleaned_system_b_events
  transform:
    steps:
      - sql: |
          SELECT * FROM cleaned_system_a_events
          UNION ALL
          SELECT * FROM cleaned_system_b_events  -- Combining sources!
```

**Why it's wrong:** Silver should process one source at a time.

**Fix:** Move the UNION to a Gold layer node.

---

## Project Structure Example

```
pipelines/
├── bronze/
│   └── bronze.yaml
│       # Nodes: bronze_system_a, bronze_system_b, bronze_system_c
│
├── silver/
│   └── silver.yaml
│       # Nodes: cleaned_system_a, cleaned_system_b, cleaned_system_c
│       # Each cleans ONE source
│
└── gold/
    └── gold.yaml
        # Nodes: combined_events, combined_production, fact_daily_efficiency
        # Combines Silver outputs, applies business logic
```

---

## Summary

| Layer | Input | Output | Key Activities |
|-------|-------|--------|----------------|
| **Bronze** | Source systems | Raw copy | Ingest, add metadata |
| **Silver** | Bronze (one source) | Cleaned version | Clean, standardize, enrich with lookups |
| **Gold** | Silver (multiple sources) | Business facts | Combine, calculate, aggregate |

**Remember:**
- **Bronze** = "Land it as-is"
- **Silver** = "Clean this ONE source"
- **Gold** = "Combine and calculate for business"
