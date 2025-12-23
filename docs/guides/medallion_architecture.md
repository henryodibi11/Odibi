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
│   "Just land it"      "Fix it"         "Make it              │
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

### What Happens Here

#### 1. Data Cleaning
Fixing problems in the source data.

| Operation | Example | Category |
|-----------|---------|----------|
| Deduplication | `ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC)` | Cleaning |
| Remove bad characters | `REPLACE(name, '"', '')` | Cleaning |
| Fix typos | `REPLACE(status, 'Iprocess', 'Inprocess')` | Cleaning |
| Handle nulls | `COALESCE(middle_name, '')` | Cleaning |
| Trim whitespace | `TRIM(customer_name)` | Cleaning |

```sql
-- Example: Cleaning product codes
CASE
    WHEN LEFT(REPLACE(Product_Code, '"', ''), 1) = 'N'
    THEN SUBSTRING(REPLACE(Product_Code, '"', ''), 2)
    ELSE REPLACE(Product_Code, '"', '')
END AS Product_Code
```

#### 2. Type Casting & Standardization
Making data types consistent.

| Operation | Example | Category |
|-----------|---------|----------|
| Cast types | `CAST(date_string AS DATE)` | Standardization |
| Parse timestamps | `to_timestamp(Date)` | Standardization |
| Unit conversion | `HOURS * 60 AS Duration_Min` | Standardization |
| Standardize casing | `UPPER(country_code)` | Standardization |

```sql
-- Example: Standardizing dates
to_timestamp(Date) AS Date,
DATEDIFF(to_date(Date), to_date('2021-01-01')) + 1 AS DateID
```

#### 3. Conforming to Standard Schema
Mapping source-specific values to enterprise-standard values.

| Operation | Example | Category |
|-----------|---------|----------|
| Code mapping | `'DRYER 1' → 'Flash Dryer 1'` | Conforming |
| Category standardization | `'Schedul%' → 'Market Related'` | Conforming |
| Rename columns | `SHIFT AS ShiftID` | Conforming |
| Add source context | `'Indianapolis' AS Plant_Name` | Conforming |

```sql
-- Example: Mapping source codes to standard names
CASE
    WHEN DRYER = 'DRYER 1' THEN 'Flash Dryer 1'
    WHEN DRYER = 'DRYER 2' THEN 'Flash Dryer 2'
    WHEN DRYER = 'DRYER 3' THEN 'Flash Dryer 3'
END AS Channel,

CASE
    WHEN CATEGORY LIKE '%Schedul%' THEN 'Market Related'
    WHEN CATEGORY LIKE '%Maintenance%' THEN 'Internal Scheduled - Maintenance'
    WHEN CATEGORY LIKE '%Mechanical%' THEN 'Internal Unscheduled - Maintenance'
    ELSE 'Internal Unscheduled - Production'
END AS Category
```

#### 4. Enrichment via Lookups
Adding dimension attributes from reference tables.

| Operation | Example | Category |
|-----------|---------|----------|
| Join to calendar | Get `DateID` from date dimension | Enrichment |
| Join to plant/process | Get `P_ID` from plant dimension | Enrichment |
| Join to reason codes | Get `LID` from reason lookup | Enrichment |
| Join to product master | Get `Product_Name` from product dim | Enrichment |

```sql
-- Example: Enriching with dimension lookups
SELECT
    s1.DateID,
    s1.Channel,
    s1.Shutdown_Duration_Min,
    lid.LID AS ReasonID,       -- From reason code lookup
    pp.P_ID                     -- From plant/process dimension
FROM step1 s1
LEFT JOIN cleaned_Indy_TD_Downtime_Reasons lid
    ON lid.Category = s1.Category 
LEFT JOIN cleaned_vw_dim_plantprocess pp
    ON s1.Channel = pp.Channel
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
❌ Business calculations (like OEE %)  
❌ Aggregations for reporting  
❌ Creating facts that span multiple sources

### Example Silver Node
```yaml
- name: cleaned_indy_downtime
  inputs:
    input_name: $bronze.indy_production_tblDryerDowntime
  depends_on:
    - cleaned_Indy_TD_Downtime_Reasons  # Lookup table
    - cleaned_vw_dim_plantprocess        # Dimension table
  transformer: deduplicate
  params:
    keys: [DryerDowntimeID]
    order_by: "_extracted_at DESC"
  transform:
    steps:
      - sql: |
          SELECT
              to_timestamp(Date) AS Date,
              DATEDIFF(to_date(Date), '2021-01-01') + 1 AS DateID,
              'Indianapolis' AS Plant_Name,
              CASE WHEN DRYER = 'DRYER 1' THEN 'Flash Dryer 1' ... END AS Channel,
              HOURS * 60 AS Shutdown_Duration_Min
          FROM df
      - function: detect_deletes
        params:
          mode: sql_compare
          keys: [DryerDowntimeID]
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
| Union facts | Combine downtime from Aspen + Cardinal + Indy | Combining |
| Reconciliation | UNION (not UNION ALL) to dedupe across sources | Combining |
| Cross-system dedup | Same event recorded in multiple systems | Combining |

```sql
-- Example: Combining downtime from all sources
SELECT DateID, P_ID, Shutdown_Duration_Min, Notes
FROM cleaned_aspen_downtime

UNION ALL

SELECT DateID, P_ID, Shutdown_Duration_Min, Notes
FROM cleaned_cardinal_downtime

UNION ALL

SELECT DateID, P_ID, Shutdown_Duration_Min, Notes
FROM cleaned_indy_downtime
```

#### 2. Business Calculations
Applying business definitions and formulas.

| Operation | Example | Category |
|-----------|---------|----------|
| Define metrics | `Production Total = COALESCE(Revised_Pounds, Pounds)` | Business Rule |
| Calculate KPIs | `OEE = Availability × Performance × Quality` | Business Rule |
| Apply business logic | "If negative, treat as zero" | Business Rule |
| Default values | "Use 67 if ReasonID is null and duration < 10 min" | Business Rule |

```sql
-- Example: Business definition of Production Total
COALESCE(
    CASE
        WHEN COALESCE(Revised_Pounds, Pounds) <= 0 THEN 0
        ELSE COALESCE(Revised_Pounds, Pounds)
    END, 
0) AS `Production Total`
```

This is Gold because it answers: *"What does 'production' MEAN to the business?"*

#### 3. Cross-Fact Joins
Joining multiple fact tables together.

| Operation | Example | Category |
|-----------|---------|----------|
| Join facts | Production + Downtime + Quality → OEE | Cross-Fact |
| Build wide tables | Denormalized reporting tables | Cross-Fact |
| Calculate ratios | Downtime / Available Hours | Cross-Fact |

```sql
-- Example: Joining facts for OEE calculation
SELECT 
    c.DateId,
    c.P_ID,
    p.`Production Total`,
    d.Shutdown_Duration_Min,
    q.BadProduct,
    -- OEE calculation uses all three facts
    (p.`Production Total` / p.`Nominal Capacity`) * 100 AS Performance_Pct
FROM calendar_with_pids c
LEFT JOIN combined_production p ON c.DateId = p.DateId AND c.P_ID = p.P_ID
LEFT JOIN combined_downtime d ON c.DateId = d.DateId AND c.P_ID = d.P_ID
LEFT JOIN combined_quality q ON c.DateId = q.DateId AND c.P_ID = q.P_ID
```

#### 4. Aggregations for Reporting
Pre-computing summaries for dashboards and reports.

| Operation | Example | Category |
|-----------|---------|----------|
| Daily rollups | SUM(production) GROUP BY date, plant | Aggregation |
| Weekly summaries | AVG(oee) by week | Aggregation |
| YTD calculations | Running totals | Aggregation |

#### 5. Derived Dimensions
Creating dimensions that don't exist in source systems.

| Operation | Example | Category |
|-----------|---------|----------|
| Date spine | Calendar × P_IDs for all combinations | Derived Dim |
| Distinct lists | All P_IDs with any activity | Derived Dim |

```sql
-- Example: Create all Date × P_ID combinations
SELECT *
FROM calendar_dimension
CROSS JOIN distinct_pids
```

### Example Gold Node
```yaml
- name: fact_oee_daily
  description: "Daily OEE metrics by plant and process"
  depends_on:
    - combined_production   # Silver → Gold: multiple sources unioned
    - combined_downtime     # Silver → Gold: multiple sources unioned
    - combined_quality      # Silver → Gold: multiple sources unioned
    - calendar_with_pids    # Scaffold dimension
  transform:
    steps:
      - sql: |
          SELECT
              c.DateId,
              c.P_ID,
              COALESCE(p.`Production Total`, 0) AS Production_Lbs,
              COALESCE(d.Shutdown_Duration_Min, 0) AS Downtime_Min,
              COALESCE(q.BadProduct, 0) AS Quality_Loss_Lbs,
              -- OEE Calculation (Business Formula)
              CASE 
                  WHEN p.`Nominal Capacity` > 0 
                  THEN (p.`Production Total` / p.`Nominal Capacity`) * 100
                  ELSE 0 
              END AS Performance_Pct
          FROM calendar_with_pids c
          LEFT JOIN combined_production p 
              ON c.DateId = p.DateId AND c.P_ID = p.P_ID
          LEFT JOIN combined_downtime d 
              ON c.DateId = d.DateId AND c.P_ID = d.P_ID
          LEFT JOIN combined_quality q 
              ON c.DateId = q.DateId AND c.P_ID = q.P_ID
  write:
    connection: gold
    table: fact_oee_daily
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
| "How do I look up P_ID from a dimension table?" | Silver |
| "How do I combine Aspen + Cardinal + Indy data?" | Gold |
| "What does 'Production Total' mean to the business?" | Gold |
| "How do I calculate OEE?" | Gold |
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
│    │   • Cleaning Aspen data                               │
│    │   • Joining Indy data to calendar dimension           │
│    │   • Mapping Cardinal codes to standard categories     │
│    │                                                        │
│   NO ───────────────────────────────► GOLD                  │
│    │                                                        │
│    │   Examples:                                            │
│    │   • Combining all downtime sources                    │
│    │   • Calculating OEE from production + downtime        │
│    │   • Creating unified fact tables                      │
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
              (Production / Capacity) * 100 AS OEE_Performance  -- Business formula!
          FROM df
```

**Why it's wrong:** OEE is a business definition. Silver should just clean the data.

**Fix:** Move the OEE calculation to Gold.

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
- name: cleaned_all_downtime
  depends_on:
    - cleaned_aspen_downtime
    - cleaned_cardinal_downtime
  transform:
    steps:
      - sql: |
          SELECT * FROM cleaned_aspen_downtime
          UNION ALL
          SELECT * FROM cleaned_cardinal_downtime  -- Combining sources!
```

**Why it's wrong:** Silver should process one source at a time.

**Fix:** Move the UNION to a Gold layer node.

---

## Project Structure Example

```
pipelines/
├── bronze/
│   └── bronze.yaml
│       # Nodes: bronze_aspen_oee, bronze_cardinal_wiki, bronze_indy_downtime
│
├── silver/
│   └── silver.yaml
│       # Nodes: cleaned_aspen_oee, cleaned_cardinal_wiki, cleaned_indy_downtime
│       # Each cleans ONE source
│
└── gold/
    └── gold.yaml
        # Nodes: combined_downtime, combined_production, fact_oee_daily
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
