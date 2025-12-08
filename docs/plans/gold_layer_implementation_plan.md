# ODIBI Gold Layer Enhancement Plan

## Executive Summary

Enhance ODIBI's Gold layer to support declarative dimensional modeling. Currently, users must write manual SQL for surrogate key lookups, aggregations, and FK validation. This plan adds patterns that make dimensional modeling as easy as Bronze/Silver.

---

## Current State vs Target State

| Capability | Current | Target |
|------------|---------|--------|
| Fact table with SK lookups | Manual SQL | `FactPattern` auto-lookups |
| Dimension with surrogate keys | Manual + SCD2 | `DimensionPattern` |
| Date dimension | Manual | `DateDimensionPattern` |
| Aggregations | Manual SQL | `AggregationPattern` |
| FK validation | None | Built into FactPattern |
| Semantic layer | Storage only | Execute + materialize |

---

## Implementation Phases

### Phase 1: Enhanced DimensionPattern

**Goal:** Single pattern to build a complete dimension table with surrogate keys and SCD2.

**Location:** `odibi/patterns/dimension.py`

**Features:**
1. Auto-generate surrogate key (sequential integer)
2. Support SCD Type 0, 1, 2 (configurable)
3. Add "Unknown" member automatically (SK = 0 or -1)
4. Add audit columns (load_timestamp, source_system)
5. Column aliasing for BI-friendly names

**Config Example:**
```yaml
pattern:
  type: dimension
  params:
    natural_key: customer_id           # Business key (from Silver)
    surrogate_key: customer_sk         # Generated (auto-increment)
    scd_type: 2                        # 0=static, 1=overwrite, 2=history
    track_columns: [name, email, city] # Columns to track for changes
    unknown_member: true               # Add row for orphan FKs
    audit:
      load_timestamp: true
      source_system: "crm"
```

**Implementation Tasks:**
- [ ] Create `DimensionPattern` class extending `Pattern`
- [ ] Implement surrogate key generation (max + row_number)
- [ ] Integrate with existing SCD2 logic (reuse `scd.py`)
- [ ] Add unknown member row insertion
- [ ] Add audit column injection
- [ ] Unit tests
- [ ] Documentation

---

### Phase 2: DateDimensionPattern

**Goal:** Generate a complete date dimension table with pre-calculated attributes.

**Location:** `odibi/patterns/date_dimension.py`

**Features:**
1. Generate date range (start_date to end_date)
2. Pre-calculate: day_of_week, is_weekend, month, quarter, year
3. Support fiscal calendar offset
4. Holiday integration (optional)

**Config Example:**
```yaml
pattern:
  type: date_dimension
  params:
    start_date: "2020-01-01"
    end_date: "2030-12-31"
    date_key_format: "yyyyMMdd"        # 20240115
    fiscal_year_start_month: 7          # July = FY start
    include_time: false                 # Date only, no time grain
```

**Generated Columns:**
- date_sk (INT, primary key)
- full_date (DATE)
- day_of_week (STRING: Monday, Tuesday...)
- day_of_week_num (INT: 1-7)
- is_weekend (BOOLEAN)
- week_of_year (INT)
- month (INT)
- month_name (STRING)
- quarter (INT: 1-4)
- quarter_name (STRING: Q1, Q2...)
- year (INT)
- fiscal_year (INT)
- fiscal_quarter (INT)
- is_month_start (BOOLEAN)
- is_month_end (BOOLEAN)

**Implementation Tasks:**
- [ ] Create `DateDimensionPattern` class
- [ ] Date range generator (Pandas date_range / Spark sequence)
- [ ] Attribute calculators
- [ ] Fiscal calendar logic
- [ ] Unit tests
- [ ] Documentation

---

### Phase 3: Enhanced FactPattern

**Goal:** Declarative fact table building with automatic SK lookups and validation.

**Location:** `odibi/patterns/fact.py` (enhance existing)

**Features:**
1. Declare dimension relationships
2. Auto-lookup surrogate keys from dimensions
3. Handle orphans (use unknown member or reject)
4. Validate grain (no duplicates at PK level)
5. Add audit columns
6. Enforce append-only mode

**Config Example:**
```yaml
pattern:
  type: fact
  params:
    grain: [order_id]                  # What makes a row unique
    
    dimensions:
      - source_column: customer_id     # Column in Silver data
        dimension_table: dim_customer  # Gold dimension to lookup
        dimension_key: customer_id     # Natural key in dimension
        surrogate_key: customer_sk     # SK to retrieve
        scd2: true                     # Filter is_current = true
        
      - source_column: product_id
        dimension_table: dim_product
        dimension_key: product_id
        surrogate_key: product_sk
        
      - source_column: order_date
        dimension_table: dim_date
        dimension_key: full_date
        surrogate_key: date_sk
    
    orphan_handling: unknown           # unknown | reject | quarantine
    
    measures:
      - quantity                       # Pass through
      - unit_price: price              # Rename
      - total_amount: "quantity * price"  # Calculated
    
    audit:
      load_timestamp: true
      source_system: "pos"
```

**Implementation Tasks:**
- [ ] Extend `FactPattern` class
- [ ] Dimension lookup logic (generate JOIN SQL)
- [ ] Orphan handling (COALESCE to unknown SK)
- [ ] Grain validation (check for duplicates)
- [ ] Measure calculation/renaming
- [ ] Audit column injection
- [ ] Unit tests
- [ ] Documentation

---

### Phase 4: AggregationPattern

**Goal:** Declarative aggregation with time-grain rollups.

**Location:** `odibi/patterns/aggregation.py`

**Features:**
1. Declare grain (GROUP BY columns)
2. Declare measures with aggregation functions
3. Incremental aggregation (merge new data)
4. Time rollups (daily → weekly → monthly)

**Config Example:**
```yaml
pattern:
  type: aggregation
  params:
    source: fact_orders
    grain: [date_sk, product_sk, region_sk]
    
    measures:
      - name: total_revenue
        expr: "SUM(total_amount)"
      - name: order_count
        expr: "COUNT(*)"
      - name: avg_order_value
        expr: "AVG(total_amount)"
      - name: unique_customers
        expr: "COUNT(DISTINCT customer_sk)"
    
    incremental:
      timestamp_column: date_sk
      merge_strategy: replace           # replace | sum
    
    rollups:                            # Optional: generate multiple grains
      - grain: [month, product_sk]
        output: agg_monthly_product
```

**Implementation Tasks:**
- [ ] Create `AggregationPattern` class
- [ ] SQL generation for GROUP BY + aggregates
- [ ] Incremental merge logic
- [ ] Rollup generation
- [ ] Unit tests
- [ ] Documentation

---

### Phase 5: Semantic Layer Execution

**Goal:** Execute metric definitions stored in meta_metrics.

**Location:** `odibi/semantics/` (new module)

**Features:**
1. Define metrics in YAML (not just SQL strings)
2. Query interface: "revenue BY region, month"
3. Materialize metrics on schedule
4. Serve via API (optional, future)

**Config Example (in odibi.yaml):**
```yaml
metrics:
  - name: revenue
    description: "Total revenue from completed orders"
    expr: "SUM(total_amount)"
    source: fact_orders
    filters:
      - "status = 'completed'"
    
  - name: avg_order_value
    description: "Average order value"
    expr: "revenue / COUNT(*)"
    type: derived

dimensions:
  - name: order_date
    source: dim_date
    hierarchy: [year, quarter, month, full_date]
    
  - name: product
    source: dim_product
    hierarchy: [category, subcategory, product_name]

materializations:
  - name: monthly_revenue_by_region
    metrics: [revenue, order_count]
    dimensions: [region, month]
    schedule: "0 2 1 * *"              # 2am on 1st of month
    output: gold/agg_monthly_revenue
```

**Implementation Tasks:**
- [ ] Create `odibi/semantics/` module
- [ ] Metric definition parser
- [ ] Query generator (metrics BY dimensions)
- [ ] Materialization executor
- [ ] CLI: `odibi query "revenue BY region"`
- [ ] Unit tests
- [ ] Documentation

---

### Phase 6: Relationship Registry & FK Validation

**Goal:** Declare and validate star schema relationships.

**Location:** `odibi/catalog.py` (extend) + `odibi/validation/fk.py` (new)

**Features:**
1. Declare relationships in YAML
2. Validate referential integrity on fact load
3. Detect orphan records
4. Generate lineage from relationships

**Config Example:**
```yaml
relationships:
  - name: orders_to_customers
    fact: fact_orders
    dimension: dim_customer
    fact_key: customer_sk
    dimension_key: customer_sk
    
  - name: orders_to_products
    fact: fact_orders
    dimension: dim_product
    fact_key: product_sk
    dimension_key: product_sk
```

**Implementation Tasks:**
- [ ] Relationship config schema
- [ ] FK validation logic
- [ ] Orphan detection reporting
- [ ] Integration with FactPattern
- [ ] Unit tests
- [ ] Documentation

---

## Implementation Order

```
Phase 1: DimensionPattern     ←── Start here (foundation)
    ↓
Phase 2: DateDimensionPattern ←── Quick win, very useful
    ↓
Phase 3: Enhanced FactPattern ←── Core value (auto SK lookups)
    ↓
Phase 4: AggregationPattern   ←── Performance optimization
    ↓
Phase 5: Semantic Layer       ←── Advanced (can defer)
    ↓
Phase 6: FK Validation        ←── Polish (can defer)
```

---

## File Structure

```
odibi/
├── patterns/
│   ├── __init__.py
│   ├── base.py              # Existing
│   ├── scd2.py              # Existing
│   ├── fact.py              # Enhance
│   ├── merge.py             # Existing
│   ├── snapshot.py          # Existing
│   ├── dimension.py         # NEW (Phase 1)
│   ├── date_dimension.py    # NEW (Phase 2)
│   └── aggregation.py       # NEW (Phase 4)
├── semantics/               # NEW (Phase 5)
│   ├── __init__.py
│   ├── metrics.py
│   ├── query.py
│   └── materialize.py
├── validation/
│   ├── __init__.py
│   ├── engine.py            # Existing
│   ├── gate.py              # Existing
│   └── fk.py                # NEW (Phase 6)
└── config.py                # Add new config schemas
```

---

## Testing Strategy

Each pattern needs:
1. Unit tests with mock data (Pandas engine)
2. Integration tests (if Spark available)
3. Example YAML configs in `examples/`
4. Documentation in `docs/`

---

## Success Criteria

After implementation, this YAML should work:

```yaml
pipelines:
  - pipeline: gold_dimensional
    nodes:
      # Date dimension (generated)
      - name: dim_date
        pattern:
          type: date_dimension
          params:
            start_date: "2020-01-01"
            end_date: "2030-12-31"
        write:
          connection: gold
          path: dim_date

      # Customer dimension (from Silver)
      - name: dim_customer
        depends_on: [clean_customers]
        pattern:
          type: dimension
          params:
            natural_key: customer_id
            surrogate_key: customer_sk
            scd_type: 2
            track_columns: [name, city]
            unknown_member: true
        write:
          connection: gold
          path: dim_customer

      # Fact table (auto SK lookups)
      - name: fact_orders
        depends_on: [clean_orders, dim_customer, dim_product, dim_date]
        pattern:
          type: fact
          params:
            grain: [order_id]
            dimensions:
              - source_column: customer_id
                dimension_table: dim_customer
                dimension_key: customer_id
                surrogate_key: customer_sk
                scd2: true
              - source_column: order_date
                dimension_table: dim_date
                dimension_key: full_date
                surrogate_key: date_sk
            orphan_handling: unknown
            measures:
              - quantity
              - total_amount: "quantity * price"
        write:
          connection: gold
          path: fact_orders
          mode: append

      # Daily aggregation
      - name: agg_daily_sales
        depends_on: [fact_orders]
        pattern:
          type: aggregation
          params:
            grain: [date_sk, product_sk]
            measures:
              - name: revenue
                expr: "SUM(total_amount)"
              - name: order_count
                expr: "COUNT(*)"
        write:
          connection: gold
          path: agg_daily_sales
```

---

## Timeline Estimate

| Phase | Effort | Priority |
|-------|--------|----------|
| Phase 1: DimensionPattern | 2-3 days | High |
| Phase 2: DateDimensionPattern | 1 day | High |
| Phase 3: Enhanced FactPattern | 3-4 days | High |
| Phase 4: AggregationPattern | 2 days | Medium |
| Phase 5: Semantic Layer | 3-5 days | Low (defer) |
| Phase 6: FK Validation | 1-2 days | Low (defer) |

**Total for Phases 1-4:** ~8-10 days
