# 6 Months of Data Engineering Content: A Complete Index

*Everything we covered, organized for reference*

---

## TL;DR

This article indexes all 26 articles in the series. Use it to find specific topics, revisit concepts, or share with colleagues. Every article is linked with a one-line summary.

---

## The Journey

Over 6 months, we built a complete data warehouse from scratch:

```
Week 1-2:   Credibility (who I am, why I built this)
Week 3-10:  Building (Bronze → Silver → Gold)
Week 11-16: Deep Dives (patterns, anti-patterns)
Week 17-22: Troubleshooting (debugging, performance)
Week 23-26: Advanced (semantic layer, production, reflections)
```

---

## Phase 1: Credibility (Weeks 1-2)

Building trust before building audience.

| # | Article | Summary |
|---|---------|---------|
| 1 | **How I Taught Myself Data Engineering While Working Night Shifts** | Origin story: 1000 hours of self-study, rotational program background |
| 2 | **The Bronze Layer Mistake That Cost Me 6 Months of Data** | Why Bronze should never transform data |
| 3 | **Setting Up a Bronze Layer with Delta Lake** | Complete Bronze config for 8 CSV files |

---

## Phase 2: Building in Public (Weeks 3-10)

The core technical series.

| # | Article | Summary |
|---|---------|---------|
| 4 | **Data Quality Contracts: Catching Problems Before Production** | Pre-conditions, severity levels, quarantine pattern |
| 5 | **Complete Silver Layer Configuration Guide** | Full configs for all 8 tables with dedup, validation |
| 6 | **Facts vs Dimensions: A Practical Guide** | Restaurant analogy, identifying facts and dimensions |
| 7 | **Building a Date Dimension from Scratch** | Fiscal calendars, unknown member, generated columns |
| 8 | **Fact Table Patterns: Lookups, Orphans, and Measures** | Surrogate key lookups, orphan handling, measure definitions |
| 9 | **From CSV to Star Schema: Complete Walkthrough** | End-to-end: 8 CSVs → Bronze → Silver → Gold |
| 10 | **Introducing Odibi: Declarative Data Pipelines** | Framework overview, core concepts, getting started |

---

## Phase 3: Pattern Deep Dives (Weeks 11-16)

Mastering the patterns.

| # | Article | Summary |
|---|---------|---------|
| 11 | **SCD2 Complete Guide: When and How to Track History** | When to use SCD2, configuration, common gotchas |
| 12 | **Dimension Table Patterns: Unknown Members, Keys, and Auditing** | Conformed dimensions, role-playing, junk dimensions |
| 13 | **Fact Table Design: Grain, Measures, and Validation** | Grain decisions, measure types, factless facts |
| 14 | **Pre-Aggregation Strategies for Fast Dashboards** | When to aggregate, incremental aggregation |
| 15 | **Data Quality Patterns: Contracts, Validation, and Quarantine** | Layer-by-layer strategy, metrics, testing |
| 16 | **Incremental Loading: Watermarks, Merge, and Append** | Three patterns, late-arriving data, state management |

---

## Phase 4: Anti-Patterns & Troubleshooting (Weeks 17-22)

Learning from mistakes.

| # | Article | Summary |
|---|---------|---------|
| 17 | **Bronze Layer Anti-Patterns: 3 Mistakes That Will Haunt You** | Transforming, overwriting, skipping metadata |
| 18 | **Silver Layer Best Practices: Centralize, Validate, Deduplicate** | SSOT, validation strategy, dedup patterns |
| 19 | **SCD2 Done Wrong: History Explosion and How to Prevent It** | Volatile columns, diagnosis, fixing exploded dimensions |
| 20 | **Performance Anti-Patterns: Why Your Pipeline Takes Hours** | Shuffles, skew, collect, partitioning |
| 21 | **Configuration Patterns for Multi-Environment Pipelines** | Variables, environment overrides, secret management |
| 22 | **Debugging Data Pipelines: A Systematic Approach** | 7-step process, log analysis, recovery |

---

## Phase 5: Advanced & Community (Weeks 23-26)

Taking it to production.

| # | Article | Summary |
|---|---------|---------|
| 23 | **Building a Semantic Layer: Metrics Everyone Can Trust** | Define metrics once, query everywhere |
| 24 | **Production Data Pipelines: Monitoring, Retry, and Testing** | Operational patterns for reliability |
| 25 | **What I Learned Building an Open Source Data Framework** | Lessons learned, advice for builders |
| 26 | **6 Months of Data Engineering Content: A Complete Index** | This article-the complete reference |

---

## Topic Index

Find articles by topic:

### Architecture

- Medallion architecture: Articles 2, 3, 5, 9
- Star schema: Articles 6, 7, 8, 9
- Layered approach: Articles 17, 18

### Bronze Layer

- Setup: Article 3
- Anti-patterns: Article 17
- Metadata: Articles 2, 3, 17

### Silver Layer

- Complete guide: Article 5
- Best practices: Article 18
- Validation: Articles 4, 5, 15

### Gold Layer

- Facts vs dimensions: Article 6
- Date dimension: Article 7
- Fact patterns: Articles 8, 13
- Aggregation: Article 14

### Dimensions

- Basics: Article 6
- Date dimension: Article 7
- SCD2: Articles 11, 19
- Patterns: Article 12

### Facts

- Basics: Article 6
- Patterns: Article 8
- Design: Article 13
- Factless facts: Article 13

### Data Quality

- Contracts: Articles 4, 15
- Validation: Articles 5, 15
- Quarantine: Articles 4, 15

### SCD2

- Complete guide: Article 11
- Anti-patterns: Article 19
- When NOT to use: Articles 11, 19

### Performance

- Aggregation: Article 14
- Anti-patterns: Article 20
- Incremental loading: Article 16

### Operations

- Configuration: Article 21
- Debugging: Article 22
- Production: Article 24
- Monitoring: Article 24

### Advanced

- Semantic layer: Article 23
- Multi-environment: Article 21

---

## Configuration Quick Reference

Common patterns you can copy:

### Bronze Node

```yaml
- name: bronze_orders
  read:
    connection: landing
    path: orders.csv
    format: csv
  transform:
    steps:
      - function: derive_columns
        params:
          columns:
            _extracted_at: "current_timestamp()"
            _source_file: "'orders.csv'"
  write:
    connection: bronze
    path: orders
    format: delta
    mode: append
```

### Silver Node with Contracts

```yaml
- name: silver_orders
  contracts:
    - type: not_null
      column: order_id
      severity: error
    - type: unique
      columns: [order_id]
      severity: error
  transformer: deduplicate
  params:
    keys: [order_id]
    order_by: _extracted_at DESC
  write:
    connection: silver
    path: orders
    format: delta
```

### Dimension with SCD2

```yaml
- name: dim_customer
  pattern:
    type: dimension
    params:
      natural_key: customer_id
      surrogate_key: customer_sk
      scd_type: 2
      track_cols: [customer_city, customer_state]
      target: gold.dim_customer
      unknown_member: true
```

### Fact with Lookups

```yaml
- name: fact_order_items
  pattern:
    type: fact
    params:
      grain: [order_id, order_item_id]
      dimensions:
        - source_column: customer_id
          dimension_table: dim_customer
          surrogate_key: customer_sk
      orphan_handling: unknown
      measures:
        - price
        - quantity
```

### Date Dimension

```yaml
- name: dim_date
  pattern:
    type: date_dimension
    params:
      start_date: "2020-01-01"
      end_date: "2030-12-31"
      fiscal_year_start_month: 7
      unknown_member: true
```

---

## Key Concepts

| Concept | Definition | Article |
|---------|------------|---------|
| Medallion Architecture | Bronze → Silver → Gold layering | 2, 3 |
| SCD2 | Slowly Changing Dimension Type 2 (history tracking) | 11 |
| Surrogate Key | Warehouse-generated integer for joins | 6, 8 |
| Natural Key | Business identifier from source | 6 |
| Grain | Level of detail in a fact table | 13 |
| Contracts | Pre-conditions on data quality | 4 |
| Quarantine | Routing bad data for review | 4, 15 |
| Unknown Member | Dimension row for orphan handling | 7, 12 |
| Conformed Dimension | Single dimension used across facts | 12 |
| Semantic Layer | Centralized metric definitions | 23 |

---

## The Brazilian E-Commerce Dataset

Throughout the series, we used the [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce):

| Table | Rows | Purpose |
|-------|------|---------|
| orders | 99,441 | Order headers |
| order_items | 112,650 | Line items |
| customers | 99,441 | Customer info |
| products | 32,951 | Product catalog |
| sellers | 3,095 | Seller info |
| payments | 103,886 | Payment details |
| reviews | 100,000 | Customer reviews |
| geolocation | 1,000,163 | Zip code data |

---

## What's Next?

This series covered the fundamentals. Future content might explore:

- **Streaming**: Real-time data pipelines
- **ML Integration**: Feature stores and model serving
- **Data Mesh**: Decentralized data ownership
- **Advanced Spark**: Optimization deep dives
- **Cloud Patterns**: AWS, Azure, GCP specifics

Follow for updates.

---

## Thank You

26 articles. 6 months. One complete data engineering education.

Thank you for reading, sharing, and building with me.

---

## Connect

- **LinkedIn:** [Your LinkedIn URL]
- **GitHub:** [Odibi Repository URL]
- **Medium:** Follow for future content

---

*This completes the 6-month series. See you in the next one.*
