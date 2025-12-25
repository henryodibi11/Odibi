# Odibi Glossary

A beginner-friendly guide to every data engineering term you'll encounter in Odibi.

---

## A

### Aggregation

**What it is:** Combining many rows of data into summary numbers—like counting, averaging, or totaling.

**Real-world analogy:** Imagine counting votes in an election. You don't care about each individual ballot; you just want the total for each candidate. That's aggregation.

**Example:**
```yaml
pattern: aggregation
aggregations:
  - column: sales_amount
    function: sum
    alias: total_sales
  - column: order_id
    function: count
    alias: order_count
group_by:
  - store_id
  - sale_date
```

**Why it matters:** Raw data has millions of rows. Business users need summaries like "total sales by store" or "average order value by month." Aggregation turns overwhelming detail into actionable insights.

**Learn more:** [Aggregation Pattern](../patterns/aggregation.md)

---

### Append

**What it is:** Adding new rows to a table without touching the rows that already exist.

**Real-world analogy:** Adding new entries to a guest book. You write on the next blank page—you don't erase or change what previous guests wrote.

**Example:**
```yaml
write_mode: append
```

**Why it matters:** When you receive daily sales data, you want to add today's transactions without accidentally deleting yesterday's. Append mode keeps your historical data safe.

**Learn more:** [Write Modes](../reference/yaml_schema.md)

---

## B

### Bronze Layer

**What it is:** The first storage layer where raw data lands exactly as it arrived—no cleaning, no changes.

**Real-world analogy:** A mailroom. Letters arrive and get sorted into bins, but nobody opens or edits them. They're stored exactly as received.

**Example:**
```yaml
layer: bronze
nodes:
  - name: raw_sales
    source: landing/sales_*.csv
    write_mode: append
    # No transformations - just store it raw
```

**Why it matters:** If something goes wrong later, you can always go back to the original data. Bronze is your "undo button" for the entire pipeline.

**Learn more:** [Medallion Architecture](#medallion-architecture)

---

## C

### Connection

**What it is:** Saved credentials and settings that tell Odibi how to access a data source (database, file storage, API).

**Real-world analogy:** A saved password in your browser. Instead of typing your username and password every time, you save it once and reuse it.

**Example:**
```yaml
connections:
  warehouse_db:
    type: postgres
    host: db.company.com
    port: 5432
    database: analytics
    # Credentials stored securely, not in YAML
```

**Why it matters:** Connections let you reuse access settings across many pipelines. Change the password once, and all pipelines using that connection keep working.

**Learn more:** [Connections Reference](../reference/connections.md)

---

## D

### DAG (Directed Acyclic Graph)

**What it is:** A map showing which pipeline steps must happen before others. "Directed" means arrows show order. "Acyclic" means no loops—you can't go in circles.

**Real-world analogy:** A recipe. You must chop vegetables before you can sauté them. You can't frost a cake before baking it. The steps have a required order.

**Example:**
```
[Load Sales] → [Clean Sales] → [Join with Products] → [Calculate Metrics]
                                       ↑
                              [Load Products]
```

**Why it matters:** Odibi uses the DAG to know what can run in parallel (Load Sales and Load Products) and what must wait (Join can't start until both loads finish).

**Learn more:** [Pipeline Concepts](../concepts/pipelines.md)

---

### Data Quality

**What it is:** Measuring whether your data is correct, complete, and trustworthy.

**Real-world analogy:** Quality control in a factory. Before products ship, inspectors check for defects. Data quality is the same—checking for missing values, wrong formats, or impossible numbers.

**Example:**
```yaml
validation:
  rules:
    - column: email
      rule: not_null
      severity: error
    - column: age
      rule: range
      min: 0
      max: 150
      severity: warning
    - column: order_total
      rule: positive
      severity: error
```

**Why it matters:** Bad data leads to bad decisions. If 20% of your sales records have missing amounts, your revenue reports are wrong. Data quality catches problems before they spread.

**Learn more:** [Validation Guide](../patterns/validation.md)

---

### Delta Lake

**What it is:** A smart file format that stores data in folders but adds superpowers: undo changes, time travel to past versions, and handle updates efficiently.

**Real-world analogy:** Google Docs version history. You can see every change ever made, go back to any previous version, and multiple people can edit without conflicts.

**Example:**
```yaml
format: delta
write_mode: merge
# Delta enables merge, time travel, and ACID transactions
```

**Why it matters:** Regular files (CSV, Parquet) can't handle updates well—you'd have to rewrite the entire file. Delta Lake lets you update just the rows that changed, and if something goes wrong, you can undo it.

**Learn more:** [Delta Lake Integration](../reference/delta.md)

---

### Dimension Table

**What it is:** A lookup table containing descriptive information about things—like products, customers, or locations.

**Real-world analogy:** A phone book or contact list. It doesn't record what calls you made (that's a fact table). It just stores information about people: name, address, phone number.

**Example:**
```yaml
pattern: dimension
table_type: scd2
natural_key:
  - customer_id
tracked_columns:
  - customer_name
  - email
  - address
  - loyalty_tier
```

**Why it matters:** Dimension tables give meaning to your facts. A sales record might say "customer_id: 12345 bought product_id: 789." The dimension tables tell you WHO customer 12345 is and WHAT product 789 is.

**Learn more:** [Dimension Pattern](../patterns/dimension.md)

---

## E

### Engine (Spark vs Pandas vs Polars)

**What it is:** The processing tool that actually does the data work. Different engines handle different data sizes.

**Real-world analogy:** 
- **Pandas** = Kitchen blender. Great for small batches, easy to use.
- **Polars** = Food processor. Faster than a blender, handles bigger jobs.
- **Spark** = Industrial food processing plant. Handles massive volumes across many machines.

**Example:**
```yaml
engine: spark  # For big data (millions+ rows)
# engine: pandas  # For small data (fits in memory)
# engine: polars  # For medium data (fast single-machine)
```

**Why it matters:** Using Spark for 100 rows is overkill (slow startup). Using Pandas for 100 million rows crashes your computer. Picking the right engine means your pipeline runs efficiently.

**Learn more:** [Engine Guide](../concepts/engines.md)

---

### ETL vs ELT

**What it is:** Two approaches to moving and transforming data.
- **ETL** (Extract, Transform, Load): Clean data BEFORE storing it.
- **ELT** (Extract, Load, Transform): Store raw data first, clean it AFTER.

**Real-world analogy:** 
- **ETL** = Sorting mail before putting it in your filing cabinet.
- **ELT** = Dumping all mail in a box, then sorting when you need something.

**Example:**
```yaml
# ELT approach (Odibi's default - medallion architecture)
# 1. Load raw to Bronze (Extract, Load)
# 2. Transform in Silver/Gold (Transform)

bronze_node:
  source: raw_file.csv
  write_mode: append  # Just load it

silver_node:
  source: bronze_table
  transformations:    # Transform after loading
    - type: clean_nulls
    - type: standardize_dates
```

**Why it matters:** ELT is more flexible because you keep the raw data. If business rules change, you can re-transform from Bronze. ETL might have thrown away data you now need.

**Learn more:** [Pipeline Architecture](../concepts/architecture.md)

---

## F

### Fact Table

**What it is:** A table storing events or transactions—things that happened at a point in time with measurable values.

**Real-world analogy:** Receipts. Each receipt records: when (timestamp), who (customer), what (products), and how much (amounts). That's a fact.

**Example:**
```yaml
pattern: fact
table_type: transaction
natural_key:
  - order_id
  - line_item_id
measures:
  - quantity
  - unit_price
  - discount_amount
  - line_total
foreign_keys:
  - column: customer_id
    references: dim_customer
  - column: product_id
    references: dim_product
```

**Why it matters:** Fact tables are where the numbers live. When someone asks "What were our total sales last quarter?", you're querying a fact table.

**Learn more:** [Fact Pattern](../patterns/fact.md)

---

### Foreign Key (FK)

**What it is:** A column that links one table to another by referencing the other table's unique identifier.

**Real-world analogy:** A reference on a job application. The application says "Reference: Jane Smith, phone: 555-1234." That phone number is a "foreign key" linking to a person who exists elsewhere.

**Example:**
```yaml
validation:
  foreign_key_checks:
    - column: customer_id
      reference_table: dim_customer
      reference_column: customer_id
      on_violation: quarantine  # Don't load orphan records
```

**Why it matters:** Foreign keys ensure data integrity. If an order references "customer_id: 99999" but that customer doesn't exist, something is wrong. FK validation catches these broken links.

**Learn more:** [FK Validation](../patterns/validation.md#foreign-key-checks)

---

## G

### Gold Layer

**What it is:** The final, business-ready layer with curated, aggregated, and report-ready data.

**Real-world analogy:** A finished meal, plated and ready to serve. The raw ingredients (Bronze) were cleaned (Silver) and now it's restaurant-quality (Gold).

**Example:**
```yaml
layer: gold
nodes:
  - name: monthly_sales_summary
    source: silver.fact_sales
    pattern: aggregation
    aggregations:
      - column: total_amount
        function: sum
        alias: monthly_revenue
    group_by:
      - year
      - month
      - region
```

**Why it matters:** Business users and dashboards consume Gold tables directly. These are optimized for fast queries and contain pre-calculated metrics so reports load instantly.

**Learn more:** [Medallion Architecture](#medallion-architecture)

---

## I

### Idempotent

**What it is:** An operation that gives the same result no matter how many times you run it.

**Real-world analogy:** Pressing an elevator button. Pressing it once calls the elevator. Pressing it 10 more times doesn't call 10 elevators—you get the same result.

**Example:**
```yaml
# Idempotent merge - safe to rerun
write_mode: merge
merge_keys:
  - order_id
# Running twice with same data = same result

# NOT idempotent - DON'T do this for reruns
write_mode: append
# Running twice = duplicate rows!
```

**Why it matters:** Pipelines fail and get retried. If your pipeline isn't idempotent, retrying it corrupts your data (duplicates, wrong totals). Idempotent pipelines are safe to rerun.

**Learn more:** [Write Modes](../reference/yaml_schema.md#write-modes)

---

### Incremental Load

**What it is:** Only processing data that's new or changed since the last run, instead of reprocessing everything.

**Real-world analogy:** Syncing photos to the cloud. Your phone doesn't upload all 10,000 photos every time—just the new ones since last sync.

**Example:**
```yaml
incremental:
  enabled: true
  watermark_column: updated_at
  lookback_period: 2 days
# Only process rows where updated_at > last_run_time - 2 days
```

**Why it matters:** Full reloads waste time and compute. If you have 5 years of data but only 1 day is new, why process all 5 years? Incremental loads are faster and cheaper.

**Learn more:** [Incremental Processing](../concepts/incremental.md)

---

## J

### Join

**What it is:** Combining rows from two or more tables based on matching values in a column.

**Real-world analogy:** Matching students to their grades. The student roster has names and IDs. The grade sheet has IDs and scores. A join combines them so you see "Name: Alice, Score: 95."

**Example:**
```yaml
transformations:
  - type: join
    right_source: dim_product
    join_type: left
    on:
      - left: product_id
        right: product_id
    select:
      - orders.*
      - dim_product.product_name
      - dim_product.category
```

**Why it matters:** Data lives in separate tables. Joins connect them. Without joins, you'd have order numbers but no customer names, product IDs but no descriptions.

**Learn more:** [Join Transformer](../transformers/join.md)

---

## M

### Medallion Architecture

**What it is:** A three-layer data organization pattern: Bronze (raw) → Silver (cleaned) → Gold (business-ready).

**Real-world analogy:** A water treatment plant:
- **Bronze** = Water from the lake (raw, unfiltered)
- **Silver** = Filtered and treated (clean but not packaged)
- **Gold** = Bottled water on store shelves (ready for consumers)

**Example:**
```yaml
# Bronze: Land raw data
bronze_orders:
  source: kafka/orders_topic
  layer: bronze
  write_mode: append

# Silver: Clean and validate
silver_orders:
  source: bronze_orders
  layer: silver
  validation:
    rules:
      - column: order_id
        rule: not_null

# Gold: Aggregate for reports
gold_daily_sales:
  source: silver_orders
  layer: gold
  pattern: aggregation
```

**Why it matters:** This structure makes debugging easy (check Bronze for raw data), ensures data quality (Silver validates), and provides fast analytics (Gold is optimized for queries).

**Learn more:** [Architecture Guide](../concepts/architecture.md)

---

### Merge (Upsert)

**What it is:** A smart write that inserts new rows and updates existing rows in one operation. "Upsert" = Update + Insert.

**Real-world analogy:** A contact list sync. New contacts get added. Existing contacts get their info updated (new phone number, new address). Nothing gets duplicated.

**Example:**
```yaml
write_mode: merge
merge_keys:
  - customer_id
# If customer_id exists → update the row
# If customer_id is new → insert new row
```

**Why it matters:** Without merge, you'd have to delete all matching rows, then insert—risky and slow. Merge handles both cases atomically, keeping your data consistent.

**Learn more:** [Merge Pattern](../patterns/merge.md)

---

## N

### Natural Key

**What it is:** A column (or columns) that uniquely identifies a row using real business data, not a generated number.

**Real-world analogy:** Your email address or Social Security Number—something from the real world that identifies you, not a made-up internal ID.

**Example:**
```yaml
natural_key:
  - employee_id      # HR system's real ID
  - effective_date   # For SCD2, identifies the version
# NOT a surrogate key (generated number)
```

**Why it matters:** Natural keys connect your data to the real world. When someone asks about "employee E12345," you can find them. Surrogate keys like "row 847291" mean nothing to business users.

**Learn more:** [Keys and Identifiers](../concepts/keys.md)

---

### Node

**What it is:** A single step in a pipeline that reads data, optionally transforms it, and writes output.

**Real-world analogy:** A station on an assembly line. Each station does one job: one paints, one installs wheels, one does quality check. Together, they build a car.

**Example:**
```yaml
nodes:
  - name: load_customers
    source: raw/customers.csv
    target: bronze.customers
    
  - name: clean_customers
    source: bronze.customers
    target: silver.customers
    transformations:
      - type: trim_strings
      - type: standardize_phone
      
  - name: customer_metrics
    source: silver.customers
    target: gold.customer_360
    pattern: aggregation
```

**Why it matters:** Breaking work into nodes makes pipelines easier to understand, debug, and maintain. If something fails, you know exactly which step broke.

**Learn more:** [Node Configuration](../reference/yaml_schema.md#nodes)

---

## O

### Orphan Record

**What it is:** A row with a foreign key value that doesn't exist in the parent table.

**Real-world analogy:** A letter addressed to someone who doesn't live at that address. The recipient doesn't exist, so the letter has nowhere to go.

**Example:**
```yaml
# Order has customer_id: 999
# But dim_customer has no customer_id: 999
# → This order is an orphan

validation:
  foreign_key_checks:
    - column: customer_id
      reference_table: dim_customer
      on_violation: quarantine
      # Orphans go to quarantine table for review
```

**Why it matters:** Orphan records break joins and analytics. Queries for "sales by customer region" can't work if the customer doesn't exist. Catching orphans prevents broken reports.

**Learn more:** [Orphan Detection](../patterns/validation.md#orphan-detection)

---

## P

### Pattern

**What it is:** A pre-built template for common data processing tasks. Instead of writing complex logic, you declare what pattern to use.

**Real-world analogy:** A recipe. You don't invent how to make bread from scratch—you follow a proven recipe. Patterns are tested recipes for data work.

**Example:**
```yaml
# Instead of writing complex SCD2 logic...
pattern: scd2
natural_key:
  - product_id
tracked_columns:
  - product_name
  - price
  - category
# Odibi handles all the history tracking automatically
```

**Available patterns:**
- `scd2` - History tracking with versioning
- `merge` - Upsert operations
- `aggregation` - Summarization
- `dimension` - Lookup table management
- `fact` - Transaction table handling

**Why it matters:** Patterns encode best practices. Writing SCD2 logic from scratch takes hours and often has bugs. Using the pattern takes 5 lines and works correctly.

**Learn more:** [Patterns Reference](../patterns/index.md)

---

### Pipeline

**What it is:** A series of connected nodes that move data from sources to targets, with transformations along the way.

**Real-world analogy:** An assembly line in a factory. Raw materials enter, go through stations (cutting, welding, painting), and finished products come out.

**Example:**
```yaml
pipeline:
  name: daily_sales_pipeline
  schedule: "0 6 * * *"  # 6 AM daily
  
  nodes:
    - name: extract_sales
      source: pos_system.transactions
      target: bronze.sales
      
    - name: clean_sales
      source: bronze.sales
      target: silver.sales
      depends_on: [extract_sales]
      
    - name: aggregate_sales
      source: silver.sales
      target: gold.daily_summary
      depends_on: [clean_sales]
```

**Why it matters:** Pipelines automate data flow. Instead of manually running scripts, pipelines run on schedule, handle failures gracefully, and process data consistently every time.

**Learn more:** [Pipeline Guide](../concepts/pipelines.md)

---

## Q

### Quarantine

**What it is:** A holding area for data that failed validation rules. Bad data is separated so it doesn't contaminate good data.

**Real-world analogy:** Airport customs. If something suspicious is found in your luggage, it's held aside for inspection. It doesn't get through to the destination until it's reviewed.

**Example:**
```yaml
validation:
  quarantine:
    enabled: true
    table: quarantine.failed_records
    include_reason: true
  rules:
    - column: email
      rule: regex
      pattern: "^[^@]+@[^@]+\\.[^@]+$"
      severity: error  # Failures go to quarantine
```

**Why it matters:** Without quarantine, bad data silently corrupts your analytics. With quarantine, good data flows through while problems are captured for review and correction.

**Learn more:** [Quarantine Setup](../patterns/validation.md#quarantine)

---

## S

### Schema

**What it is:** The structure of a table—what columns exist, what data type each column holds, and any constraints.

**Real-world analogy:** A form template. It defines: Name (text), Age (number), Email (text with @ symbol). The schema says what information goes where and in what format.

**Example:**
```yaml
schema:
  columns:
    - name: customer_id
      type: string
      nullable: false
    - name: email
      type: string
      nullable: true
    - name: signup_date
      type: date
      nullable: false
    - name: lifetime_value
      type: decimal(10,2)
      nullable: true
```

**Why it matters:** Schemas catch errors early. If someone tries to put "hello" in an integer column, the schema rejects it immediately instead of corrupting downstream reports.

**Learn more:** [Schema Definition](../reference/yaml_schema.md#schema)

---

### SCD Type 1

**What it is:** Slowly Changing Dimension handling that overwrites old values with new ones. No history is kept.

**Real-world analogy:** Updating your address with the post office. They replace your old address with the new one. They don't keep a record of where you used to live.

**Example:**
```yaml
pattern: scd1
natural_key:
  - employee_id
# Old values are overwritten:
# Before: employee_id: 123, department: "Sales"
# After:  employee_id: 123, department: "Marketing"
# No history of "Sales" is kept
```

**Why it matters:** Use SCD1 when history doesn't matter (typo corrections, updated contact info). It's simpler and uses less storage than SCD2.

**Learn more:** [SCD Patterns](../patterns/scd.md)

---

### SCD Type 2

**What it is:** Slowly Changing Dimension handling that keeps full history. Old values are marked as inactive; new values get new rows.

**Real-world analogy:** A medical record. When your weight changes, the doctor doesn't erase the old weight—they add a new entry with today's date. You can see your weight history over time.

**Example:**
```yaml
pattern: scd2
natural_key:
  - customer_id
tracked_columns:
  - loyalty_tier
  - region
valid_from_column: effective_start
valid_to_column: effective_end
is_current_column: is_current
```

**Result:**
```
customer_id | loyalty_tier | effective_start | effective_end | is_current
123         | Bronze       | 2023-01-01      | 2024-06-15    | false
123         | Gold         | 2024-06-15      | 9999-12-31    | true
```

**Why it matters:** Historical analysis requires history. "What tier was this customer when they made this purchase?" Without SCD2, you can't answer that question.

**Learn more:** [SCD2 Pattern](../patterns/scd.md#scd-type-2)

---

### Silver Layer

**What it is:** The middle layer where data is cleaned, validated, and standardized—but not yet aggregated.

**Real-world analogy:** A restaurant's prep kitchen. Raw ingredients (Bronze) are washed, chopped, and portioned (Silver). They're ready to cook but not yet finished dishes (Gold).

**Example:**
```yaml
layer: silver
nodes:
  - name: clean_orders
    source: bronze.raw_orders
    validation:
      rules:
        - column: order_id
          rule: not_null
        - column: total
          rule: positive
    transformations:
      - type: deduplicate
        keys: [order_id]
      - type: standardize_dates
        columns: [order_date]
```

**Why it matters:** Silver is your "single source of truth." Bronze might have duplicates and errors. Silver has clean, validated data that Gold and other consumers can trust.

**Learn more:** [Medallion Architecture](#medallion-architecture)

---

### Star Schema

**What it is:** A database design where a central fact table connects to multiple dimension tables, forming a star shape.

**Real-world analogy:** A wheel with spokes. The hub (fact table) is at the center. Each spoke leads to a dimension (who, what, where, when). All analysis starts at the center and reaches out.

**Diagram:**
```
                    dim_customer
                         |
    dim_product ---- fact_sales ---- dim_date
                         |
                    dim_store
```

**Example:**
```yaml
# Fact at center
fact_sales:
  pattern: fact
  foreign_keys:
    - column: customer_id
      references: dim_customer
    - column: product_id
      references: dim_product
    - column: date_id
      references: dim_date
    - column: store_id
      references: dim_store
```

**Why it matters:** Star schemas are optimized for analytics. Queries like "sales by region by month by product category" are fast because the structure matches how business users think.

**Learn more:** [Dimensional Modeling](../concepts/dimensional-modeling.md)

---

### Surrogate Key

**What it is:** An internally generated unique identifier (usually a number) that has no business meaning. Created by the system, not from source data.

**Real-world analogy:** A library book's barcode number. It's not the ISBN or title—it's a number the library made up to track that specific copy internally.

**Example:**
```yaml
generate_surrogate_key:
  column_name: customer_sk
  strategy: hash  # or: sequence, uuid
  source_columns:
    - customer_id
    - effective_start_date
```

**Why it matters:** Surrogate keys are stable (never change), performant (integers join faster than strings), and handle SCD2 (each version gets its own key). They're the internal "address" for each row.

**Learn more:** [Key Generation](../reference/yaml_schema.md#surrogate-keys)

---

## T

### Transformer

**What it is:** A reusable operation that modifies data—like a function you can apply to any dataset.

**Real-world analogy:** A coffee grinder. You put in beans (input), it grinds them (transformation), you get ground coffee (output). The same grinder works for any type of bean.

**Example:**
```yaml
transformations:
  - type: rename_columns
    mapping:
      cust_nm: customer_name
      ord_dt: order_date
      
  - type: add_column
    name: order_year
    expression: "year(order_date)"
    
  - type: filter
    condition: "order_total > 0"
```

**Available transformers:**
- `rename_columns` - Change column names
- `add_column` - Create calculated columns
- `filter` - Keep only matching rows
- `deduplicate` - Remove duplicate rows
- `join` - Combine with other tables
- And many more...

**Why it matters:** Transformers are composable building blocks. Complex data processing becomes a readable list of simple steps.

**Learn more:** [Transformers Reference](../transformers/index.md)

---

## V

### Validation

**What it is:** Checking that data meets defined rules before accepting it into your system.

**Real-world analogy:** A bouncer at a club checking IDs. No valid ID? You don't get in. Validation checks if data "has valid ID" before letting it into your tables.

**Example:**
```yaml
validation:
  rules:
    # Must have a value
    - column: order_id
      rule: not_null
      severity: error
      
    # Must be a valid email format
    - column: email
      rule: regex
      pattern: "^[^@]+@[^@]+$"
      severity: warning
      
    # Must be a real date
    - column: order_date
      rule: not_in_future
      severity: error
      
    # Must be positive
    - column: quantity
      rule: positive
      severity: error
```

**Severity levels:**
- `error` - Stop processing, quarantine the row
- `warning` - Log the issue, continue processing

**Why it matters:** Bad data in = bad decisions out. Validation catches problems at the door instead of letting them corrupt your analytics.

**Learn more:** [Validation Guide](../patterns/validation.md)

---

## Quick Reference Table

| Term | One-Line Definition |
|------|---------------------|
| Aggregation | Summarizing many rows into totals/averages |
| Append | Adding rows without changing existing ones |
| Bronze Layer | Raw data storage, untouched |
| Connection | Saved credentials for data sources |
| DAG | Map of step dependencies |
| Data Quality | Measuring data correctness |
| Delta Lake | Smart file format with versioning |
| Dimension Table | Lookup/reference data |
| Engine | Processing tool (Spark/Pandas/Polars) |
| ETL vs ELT | When transformation happens |
| Fact Table | Transaction/event data |
| Foreign Key | Link between tables |
| Gold Layer | Business-ready, curated data |
| Idempotent | Safe to run multiple times |
| Incremental Load | Only process new/changed data |
| Join | Combining data from multiple tables |
| Medallion Architecture | Bronze → Silver → Gold layering |
| Merge | Insert new, update existing |
| Natural Key | Business identifier |
| Node | Single pipeline step |
| Orphan Record | FK with no matching parent |
| Pattern | Reusable template for common tasks |
| Pipeline | Series of connected processing steps |
| Quarantine | Holding area for bad data |
| Schema | Structure of data |
| SCD Type 1 | Overwrite old with new |
| SCD Type 2 | Keep full history |
| Silver Layer | Cleaned, validated data |
| Star Schema | Facts center, dimensions around |
| Surrogate Key | Generated internal ID |
| Transformer | Reusable data operation |
| Validation | Checking data meets rules |

---

## Next Steps

- **New to Odibi?** Start with [Getting Started](../getting-started/index.md)
- **Building your first pipeline?** See [Tutorial](../tutorials/first-pipeline.md)
- **Looking for specific syntax?** Check [YAML Schema Reference](../reference/yaml_schema.md)
