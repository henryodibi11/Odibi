# 🎓 Odibi Learning Curriculum

**A 4-Week Journey from Zero to Data Engineer**

This curriculum is designed for complete beginners with **no prior data engineering experience**. By the end, you'll be able to build production-ready data pipelines.

---

## How This Course Works

- **Pace:** ~1-2 hours per day, 5 days per week
- **Style:** Learn by doing — every concept has hands-on exercises
- **Format:** Read → Try → Check → Repeat

Each week builds on the previous one, like stacking building blocks.

---

# 📅 Week 1: Bronze Layer + Basic Concepts

## 📚 Learning Objectives

By the end of this week, you will:
- Understand what data is and common file formats
- Know what a data pipeline does and why it matters
- Install Odibi and run your first pipeline
- Load raw data into a Bronze layer

## ✅ Prerequisites

Before starting, make sure you have:
- A computer (Windows, Mac, or Linux)
- Python 3.9+ installed ([Download Python](https://python.org))
- A text editor (VS Code recommended)
- Basic comfort using a terminal/command prompt

---

## Day 1: What is Data?

### 🍳 Kitchen Analogy
Think of data like ingredients in your kitchen. You have:
- **Raw ingredients** (flour, eggs, sugar) = raw data files
- **Recipes** = data transformations
- **Finished dishes** = clean, usable reports

Data comes in many "containers":

| Format | What it looks like | When to use |
|--------|-------------------|-------------|
| **CSV** | Spreadsheet-like rows and columns | Simple tabular data |
| **JSON** | Nested key-value pairs | API responses, configs |
| **Parquet** | Binary columnar format | Large datasets, analytics |
| **Delta** | Parquet + versioning + ACID | Production data lakes |

### 💻 Hands-On: Create Your First Data File

Create a folder called `my_first_pipeline` and inside it create `customers.csv`:

```csv
id,name,email,signup_date
1,Alice,alice@example.com,2024-01-15
2,Bob,bob@example.com,2024-02-20
3,Charlie,charlie@example.com,2024-03-10
```

This is **tabular data**: rows (records) and columns (fields).

### 🧪 Self-Check
- [ ] Can you explain what a CSV file is?
- [ ] What's the difference between a row and a column?

---

## Day 2: What is a Data Pipeline?

### 🏭 Assembly Line Analogy
Imagine a car factory. Raw materials enter one end, go through multiple stations (welding, painting, assembly), and a finished car comes out the other end.

A **data pipeline** works the same way:
1. **Extract** — Get raw data from somewhere (files, databases, APIs)
2. **Transform** — Clean, reshape, and enrich the data
3. **Load** — Save the result somewhere useful

This is called **ETL** (Extract, Transform, Load).

### The Medallion Architecture

Odibi uses a "layered" approach to organize data:

```
┌─────────────────────────────────────────────────────────────┐
│                      YOUR DATA LAKE                         │
├─────────────┬─────────────────┬─────────────────────────────┤
│   BRONZE    │     SILVER      │           GOLD              │
│   (Raw)     │    (Cleaned)    │       (Business-Ready)      │
├─────────────┼─────────────────┼─────────────────────────────┤
│ • As-is     │ • Deduplicated  │ • Aggregated                │
│ • Untouched │ • Typed         │ • Joined                    │
│ • Archived  │ • Validated     │ • Ready for reporting       │
└─────────────┴─────────────────┴─────────────────────────────┘
```

**Why layers?**
- If something breaks, you can always go back to Bronze
- Each layer has a clear purpose
- Teams can work on different layers independently

📖 **Deep Dive:** [Medallion Architecture Guide](../guides/medallion_architecture.md)

### 🧪 Self-Check
- [ ] What does ETL stand for?
- [ ] Why do we have separate Bronze, Silver, and Gold layers?

---

## Day 3: Introduction to Odibi

### What is Odibi?

Odibi is a **YAML-first data pipeline framework**. Instead of writing hundreds of lines of code, you describe what you want in simple configuration files.

### 🔧 Installation

Open your terminal and run:

```bash
# Create a virtual environment (recommended)
python -m venv .venv

# Activate it
# Windows:
.venv\Scripts\activate
# Mac/Linux:
source .venv/bin/activate

# Install Odibi
pip install odibi
```

Verify it works:
```bash
odibi --version
```

### Your First Odibi Project

Let Odibi create a project structure for you:

```bash
odibi init-pipeline my_first_project --template local-medallion
cd my_first_project
```

This creates:
```
my_first_project/
├── odibi.yaml          # Your pipeline configuration
├── data/
│   ├── landing/        # Where raw files arrive
│   ├── bronze/         # Raw data preserved
│   ├── silver/         # Cleaned data
│   └── gold/           # Business-ready data
└── README.md
```

📖 **Deep Dive:** [Getting Started Tutorial](../tutorials/getting_started.md)

### 🧪 Self-Check
- [ ] What command installs Odibi?
- [ ] What folder does raw data go into?

---

## Day 4: The Bronze Layer

### 📦 Filing Cabinet Analogy
Think of Bronze as your filing cabinet where you store **original documents**. You never write on the originals — you make copies first.

The Bronze layer:
- Stores data **exactly as received**
- Never modifies or cleans anything
- Acts as your "source of truth" backup

### 💻 Hands-On: Build a Bronze Pipeline

1. Copy your `customers.csv` to `data/landing/`

2. Edit `odibi.yaml`:

```yaml
project: "my_first_project"
engine: "pandas"

connections:
  local:
    type: local
    base_path: "./data"

story:
  connection: local
  path: stories

system:
  connection: local
  path: system

pipelines:
  - pipeline: bronze_customers
    layer: bronze
    description: "Load raw customer data"
    nodes:
      - name: raw_customers
        description: "Ingest customers from landing zone"
        
        read:
          connection: local
          path: landing/customers.csv
          format: csv
        
        write:
          connection: local
          path: bronze/customers
          format: parquet
          mode: overwrite
```

3. Run your pipeline:

```bash
odibi run odibi.yaml
```

4. Check your output:
```bash
# You should see a parquet file in data/bronze/customers/
ls data/bronze/customers/
```

### What Just Happened?

1. Odibi **read** your CSV file
2. Converted it to **Parquet** format (more efficient for analytics)
3. Saved it to the Bronze layer

No data was modified — just preserved in a better format.

📖 **Deep Dive:** [Bronze Layer Tutorial](../tutorials/bronze_layer.md)

### 🧪 Self-Check
- [ ] Why don't we clean data in Bronze?
- [ ] What format did we convert the CSV to?

---

## Day 5: Multi-Node Pipelines

### 🚂 Train Cars Analogy
A pipeline is like a train. Each **node** is a train car — they're connected and run in sequence.

### 💻 Hands-On: Add More Data

1. Create `data/landing/orders.csv`:

```csv
order_id,customer_id,product,amount,order_date
1001,1,Widget A,29.99,2024-01-20
1002,2,Widget B,49.99,2024-02-25
1003,1,Widget C,19.99,2024-03-15
1004,3,Widget A,29.99,2024-03-20
```

2. Add a second node to your pipeline:

```yaml
pipelines:
  - pipeline: bronze_ingest
    layer: bronze
    description: "Load all raw data"
    nodes:
      - name: raw_customers
        description: "Ingest customers"
        read:
          connection: local
          path: landing/customers.csv
          format: csv
        write:
          connection: local
          path: bronze/customers
          format: parquet
          mode: overwrite

      - name: raw_orders
        description: "Ingest orders"
        read:
          connection: local
          path: landing/orders.csv
          format: csv
        write:
          connection: local
          path: bronze/orders
          format: parquet
          mode: overwrite
```

3. Run it:
```bash
odibi run odibi.yaml
```

Both datasets are now in your Bronze layer.

### 🧪 Self-Check
- [ ] What is a "node" in Odibi?
- [ ] Can a pipeline have multiple nodes?

---

## 📝 Week 1 Summary

You learned:
- Data comes in different formats (CSV, JSON, Parquet)
- Pipelines move data through stages (ETL)
- Medallion architecture organizes data into layers
- Bronze layer stores raw, unmodified data
- Odibi uses YAML configuration to define pipelines

**Congratulations!** You've built your first data pipeline. 🎉

---

# 📅 Week 2: Silver Layer + SCD2 + Data Quality

## 📚 Learning Objectives

By the end of this week, you will:
- Clean and transform data in the Silver layer
- Understand and implement SCD2 (history tracking)
- Add data quality checks to catch bad data
- Handle missing values and invalid data

## ✅ Prerequisites

Before starting, make sure you have:
- Completed Week 1
- A working Bronze layer with customer and order data

---

## Day 1: Why Data Cleaning Matters

### 🧹 Dirty Kitchen Analogy
Imagine cooking with ingredients covered in dirt, or using expired milk. The result would be... unpleasant.

**Bad data causes:**
- Wrong business decisions
- Broken reports
- Angry users
- Lost revenue

### Common Data Problems

| Problem | Example | Impact |
|---------|---------|--------|
| **Missing values** | `email: NULL` | Can't contact customer |
| **Invalid format** | `date: "not-a-date"` | Calculations fail |
| **Duplicates** | Same order twice | Revenue doubled incorrectly |
| **Inconsistent** | "CA", "California", "ca" | Grouping breaks |

### The Silver Layer's Job

The Silver layer is your **cleaning station**:
- Fix data types (strings to dates, etc.)
- Remove duplicates
- Handle missing values
- Validate data quality

### 🧪 Self-Check
- [ ] Name 3 common data quality problems
- [ ] What layer handles data cleaning?

---

## Day 2: Building a Silver Pipeline

### 💻 Hands-On: Clean Your Customer Data

1. Update `odibi.yaml` to add a Silver pipeline:

```yaml
pipelines:
  # ... your bronze pipeline from Week 1 ...

  - pipeline: silver_customers
    layer: silver
    description: "Clean and standardize customers"
    nodes:
      - name: clean_customers
        description: "Apply cleaning transformations"
        
        read:
          connection: local
          path: bronze/customers
          format: parquet
        
        transform:
          - type: rename
            columns:
              id: customer_id
          
          - type: cast
            columns:
              customer_id: integer
              signup_date: date
          
          - type: fill_null
            columns:
              email: "unknown@example.com"
          
          - type: lower
            columns:
              - email
        
        write:
          connection: local
          path: silver/customers
          format: parquet
          mode: overwrite
```

2. Run it:
```bash
odibi run odibi.yaml --pipeline silver_customers
```

### What Each Transform Does

| Transform | Purpose | Example |
|-----------|---------|---------|
| `rename` | Change column names | `id` → `customer_id` |
| `cast` | Change data types | String → Date |
| `fill_null` | Replace missing values | NULL → default value |
| `lower` | Lowercase text | "BOB@EMAIL.COM" → "bob@email.com" |

📖 **Deep Dive:** [Silver Layer Tutorial](../tutorials/silver_layer.md)

### 🧪 Self-Check
- [ ] What does `cast` do?
- [ ] Why lowercase email addresses?

---

## Day 3: SCD2 — Tracking History

### ⏰ Time Machine Analogy
Imagine you could look at a customer's record **as it was 6 months ago**. Where did they live? What tier were they?

**SCD2** (Slowly Changing Dimension Type 2) makes this possible by:
- Never deleting old records
- Adding new versions when data changes
- Tracking when each version was valid

### Visual Example

**Customer moves from CA to NY on Feb 1:**

| customer_id | address | valid_from | valid_to   | is_current |
|-------------|---------|------------|------------|------------|
| 101         | CA      | 2024-01-01 | 2024-02-01 | false      |
| 101         | NY      | 2024-02-01 | NULL       | true       |

Now you can answer: "Where did customer 101 live on January 15th?" → **CA**

### 💻 Hands-On: Add SCD2 to Customers

1. First, update your source data. Create `data/landing/customers_update.csv`:

```csv
id,name,email,signup_date,tier
1,Alice,alice@example.com,2024-01-15,Gold
2,Bob,bob_new@example.com,2024-02-20,Silver
3,Charlie,charlie@example.com,2024-03-10,Bronze
4,Diana,diana@example.com,2024-04-01,Gold
```

*(Notice: Bob has a new email, and Diana is a new customer)*

2. Add an SCD2 node:

```yaml
  - pipeline: silver_customers_scd2
    layer: silver
    description: "Track customer history"
    nodes:
      - name: customers_with_history
        description: "Apply SCD2 for full history"
        
        read:
          connection: local
          path: landing/customers_update.csv
          format: csv
        
        transformer: scd2
        params:
          connection: local
          path: silver/dim_customers
          keys:
            - id
          track_cols:
            - email
            - tier
          effective_time_col: signup_date
        
        write:
          connection: local
          path: silver/dim_customers
          format: parquet
          mode: overwrite
```

3. Run it:
```bash
odibi run odibi.yaml --pipeline silver_customers_scd2
```

📖 **Deep Dive:** [SCD2 Pattern](../patterns/scd2.md)

### 🧪 Self-Check
- [ ] What does SCD2 stand for?
- [ ] What column tells you if a record is the current version?

---

## Day 4: Data Quality Validation

### 🚨 Security Guard Analogy
Before entering a building, security checks your ID. Data quality validation checks your data before it enters the Silver layer.

### Types of Checks

| Check Type | What it does | Example |
|------------|--------------|---------|
| **not_null** | Ensure value exists | `customer_id` can't be empty |
| **unique** | No duplicates | Each `email` is unique |
| **range** | Value in bounds | `age` between 0 and 150 |
| **regex** | Pattern matching | Email contains `@` |
| **foreign_key** | Reference exists | `customer_id` exists in customers table |

### 💻 Hands-On: Add Validation

1. Add validation to your node:

```yaml
      - name: clean_customers
        description: "Clean with validation"
        
        read:
          connection: local
          path: bronze/customers
          format: parquet
        
        validation:
          rules:
            - column: customer_id
              check: not_null
              severity: error
            
            - column: email
              check: not_null
              severity: warn
            
            - column: email
              check: regex
              pattern: ".*@.*\\..*"
              severity: error
          
          on_failure: quarantine  # Bad rows go to quarantine
        
        write:
          connection: local
          path: silver/customers
          format: parquet
          mode: overwrite
```

### Severity Levels

| Level | What happens |
|-------|--------------|
| `warn` | Log warning, continue processing |
| `error` | Quarantine bad rows, continue with good rows |
| `fatal` | Stop the entire pipeline |

📖 **Deep Dive:** [Data Validation](../validation/README.md)

### 🧪 Self-Check
- [ ] What does `quarantine` mean?
- [ ] What's the difference between `warn` and `error` severity?

---

## Day 5: Putting It Together

### 💻 Hands-On: Complete Silver Pipeline

Create a complete Silver pipeline that:
1. Reads from Bronze
2. Cleans and transforms
3. Validates quality
4. Tracks history with SCD2

```yaml
  - pipeline: silver_complete
    layer: silver
    description: "Complete silver processing"
    nodes:
      - name: stg_customers
        description: "Stage and clean customers"
        
        read:
          connection: local
          path: bronze/customers
          format: parquet
        
        transform:
          - type: rename
            columns:
              id: customer_id
          - type: cast
            columns:
              customer_id: integer
              signup_date: date
          - type: trim
            columns:
              - name
              - email
        
        validation:
          rules:
            - column: customer_id
              check: not_null
              severity: error
            - column: email
              check: regex
              pattern: ".*@.*"
              severity: warn
          on_failure: quarantine
        
        write:
          connection: local
          path: silver/stg_customers
          format: parquet
          mode: overwrite

      - name: dim_customers
        description: "Create customer dimension with history"
        depends_on:
          - stg_customers
        
        read:
          connection: local
          path: silver/stg_customers
          format: parquet
        
        transformer: scd2
        params:
          connection: local
          path: silver/dim_customers
          keys:
            - customer_id
          track_cols:
            - name
            - email
          effective_time_col: signup_date
        
        write:
          connection: local
          path: silver/dim_customers
          format: parquet
          mode: overwrite
```

### 🧪 Self-Check
- [ ] What does `depends_on` do?
- [ ] Why do we stage data before applying SCD2?

---

## 📝 Week 2 Summary

You learned:
- Why data cleaning is critical
- How to transform data (rename, cast, fill_null)
- SCD2 tracks historical changes
- Validation catches bad data before it causes problems
- Quarantine isolates bad rows for review

**Great progress!** Your data is now clean and trackable. 🎉

---

# 📅 Week 3: Gold Layer + Dimensional Modeling

## 📚 Learning Objectives

By the end of this week, you will:
- Understand Facts vs Dimensions
- Build a star schema
- Use surrogate keys
- Create aggregations for reporting
- Build a complete data warehouse

## ✅ Prerequisites

Before starting, make sure you have:
- Completed Weeks 1 and 2
- Working Bronze and Silver layers

---

## Day 1: Facts vs Dimensions

### 🎭 Theater Analogy
Think of a theater production:
- **Facts** = The events (ticket sales, performances)
- **Dimensions** = The context (who, what, when, where)

**Every fact answers: "What happened?"**
**Every dimension answers: "Tell me more about..."**

### Examples

| Facts (Events) | Dimensions (Context) |
|----------------|----------------------|
| Order placed | Customer, Product, Date |
| Payment received | Customer, Account, Date |
| Page viewed | User, Page, Date |

### Visual: A Sales Transaction

```
┌─────────────────────────────────────────────────────────────┐
│                     FACT: Order                             │
│  order_id=1001, amount=49.99, quantity=2                    │
└──────────┬──────────┬──────────┬───────────────────────────┘
           │          │          │
           ▼          ▼          ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│ DIM: Customer│ │ DIM: Product │ │ DIM: Date    │
│ name=Alice   │ │ name=Widget  │ │ date=2024-01 │
│ tier=Gold    │ │ category=HW  │ │ quarter=Q1   │
└──────────────┘ └──────────────┘ └──────────────┘
```

### 🧪 Self-Check
- [ ] Is "order amount" a fact or dimension?
- [ ] Is "customer name" a fact or dimension?

---

## Day 2: Star Schema Basics

### ⭐ Star Analogy
A star schema looks like a star: the **fact table** is in the center, with **dimension tables** around it like points.

```
                    ┌─────────────┐
                    │ dim_product │
                    └──────┬──────┘
                           │
┌─────────────┐     ┌──────┴──────┐     ┌─────────────┐
│ dim_customer│─────│  fact_sales │─────│  dim_date   │
└─────────────┘     └──────┬──────┘     └─────────────┘
                           │
                    ┌──────┴──────┐
                    │ dim_location│
                    └─────────────┘
```

**Why stars?**
- Simple to understand
- Fast to query (fewer joins)
- Works with every BI tool

### Dimension Table Structure

```yaml
# dim_customers
customer_key: 1          # Surrogate key (system-generated)
customer_id: "C001"      # Natural key (from source)
name: "Alice"
email: "alice@example.com"
tier: "Gold"
effective_from: "2024-01-01"
effective_to: null
is_current: true
```

### Fact Table Structure

```yaml
# fact_orders
order_key: 1001
customer_key: 1          # Points to dim_customers
product_key: 42          # Points to dim_products
date_key: 20240120       # Points to dim_date
quantity: 2
amount: 49.99
```

📖 **Deep Dive:** [Dimensional Modeling Guide](../guides/dimensional_modeling_guide.md)

### 🧪 Self-Check
- [ ] Why is it called a "star" schema?
- [ ] What's in the center of the star?

---

## Day 3: Surrogate Keys

### 🔑 Hotel Room Key Analogy
When you check into a hotel, they give you a room key. This key is:
- **Unique to your stay** (not your name)
- **System-generated** (you don't choose it)
- **Internal** (the hotel manages it)

A **surrogate key** works the same way:
- Unique identifier for each record
- Generated by the system (not from source data)
- Never changes, even if source data changes

### Why Not Use Natural Keys?

| Problem | Natural Key Example | Issue |
|---------|---------------------|-------|
| Changes | SSN gets corrected | Breaks all references |
| Duplicates | "John Smith" | Too common |
| Missing | New customer, no ID yet | Can't insert |
| Composite | firstName + lastName + DOB | Slow to join |

### 💻 Hands-On: Generate Surrogate Keys

Odibi can auto-generate surrogate keys:

```yaml
  - pipeline: gold_dimensions
    layer: gold
    description: "Build dimension tables"
    nodes:
      - name: dim_customers
        description: "Customer dimension with surrogate keys"
        
        read:
          connection: local
          path: silver/dim_customers
          format: parquet
        
        transform:
          - type: generate_surrogate_key
            key_column: customer_key
            source_columns:
              - customer_id
        
        write:
          connection: local
          path: gold/dim_customers
          format: parquet
          mode: overwrite
```

The `generate_surrogate_key` transform creates a unique integer for each unique combination of source columns.

📖 **Deep Dive:** [Dimension Pattern](../patterns/dimension.md)

### 🧪 Self-Check
- [ ] What's wrong with using email as a primary key?
- [ ] Who generates surrogate keys — the source system or our data warehouse?

---

## Day 4: Building Fact Tables

### 💻 Hands-On: Create a Sales Fact Table

1. First, ensure you have a date dimension. Create `data/landing/dates.csv`:

```csv
date_key,full_date,year,quarter,month,day_of_week
20240115,2024-01-15,2024,Q1,January,Monday
20240120,2024-01-20,2024,Q1,January,Saturday
20240220,2024-02-20,2024,Q1,February,Tuesday
20240225,2024-02-25,2024,Q1,February,Sunday
20240310,2024-03-10,2024,Q1,March,Sunday
20240315,2024-03-15,2024,Q1,March,Friday
20240320,2024-03-20,2024,Q1,March,Wednesday
```

2. Build the fact table:

```yaml
      - name: fact_orders
        description: "Order fact table"
        depends_on:
          - dim_customers
        
        read:
          - connection: local
            path: silver/orders
            format: parquet
            alias: orders
          - connection: local
            path: gold/dim_customers
            format: parquet
            alias: customers
        
        transform:
          - type: join
            left: orders
            right: customers
            on:
              - left: customer_id
                right: customer_id
            how: left
            filter: "is_current = true"  # Only join to current customer version
          
          - type: select
            columns:
              - order_id
              - customer_key
              - product
              - amount
              - order_date
          
          - type: cast
            columns:
              order_date: date
          
          - type: add_column
            name: date_key
            expression: "date_format(order_date, 'yyyyMMdd')"
        
        write:
          connection: local
          path: gold/fact_orders
          format: parquet
          mode: overwrite
```

📖 **Deep Dive:** [Fact Pattern](../patterns/fact.md)

### 🧪 Self-Check
- [ ] Why do we filter for `is_current = true` when joining?
- [ ] What's the purpose of `date_key`?

---

## Day 5: Aggregations for Reporting

### 📊 Summary Reports Analogy
Instead of reading every receipt, store managers want:
- "Total sales this month"
- "Average order size by customer tier"
- "Top 10 products"

Aggregations pre-compute these summaries.

### 💻 Hands-On: Build an Aggregation

```yaml
  - pipeline: gold_aggregations
    layer: gold
    description: "Pre-computed summaries"
    nodes:
      - name: agg_sales_by_customer
        description: "Sales summary per customer"
        
        read:
          connection: local
          path: gold/fact_orders
          format: parquet
        
        pattern:
          type: aggregation
          params:
            group_by:
              - customer_key
            metrics:
              - name: total_orders
                expression: "count(*)"
              - name: total_revenue
                expression: "sum(amount)"
              - name: avg_order_value
                expression: "avg(amount)"
              - name: first_order_date
                expression: "min(order_date)"
              - name: last_order_date
                expression: "max(order_date)"
        
        write:
          connection: local
          path: gold/agg_sales_by_customer
          format: parquet
          mode: overwrite
```

📖 **Deep Dive:** [Aggregation Pattern](../patterns/aggregation.md)

### 🧪 Self-Check
- [ ] Why pre-compute aggregations instead of calculating on-the-fly?
- [ ] What does `group_by` do?

---

## 📝 Week 3 Summary

You learned:
- Facts record events, dimensions provide context
- Star schemas are simple and fast
- Surrogate keys are stable, system-generated identifiers
- Fact tables link to dimensions via keys
- Aggregations pre-compute summaries for fast reporting

**Amazing work!** You've built a complete data warehouse. 🎉

---

# 📅 Week 4: Production Deployment + Best Practices

## 📚 Learning Objectives

By the end of this week, you will:
- Configure connections for different environments
- Implement error handling and retry logic
- Add monitoring and logging
- Tune performance for large datasets
- Deploy to production with confidence

## ✅ Prerequisites

Before starting, make sure you have:
- Completed Weeks 1-3
- A complete Bronze → Silver → Gold pipeline

---

## Day 1: Connections and Environments

### 🏠 Different Homes Analogy
Your pipeline needs to work in different "homes":
- **Development** — Your laptop, small test data
- **Staging** — Test server, realistic data
- **Production** — Real deal, live data

Each environment has different connection details.

### 💻 Hands-On: Configure Environments

```yaml
project: "my_project"
engine: "pandas"

# Global variables
vars:
  env: ${ODIBI_ENV:dev}  # Default to 'dev' if not set

# Environment-specific overrides
environments:
  dev:
    connections:
      data_lake:
        type: local
        base_path: "./data"
  
  staging:
    connections:
      data_lake:
        type: azure_blob
        account_name: "mystorageacct"
        container: "staging-data"
        credential: ${AZURE_STORAGE_KEY}
  
  prod:
    connections:
      data_lake:
        type: azure_blob
        account_name: "prodstorageacct"
        container: "prod-data"
        credential: ${AZURE_STORAGE_KEY}

connections:
  data_lake:
    type: local
    base_path: "./data"
```

Run for a specific environment:
```bash
ODIBI_ENV=staging odibi run odibi.yaml
```

📖 **Deep Dive:** [Environments Guide](../guides/environments.md)

### 🧪 Self-Check
- [ ] Why use environment variables for credentials?
- [ ] What's the default environment if `ODIBI_ENV` isn't set?

---

## Day 2: Error Handling and Retry Logic

### 🔄 Retry Analogy
If your phone call fails, you try again. Networks are unreliable; databases timeout. Retries handle temporary failures.

### 💻 Hands-On: Configure Retries

```yaml
project: "production_pipeline"
engine: "spark"

retry:
  enabled: true
  max_attempts: 3
  backoff: exponential    # Wait longer between each retry
  initial_delay: 5        # First retry after 5 seconds
  max_delay: 300          # Never wait more than 5 minutes

pipelines:
  - pipeline: bronze_ingest
    nodes:
      - name: fetch_api_data
        retry:
          max_attempts: 5  # Override for this node
        read:
          connection: external_api
          path: /customers
          format: json
```

### Backoff Strategies

| Strategy | Wait times (5s initial) | Best for |
|----------|------------------------|----------|
| `constant` | 5s, 5s, 5s | Simple cases |
| `linear` | 5s, 10s, 15s | Gradual increase |
| `exponential` | 5s, 10s, 20s, 40s | API rate limits |

### Handling Failures

```yaml
        on_failure: continue  # Options: fail, continue, skip
```

| Action | Behavior |
|--------|----------|
| `fail` | Stop entire pipeline (default) |
| `continue` | Log error, continue to next node |
| `skip` | Skip downstream nodes that depend on this one |

### 🧪 Self-Check
- [ ] What does "exponential backoff" mean?
- [ ] When would you use `on_failure: continue`?

---

## Day 3: Monitoring and Logging

### 📺 Dashboard Analogy
A pilot needs instruments to fly safely. You need monitoring to run pipelines safely.

### 💻 Hands-On: Configure Logging

```yaml
logging:
  level: INFO              # DEBUG, INFO, WARNING, ERROR
  structured: true         # JSON format for log aggregators
  include_metrics: true    # Row counts, timing
  
alerts:
  - type: slack
    url: ${SLACK_WEBHOOK_URL}
    on_events:
      - on_failure
      - on_success
  
  - type: email
    to:
      - data-team@company.com
    on_events:
      - on_failure
```

### What Gets Logged

Every pipeline run generates a **Data Story** with:
- Start/end timestamps
- Row counts (read/written/quarantined)
- Validation results
- Error messages

View your story:
```bash
odibi story show --latest
```

### 🧪 Self-Check
- [ ] What's the difference between INFO and DEBUG logging?
- [ ] What is a "Data Story"?

---

## Day 4: Performance Tuning

### 🏎️ Race Car Analogy
A race car needs tuning to go fast. Data pipelines need tuning for large datasets.

### Key Performance Levers

| Lever | When to use | Configuration |
|-------|-------------|---------------|
| **Partitioning** | Large tables (>1M rows) | Split data by date/category |
| **Caching** | Reused datasets | Keep in memory |
| **Parallelism** | Multiple nodes | Run independent nodes together |
| **Batch size** | Memory limits | Process in chunks |

### 💻 Hands-On: Add Partitioning

```yaml
        write:
          connection: data_lake
          path: gold/fact_orders
          format: delta
          mode: overwrite
          partition_by:
            - order_year
            - order_month
```

### 💻 Hands-On: Enable Caching

```yaml
      - name: dim_customers
        cache: true          # Keep in memory for downstream nodes
        read:
          connection: local
          path: silver/dim_customers
```

### 💻 Hands-On: Performance Config

```yaml
performance:
  max_parallel_nodes: 4    # Run up to 4 nodes simultaneously
  batch_size: 100000       # Process 100k rows at a time
  shuffle_partitions: 200  # Spark shuffle partitions
```

📖 **Deep Dive:** [Performance Tuning Guide](../guides/performance_tuning.md)

### 🧪 Self-Check
- [ ] Why partition by date?
- [ ] What does caching do?

---

## Day 5: Production Deployment Checklist

### 🚀 Launch Checklist

Before deploying to production, verify:

#### Configuration
- [ ] All secrets use environment variables (never hardcoded)
- [ ] Correct environment settings for prod
- [ ] Retry logic enabled
- [ ] Alerts configured

#### Data Quality
- [ ] Validation rules on all critical columns
- [ ] Quarantine configured for bad rows
- [ ] Foreign key checks enabled

#### Performance
- [ ] Partitioning on large tables
- [ ] Appropriate parallelism
- [ ] Tested with production-scale data

#### Operations
- [ ] Logging at INFO level
- [ ] Monitoring dashboard set up
- [ ] Runbook for common failures
- [ ] Backup/restore procedures documented

### Complete Production Config

```yaml
project: "customer360"
engine: "spark"
version: "1.0.0"
owner: "data-team@company.com"
description: "Customer analytics pipeline"

vars:
  env: ${ODIBI_ENV:prod}

retry:
  enabled: true
  max_attempts: 3
  backoff: exponential

logging:
  level: INFO
  structured: true
  include_metrics: true

alerts:
  - type: slack
    url: ${SLACK_WEBHOOK}
    on_events: [on_failure]

performance:
  max_parallel_nodes: 8
  batch_size: 500000

connections:
  data_lake:
    type: azure_blob
    account_name: ${AZURE_STORAGE_ACCOUNT}
    container: "prod-data"
    credential: ${AZURE_STORAGE_KEY}

story:
  connection: data_lake
  path: _odibi/stories

system:
  connection: data_lake
  path: _odibi/system

pipelines:
  # ... your pipelines ...
```

📖 **Deep Dive:** [Production Deployment Guide](../guides/production_deployment.md)

### 🧪 Self-Check
- [ ] What should NEVER be hardcoded in config?
- [ ] What logging level is recommended for production?

---

## 📝 Week 4 Summary

You learned:
- Environments separate dev/staging/prod configurations
- Retry logic handles temporary failures
- Logging and alerts keep you informed
- Partitioning and caching improve performance
- A production checklist prevents common mistakes

**Congratulations! You've completed the Odibi curriculum!** 🎓🎉

---

# 🎯 What's Next?

Now that you've completed the basics:

1. **Build a real project** — Apply what you learned to actual data
2. **Explore advanced patterns** — [Browse all patterns](../patterns/README.md)
3. **Learn the CLI** — [CLI Master Guide](../guides/cli_master_guide.md)
4. **Join the community** — Share your projects, ask questions

## Quick Reference Links

| Topic | Link |
|-------|------|
| All Patterns | [../patterns/README.md](../patterns/README.md) |
| YAML Reference | [../reference/yaml_schema.md](../reference/yaml_schema.md) |
| Best Practices | [../guides/best_practices.md](../guides/best_practices.md) |
| Troubleshooting | [../troubleshooting.md](../troubleshooting.md) |

---

*Built with ❤️ for data engineers who are just getting started.*
