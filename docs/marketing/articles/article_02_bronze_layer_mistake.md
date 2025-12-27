# The Bronze Layer Mistake That Cost Me 6 Months of Data

*Why you should never transform data during ingestion-and what to do instead*

---

## TL;DR

I "cleaned" data during ingestion to the Bronze layer-removing duplicates, filtering records, transforming dates. Six months later, the business needed the original values. They were gone. The lesson: Bronze layer has one job-preserve raw data exactly as received. All transformations belong in Silver.

---

## The Mistake

It seemed like a good idea at the time.

I was building a data pipeline for operational reporting. The source data was messy:
- Duplicate records
- Inconsistent date formats
- Records with statuses we "didn't need"
- Columns with strange characters

So I cleaned it up. Right there, during the ingest process.

```yaml
# What I did (DON'T DO THIS)
- name: bronze_orders
  read:
    connection: source
    path: orders.csv
  transform:
    steps:
      - sql: "SELECT * FROM df WHERE status != 'canceled'"
      - function: deduplicate
        params:
          keys: [order_id]
      - function: standardize_dates
        params:
          columns: [order_date]
  write:
    connection: bronze
    path: orders
```

Clean data in, clean data stored. Efficient, right?

Wrong.

## The Consequences

Six months later, the business came with a request:

> "We need to analyze canceled orders. Can you show us the original cancellation patterns?"

Me: "Sure, let me pull that from... oh."

The canceled orders were gone. I had filtered them out during ingestion.

> "Also, we noticed some date discrepancies. Can you show us what the original dates looked like before transformation?"

Me: "I... no. I can't."

I had transformed the dates. The originals were overwritten.

**Six months of historical data. Unrecoverable.**

Not because of a system failure. Because of a design decision I made on day one.

## What is the Bronze Layer?

To understand why this was such a big mistake, you need to understand the Medallion Architecture.

### The Medallion Architecture

Modern data platforms typically organize data into three layers:

```
┌─────────────────────────────────────────────────────────────┐
│                      YOUR DATA LAKE                         │
├─────────────┬─────────────────┬─────────────────────────────┤
│   BRONZE    │     SILVER      │           GOLD              │
│   (Raw)     │    (Cleaned)    │       (Business-Ready)      │
├─────────────┼─────────────────┼─────────────────────────────┤
│ • As-is     │ • Deduplicated  │ • Aggregated                │
│ • Untouched │ • Validated     │ • Joined                    │
│ • Archived  │ • Typed         │ • Ready for dashboards      │
└─────────────┴─────────────────┴─────────────────────────────┘
```

**Bronze:** Raw data, exactly as received from the source. No transformations.

**Silver:** Cleaned, validated, deduplicated. Business logic applied. Single source of truth.

**Gold:** Aggregated, joined, optimized for consumption. Ready for dashboards and reports.

### Why Bronze Exists

Bronze is your insurance policy.

When (not if) something goes wrong downstream, Bronze lets you:
- See what the source actually sent
- Reprocess with different logic
- Debug data quality issues
- Answer questions you didn't anticipate

If you transform in Bronze, you lose all of this.

## The Right Way

Here's what I should have done:

### Step 1: Bronze - Land Raw Data

```yaml
- name: bronze_orders
  read:
    connection: source
    path: orders.csv
  
  # Only add metadata - no transformations!
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

That's it. Read it. Add metadata. Write it.

The only columns I add in Bronze:
- `_extracted_at` - When this data was loaded
- `_source_file` - Where it came from
- `_batch_id` - Optional, for tracking pipeline runs

These are metadata columns that don't alter the source data. They help with debugging but preserve everything from the source.

### Step 2: Silver - Clean and Transform

```yaml
- name: silver_orders
  read:
    connection: bronze
    path: orders
  
  # NOW we transform
  transform:
    steps:
      - function: deduplicate
        params:
          keys: [order_id]
          order_by: "_extracted_at DESC"
      
      - function: cast_columns
        params:
          columns:
            order_date: timestamp
            amount: decimal
      
      - function: clean_text
        params:
          columns: [customer_name]
          trim: true
          lowercase: true
  
  # Validation contracts
  contracts:
    - type: not_null
      columns: [order_id, customer_id]
    - type: accepted_values
      column: status
      values: [pending, shipped, delivered, canceled]
  
  write:
    connection: silver
    path: orders
    format: delta
```

Notice: I'm NOT filtering out canceled orders. I'm validating that the status values are expected, but I'm keeping all records.

If I want to exclude canceled orders, I do that in Gold or in specific reports-not in the core data layer.

### Step 3: Gold - Aggregate and Optimize

```yaml
- name: gold_daily_orders
  read:
    connection: silver
    path: orders
  
  # Filter if needed for this specific use case
  transform:
    steps:
      - sql: "SELECT * FROM df WHERE status = 'delivered'"
  
  # Aggregate
  pattern:
    type: aggregation
    params:
      grain: [order_date, region]
      measures:
        - name: order_count
          expr: "COUNT(*)"
        - name: total_revenue
          expr: "SUM(amount)"
  
  write:
    connection: gold
    table: daily_orders_summary
```

Now if the business wants canceled orders, I can:
1. Go back to Bronze (raw data is intact)
2. Create a different Gold table with different filters
3. Answer questions I didn't anticipate

## The Rules

### What Belongs in Bronze

✅ **Do:**
- Store data exactly as received
- Add extraction metadata (_extracted_at, _source_file)
- Use append mode (preserve history)
- Use a durable format (Delta Lake, Parquet)

❌ **Don't:**
- Filter rows
- Transform values
- Deduplicate
- Apply business logic
- Cast types

### What Belongs in Silver

✅ **Do:**
- Deduplicate
- Cast types
- Standardize formats
- Validate data quality
- Apply business rules
- Handle nulls

### What Belongs in Gold

✅ **Do:**
- Aggregate for specific use cases
- Join across domains
- Optimize for query patterns
- Build dimensional models

## Real-World Implications

### Debugging is Easier

When a dashboard shows wrong numbers, the first question is: "What did the source send?"

With Bronze intact, you can answer that in seconds.

Without Bronze, you're guessing.

### Reprocessing is Possible

Business logic changes. What was "correct" six months ago might be wrong today.

With Bronze, you can reprocess all historical data with new logic.

Without Bronze, you're stuck with decisions made months ago.

### Auditing is Simple

Auditors ask: "Show me the original data."

With Bronze, you show them.

Without Bronze, you show them transformed data and hope they don't ask follow-up questions.

## The Cost of Getting it Wrong

In my case:
- Six months of canceled order data: unrecoverable
- Original date formats: lost
- Audit trail: compromised
- Trust with business stakeholders: damaged

All because I wanted to be "efficient" by cleaning data early.

The storage cost of keeping raw data? Negligible.

The cost of losing it? Significant.

## How I Fixed It

I couldn't recover the lost data. What I could do:

1. **Redesign the pipeline** with proper Bronze/Silver/Gold separation
2. **Document the incident** so others don't repeat it
3. **Build better habits** for future projects

From that point forward, Bronze has been sacred. No transformations. Ever.

## Takeaways

1. **Bronze is your undo button.** Don't break it.

2. **Store everything.** Disk is cheap. Lost data is expensive.

3. **Transform in Silver.** That's what it's for.

4. **Filter in Gold.** Specific use cases get specific views.

5. **Add metadata.** `_extracted_at` and `_source_file` cost nothing and save hours of debugging.

---

## The Framework

After making this mistake (and others), I started building patterns to prevent them.

That became [Odibi](https://github.com/henryodibi11/odibi)-a framework for declarative data pipelines that enforces good practices by default.

If you're interested in how I approach these problems now, follow along. I'll be sharing more patterns, anti-patterns, and lessons learned.

---

*I'm Henry-a data engineer who learned most of these lessons the hard way. I write about data engineering, building in public, and the tools I create along the way.*

---

## Connect

- **LinkedIn:** [Your LinkedIn URL]
- **GitHub:** [Odibi Repository URL]  
- **Medium:** Follow for more articles in this series
