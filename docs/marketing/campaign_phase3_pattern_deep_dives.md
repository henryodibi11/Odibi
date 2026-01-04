# LinkedIn Campaign - Phase 3: Pattern Deep Dives

**Duration:** Weeks 11-16 (18 posts)
**Goal:** Establish expertise through deep technical content on data patterns
**Tone:** Teaching, authoritative but humble, practical examples

---

## Week 11: SCD2 Deep Dive

### Post 11.1 - What is SCD2? (Monday)

**Hook:**
Your customer moved from California to New York. Do you update their record or keep both addresses?

**Body:**
This is the SCD2 question.

**SCD = Slowly Changing Dimension**

Type 1: Overwrite. Customer now lives in NY. CA is forgotten.

Type 2: Keep history. Customer lived in CA from Jan-Jun, now lives in NY.

Why does this matter?

Imagine an order from March. Which address was valid then? CA.

If you overwrote with NY, you'd show the wrong state in your March reports.

SCD2 tracks versions:
```
customer_id | address | valid_from | valid_to   | is_current
101         | CA      | 2024-01-01 | 2024-06-30 | false
101         | NY      | 2024-07-01 | NULL       | true
```

Now you can time-travel. "What was the address on March 15th?"

This is how enterprises track history.

**CTA:** Config tomorrow

---

### Post 11.2 - SCD2 Configuration (Wednesday)

**Hook:**
Here's how to implement SCD2 without writing complex merge logic.

**Body:**
```yaml
- name: dim_customer
  read:
    connection: silver
    path: customers
  
  transformer: scd2
  params:
    target: gold.dim_customer
    keys: [customer_id]
    track_cols: [address, city, state, tier]
    effective_time_col: updated_at
    end_time_col: valid_to
    current_flag_col: is_current
  
  write:
    connection: gold
    table: dim_customer
    format: delta
    mode: overwrite
```

What this does:
1. Compare incoming data to existing dim_customer
2. If track_cols changed → close old row, insert new row
3. If no changes → do nothing
4. New customers → insert with is_current = true

The framework handles:
- Row versioning
- Date management
- Duplicate prevention

You just declare what to track.

I wrote a complete SCD2 guide as a LinkedIn article covering when to use it, how it works, and common gotchas. Link in comments.

**CTA:** Link to Article 11

---

### Post 11.3 - SCD2 Gotchas (Friday)

**Hook:**
3 ways SCD2 will blow up your table if you're not careful.

**Body:**
**1. Duplicate source data**

If your source has the same customer twice:
```
customer_id | address  | updated_at
101         | CA       | 2024-01-01 10:00:00
101         | CA       | 2024-01-01 10:00:01  ← duplicate!
```

SCD2 sees two "changes" → creates two versions.

Fix: Deduplicate BEFORE SCD2.

**2. Tracking volatile columns**

If you track `last_login_timestamp`:
- Customer logs in 50 times
- 50 versions created
- Table explodes

Fix: Only track slowly changing attributes (address, tier, status).

**3. Running without deduplication**

Each pipeline run processes ALL source data. If you don't dedupe, old records create new versions.

Fix: Use incremental loading or dedupe on keys.

SCD2 is powerful. But it amplifies bad data hygiene.

**CTA:** Dedupe first, always

---

## Week 12: Dimensions Advanced

### Post 12.1 - Unknown Member Pattern (Monday)

**Hook:**
Customer ID is null. The join fails. Your dashboard breaks.

**Body:**
This happens constantly:
- Source system has gaps
- Foreign key wasn't populated
- Data arrived out of order

Solution: Unknown member row.

Every dimension has a row with surrogate_key = 0:
```
customer_sk | customer_id | name
0           | UNKNOWN     | Unknown Customer
1           | abc123      | Alice
2           | def456      | Bob
```

When fact table has null/invalid customer_id, it joins to customer_sk = 0.

Dashboard still works. Query still runs. You just see "Unknown Customer."

```yaml
pattern:
  type: dimension
  params:
    unknown_member: true
```

One line. Problem solved.

Later, you can investigate:
```sql
SELECT COUNT(*) 
FROM fact_orders 
WHERE customer_sk = 0
```

Now you know how many orphans you have.

**CTA:** Defense against bad data

---

### Post 12.2 - Surrogate Key Generation (Wednesday)

**Hook:**
How do you generate 100,000 unique IDs efficiently?

**Body:**
Options:

**1. Hash-based (deterministic)**
```yaml
generate_surrogate_key:
  strategy: hash
  source_columns: [customer_id]
```
Same input → same key. Always.
Pro: Idempotent.
Con: Collisions possible (rare).

**2. Sequence-based (incremental)**
```yaml
generate_surrogate_key:
  strategy: sequence
```
1, 2, 3, 4...
Pro: Simple, small.
Con: Gaps if rows deleted.

**3. UUID**
```yaml
generate_surrogate_key:
  strategy: uuid
```
Random globally unique IDs.
Pro: No coordination needed.
Con: Large, not human-readable.

My default: **Sequence for dimensions** (small, fast joins), **Hash when I need idempotency**.

**CTA:** Pick based on use case

---

### Post 12.3 - Audit Columns (Friday)

**Hook:**
"When was this row loaded?" "From what source?"

**Body:**
Every dimension should have:
- `load_timestamp` - When the pipeline ran
- `source_system` - Where the data came from

```yaml
pattern:
  type: dimension
  params:
    audit:
      load_timestamp: true
      source_system: "ecommerce_api"
```

Result:
```
customer_sk | name  | load_timestamp      | source_system
1           | Alice | 2024-12-01 08:00:00 | ecommerce_api
```

Why?
1. **Debugging** - "This value looks wrong. When did it arrive?"
2. **Lineage** - "Which system is the source of truth?"
3. **SLA tracking** - "Is data fresh?"

Tiny addition. Massive debugging value.

Full guide on dimension table patterns including unknown members, keys, and auditing as a LinkedIn article. Link in comments.

**CTA:** Link to Article 12

---

## Week 13: Fact Tables Advanced

### Post 13.1 - Fact Table Grain (Monday)

**Hook:**
The grain determines everything about your fact table.

**Body:**
Grain = what does one row represent?

**Order grain:**
```
order_id | customer_sk | total_amount
```
One row per order. Can't see individual products.

**Order-item grain:**
```
order_id | product_sk | customer_sk | quantity | price
```
One row per product in each order. More detail.

**Daily aggregate grain:**
```
date_sk | product_sk | total_quantity | total_revenue
```
One row per product per day. Less storage, less detail.

Choose based on questions:
- "Total revenue by month?" → Any grain works
- "Which products were bought together?" → Need order-item grain
- "What time was each order placed?" → Need order grain

Grain is the first decision. Everything else follows.

**CTA:** Grain = detail level

---

### Post 13.2 - Calculated Measures (Wednesday)

**Hook:**
Don't store what you can calculate. Except when you should.

**Body:**
**Option 1: Calculate at query time**
```sql
SELECT quantity * unit_price as line_total
FROM fact_orders
```
Pro: Always correct.
Con: Slower queries.

**Option 2: Store pre-calculated**
```yaml
measures:
  - line_total: "quantity * unit_price"
  - margin: "(unit_price - cost) * quantity"
```
Pro: Fast queries.
Con: Must recalculate if formula changes.

My rule:
- **Store** stable calculations (price * quantity)
- **Calculate** complex/changing formulas at query time

For this warehouse:
```yaml
measures:
  - quantity
  - price
  - freight_value
  - total_amount: "price + freight_value"
```

Simple math = store it. Complex business logic = calculate it.

**CTA:** Balance storage vs compute

---

### Post 13.3 - Grain Validation (Friday)

**Hook:**
Duplicate grain rows corrupt your metrics. Catch them before they happen.

**Body:**
If your grain is [order_id, product_sk] and you have:
```
order_id | product_sk | quantity
1001     | 42         | 1
1001     | 42         | 1  ← DUPLICATE GRAIN
```

Your revenue will be 2x the actual value.

Validate grain before load:
```yaml
pattern:
  type: fact
  params:
    grain: [order_id, product_sk, seller_sk]
```

This checks: is each combination unique?

If not, pipeline fails with:
```
GrainValidationError: 23 duplicate grain rows found
```

Better to fail loudly than silently corrupt reports.

I wrote a complete guide on fact table design covering grain, measures, and validation as a LinkedIn article. Link in comments.

**CTA:** Link to Article 13

---

## Week 14: Aggregation Patterns

### Post 14.1 - Why Pre-Aggregate? (Monday)

**Hook:**
100 million fact rows. Dashboard needs to load in 3 seconds.

**Body:**
Options:

**1. Query raw facts**
```sql
SELECT month, SUM(revenue)
FROM fact_orders
GROUP BY month
```
Works. But 100M rows = slow.

**2. Pre-aggregate**
```sql
-- Pre-built table
SELECT month, revenue
FROM agg_monthly_sales
```
10 rows = instant.

Pre-aggregation trades storage for speed.

When to use:
- Dashboard queries are slow
- Same GROUP BY runs repeatedly
- Users can't wait for complex queries

When not to use:
- Exploratory analysis (need detail)
- Data changes frequently (aggregates go stale)

**CTA:** Aggregates = speed

---

### Post 14.2 - Aggregation Pattern (Wednesday)

**Hook:**
Here's how to build an aggregate table declaratively.

**Body:**
```yaml
- name: agg_daily_sales
  read:
    connection: gold
    path: fact_orders
  
  pattern:
    type: aggregation
    params:
      grain: [order_date_sk, product_sk]
      measures:
        - name: total_revenue
          expr: "SUM(total_amount)"
        - name: order_count
          expr: "COUNT(DISTINCT order_id)"
        - name: avg_order_value
          expr: "AVG(total_amount)"
  
  write:
    connection: gold
    table: agg_daily_sales
    format: delta
```

Input: 112,650 fact rows
Output: ~30,000 aggregate rows (one per date + product)

Queries on aggregates: <100ms
Queries on raw facts: 5-10 seconds

10-100x faster for common questions.

Full guide on pre-aggregation strategies for fast dashboards as a LinkedIn article. Link in comments.

**CTA:** Link to Article 14

---

### Post 14.3 - Aggregate Refresh Strategy (Friday)

**Hook:**
Aggregates go stale. Here's how to keep them fresh.

**Body:**
**Option 1: Full rebuild**
```yaml
mode: overwrite
```
Drop and recreate every run. Simple. Safe. Expensive.

**Option 2: Incremental**
```yaml
incremental:
  timestamp_column: order_date_sk
  merge_strategy: replace
```
Only update rows that changed. Faster. More complex.

**Option 3: Append + dedupe**
Add new aggregates, then dedupe on grain.

My recommendation:
- Daily aggregates → Full rebuild (cheap enough)
- Hourly/real-time → Incremental
- Monthly summaries → Full rebuild monthly

Start simple. Optimize when it hurts.

**CTA:** Start with full rebuild

---

## Week 15: Data Quality Patterns

### Post 15.1 - Contracts vs Validation (Monday)

**Hook:**
Contracts run BEFORE load. Validation runs AFTER load. Both matter.

**Body:**
**Contracts** = Pre-conditions
"Is this data safe to load?"
- Required columns exist?
- Types are correct?
- Values in expected range?

If contract fails → pipeline stops → no bad data enters.

**Validation** = Post-conditions
"Did the load produce good results?"
- Row counts make sense?
- Aggregates balance?
- No orphan keys?

If validation fails → alert → investigate.

```yaml
# Contracts (pre-load)
contracts:
  - type: not_null
    columns: [order_id]
  - type: row_count
    min: 1000

# Validation (post-load)
validation:
  rules:
    - type: freshness
      column: load_timestamp
      max_age: 24h
```

Both are guardrails. Use them.

**CTA:** Pre + post checks

---

### Post 15.2 - Quarantine Pattern (Wednesday)

**Hook:**
Bad rows don't have to kill your pipeline. Route them somewhere else.

**Body:**
Instead of failing on bad data:
1. Route bad rows to quarantine table
2. Load good rows normally
3. Review quarantine later

```yaml
pattern:
  type: fact
  params:
    orphan_handling: quarantine
    quarantine:
      connection: silver
      path: fact_orders_quarantine
      add_columns:
        _rejection_reason: true
        _rejected_at: true
```

Result:
- `fact_orders` - 112,000 good rows
- `fact_orders_quarantine` - 650 problem rows

Query quarantine:
```sql
SELECT _rejection_reason, COUNT(*)
FROM fact_orders_quarantine
GROUP BY _rejection_reason
```

Pipeline keeps running. You investigate at your pace.

Complete guide on data quality patterns covering contracts, validation, and quarantine as a LinkedIn article. Link in comments.

**CTA:** Link to Article 15

---

### Post 15.3 - Volume Drop Detection (Friday)

**Hook:**
Yesterday: 100,000 rows. Today: 500 rows. Something is wrong.

**Body:**
A sudden drop in data volume usually means:
- Source system failed
- Extract job broke
- Filter accidentally added

Catch it:
```yaml
contracts:
  - type: volume_drop
    threshold: 0.5  # Alert if < 50% of average
    baseline: last_7_days
```

If today's count is less than 50% of the 7-day average → pipeline fails.

Better to fail and investigate than to report incomplete data.

Real story: Source system had an outage. Sent 0 rows. Dashboard showed "$0 revenue."

CFO called. Not a good day.

Volume checks would have caught it before it hit the dashboard.

**CTA:** Volume = first warning sign

---

## Week 16: Incremental Patterns

### Post 16.1 - Why Incremental? (Monday)

**Hook:**
5 years of history. 500GB of data. Do you reload it all every day?

**Body:**
Full load:
- Read 500GB
- Process 500GB
- Write 500GB
- Time: hours, cost: $$$

Incremental load:
- Read 1GB (today's data)
- Process 1GB
- Merge with existing
- Time: minutes, cost: $

When to use:
- Large historical tables
- Daily/frequent refreshes
- Append-mostly data (events, transactions)

When NOT to use:
- Small tables (full load is fine)
- Complete snapshots (need full picture)
- First run (nothing to increment from)

**CTA:** Big data = incremental

---

### Post 16.2 - Watermark Pattern (Wednesday)

**Hook:**
"Only load rows newer than what I already have."

**Body:**
Watermark = highest value already loaded.

```yaml
read:
  incremental:
    mode: stateful
    column: updated_at
    watermark_lag: 2h  # Safety overlap
```

How it works:
1. Check: What's the max updated_at in target? → 2024-12-01 08:00
2. Subtract lag: 2024-12-01 06:00
3. Query source: WHERE updated_at >= '2024-12-01 06:00'
4. Load only those rows

Why the lag?
Late-arriving data. A row might be created at 07:55 but arrive at 08:05.
The 2-hour overlap catches stragglers.

After load, update watermark to new max.

Next run: Only new rows.

**CTA:** Watermarks save compute

---

### Post 16.3 - Merge vs Append (Friday)

**Hook:**
Incremental data can be appended or merged. Choose wisely.

**Body:**
**Append**
```yaml
mode: append
```
New rows added to table. No updates to existing.
Use for: Events, logs, immutable facts.

**Merge**
```yaml
transformer: merge
params:
  keys: [order_id]
```
New rows inserted. Existing rows updated.
Use for: Dimensions, correctable facts.

**Example:**

Order #1001 arrives on Day 1 with status "shipped".
Order #1001 updated on Day 2 with status "delivered".

Append: Two rows for #1001 (confusing)
Merge: One row for #1001 with latest status (correct)

Facts are usually append-only.
Dimensions are usually merge.

Aggregates depend on your refresh strategy.

Complete guide on incremental loading covering watermarks, merge, and append as a LinkedIn article. Link in comments.

**CTA:** Link to Article 16

---

## LinkedIn Articles for Phase 3

| Week | Article |
|------|---------|
| 11 | "SCD2 Complete Guide: When and How to Track History" |
| 12 | "Dimension Table Patterns: Unknown Members, Keys, and Auditing" |
| 13 | "Fact Table Design: Grain, Measures, and Validation" |
| 14 | "Pre-Aggregation Strategies for Fast Dashboards" |
| 15 | "Data Quality Patterns: Contracts, Validation, and Quarantine" |
| 16 | "Incremental Loading: Watermarks, Merge, and Append" |
