# Business Analyst Learning Journey

---

## 📌 Who Is This For?

**Business analysts, product managers, and stakeholders** who need to:
- Understand what data pipelines are doing
- Read and interpret Data Story reports
- Validate data quality and spot issues
- Communicate effectively with data engineers

**No coding or data engineering experience required.**

---

## ⏱️ Time to Complete

**2-3 hours** (can be broken into 30-minute sessions)

---

## 🎯 Learning Outcomes

By the end of this journey, you will be able to:

✅ Explain what Odibi is and what it's NOT  
✅ Open and interpret a Data Story HTML report  
✅ Understand star schemas (dimensions and facts)  
✅ Identify quality issues from validation results  
✅ File high-signal tickets with clear context  
✅ Distinguish between contracts and validation tests  

---

## 📋 Prerequisites

- **None!** Just a browser and curiosity.
- Optional: Basic familiarity with spreadsheets/CSV files

---

## 📚 Learning Modules

### Module 1: What Is Odibi? (15 min)

#### 📖 Read
- [Philosophy](../philosophy.md) - Focus on "Why Odibi Exists" and "What Odibi Is/Is NOT"
- [Playbook Introduction](../playbook/README.md) - Scan "Most Common Flows"

#### ✋ Do
Answer these questions:
1. Is Odibi a scheduler like Airflow? (Yes/No)
2. What does "Declarative over Imperative" mean in your own words?
3. What are the 5 Laws? Pick one that resonates with you.

#### ✅ Verify
- [ ] You can explain "Raw is Sacred" to a colleague
- [ ] You know what tools to use FOR scheduling (Airflow, Databricks Workflows)

---

### Module 2: Reading a Data Story (30 min)

#### 📖 Read
- [How to Read a Data Story](../guides/how_to_read_a_story.md) ← **New guide**
- [Golden Path - Step 4](../golden_path.md#step-4-see-what-happened)

#### 👀 Watch
- Video: "Data Story Walkthrough" (7 min) ← **Coming soon**

#### ✋ Do
Download a sample Data Story:
```bash
# If you have Odibi installed
odibi run docs/examples/canonical/runnable/01_hello_world.yaml
odibi story last

# Or download pre-generated Story
curl -O https://github.com/henryodibi11/Odibi/raw/main/docs/examples/stories/sample_story.html
open sample_story.html
```

Answer these questions by inspecting the Story:
1. How many rows were read? How many were written?
2. Did the schema change between read and write?
3. How long did the pipeline take to run?
4. What SQL was executed (if any)?

#### ✅ Verify
- [ ] You can find the row count in the Story
- [ ] You can see the data sample (first 10 rows)
- [ ] You understand the "Lineage" section (input → node → output)

---

### Module 3: Star Schemas 101 (30 min)

#### 📖 Read
- [Dimension Pattern](../patterns/dimension.md) - Read "What is a Dimension?" section
- [Fact Pattern](../patterns/fact.md) - Read "What is a Fact?" section
- [SCD2 Pattern Overview](../patterns/scd2.md) - Just the introduction

#### 👀 Watch
- Video: "Star Schema in 5 Minutes" ← **Coming soon**

#### ✋ Do
Look at this table and classify each as Dimension or Fact:

| Table | Dimension or Fact? |
|-------|-------------------|
| `customer` (id, name, email, city) | ? |
| `sales` (order_id, customer_id, product_id, amount, date) | ? |
| `product` (id, name, category, price) | ? |
| `daily_revenue` (date, total_revenue, order_count) | ? |

#### ✅ Verify
- [ ] `customer` and `product` are **Dimensions** (who/what)
- [ ] `sales` is a **Fact** (measures/transactions)
- [ ] `daily_revenue` is an **Aggregated Fact** (pre-summarized)

---

### Module 4: Data Quality Signals (30 min)

#### 📖 Read
- [Quality Gates](../features/quality_gates.md)
- [Contracts vs Validation Tests](../validation/README.md)

#### 👀 Visual
```
Contracts (Before)           Validation (After)
      ↓                             ↓
   [Source] → [Transform] → [Validate] → [Write]
      ↓                             ↓
   "Is this safe       "Is the output
    to process?"        what we expect?"
```

**Key Difference:**
- **Contracts** = Input checks (fail early, before wasting compute)
- **Validation** = Output checks (verify transformations worked)

#### ✋ Do
Read this YAML snippet and answer:

```yaml
contracts:
  - type: not_null
    columns: [customer_id, order_date]
  - type: freshness
    column: order_date
    max_age: "24h"

validation:
  tests:
    - type: unique
      columns: [order_id]
    - type: row_count
      min: 1
  gate:
    on_failure: fail  # or "warn"
```

Questions:
1. What happens if `customer_id` is NULL in the source?
2. What happens if the output has duplicate `order_id`?
3. If `on_failure: warn`, does the pipeline stop?

#### ✅ Verify
- [ ] Contracts check **source** data before processing
- [ ] Validation tests check **output** data after transformation
- [ ] `on_failure: fail` stops the pipeline; `warn` logs but continues

---

### Module 5: Hands-On Story Analysis (30 min)

#### ✋ Do
Use the sample Data Story from Module 2 (or generate a new one from Example 4):

```bash
odibi run docs/examples/canonical/runnable/04_fact_table.yaml
odibi story last
```

Answer these questions by reading the Story:

**Lineage Section:**
1. Draw the dependency graph (which nodes depend on which?)
2. Which node ran first? Which ran last?

**Data Profile Section:**
3. How many rows are in `fact_sales`?
4. Are there any NULL values in the `customer_sk` column?

**Validation Section:**
5. Did any validation tests fail?
6. If yes, what was the error message?

**Explanation Section:**
7. Does the `fact_sales` node have business logic documented?

#### ✅ Verify
Share your answers with a colleague or data engineer. Can you explain:
- Why dimensions run before facts?
- What "orphan handling" means in the Fact pattern?

---

## 🏆 Capstone Project

**Scenario:** A Data Story shows a failed validation test. Your job is to diagnose the issue and file a ticket.

### Instructions

1. Download this Story: [`failed_validation_story.html`](../examples/stories/failed_validation_story.html)
2. Open it and find the failed validation test
3. Write a ticket with:
   - **Node name** that failed
   - **Validation test** that failed (e.g., "unique check on order_id")
   - **Error details** (how many duplicates? which IDs?)
   - **Business impact** (why does this matter?)
   - **Suggested fix** (even if not technical)

### Example Ticket

```
Title: Duplicate order_ids in fact_sales

Node: fact_sales
Validation: unique check on [order_id]
Error: Found 5 duplicate order_ids (12345, 12346, 12347, 12348, 12349)

Business Impact:
Revenue will be double-counted for these orders, inflating KPIs.

Suggested Fix:
Investigate why source data has duplicates. Should we deduplicate before
the fact table? Or is this a data entry error that needs to be fixed upstream?

Data Story: [link to .html file]
```

#### ✅ Verify
- [ ] Your ticket includes the node name, test name, and error details
- [ ] You explained the business impact (not just technical details)
- [ ] You attached or linked the Data Story HTML

---

## ➡️ Next Steps

You've completed the Business Analyst journey! Here's where to go next:

### Continue Learning
- [Glossary](../reference/glossary.md) - Define terms like "HWM," "SCD2," "quarantine"
- [Dimensional Modeling Tutorial](../tutorials/dimensional_modeling/01_introduction.md) - Deeper dive into star schemas

### Work with Your DE Team
- [Request a Metric](../guides/requesting_metrics.md) ← **New guide**
- [How to File Data Quality Issues](../guides/filing_issues.md) ← **New guide**

### Advanced (Optional)
- [Jr Data Engineer Journey](junior-data-engineer.md) - Learn to run pipelines yourself

---

## 📣 Share Your Success

Completed the capstone? **We want to hear from you!**

- Share your first ticket in [GitHub Discussions](https://github.com/henryodibi11/Odibi/discussions)
- Tag us on LinkedIn with #OdibiLearning

---

**Questions?** [Troubleshooting Guide](../troubleshooting.md) | [FAQ](../guides/faq.md)
