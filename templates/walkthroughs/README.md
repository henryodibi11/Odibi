# Data Engineering Planning Walkthroughs

> "30 minutes of planning saves 3 hours of debugging."

These workbooks guide you through planning data pipelines **before writing code**. By the time you finish filling them out, writing YAML is just typing.

---

## Philosophy

**Plan first, code second.**

Traditional approach: Jump into code → hit edge cases → backtrack → discover missing requirements → refactor.

Walkthrough approach: Answer hard questions upfront → document decisions → write code once → ship.

---

## The Three Workbooks

| Layer | Purpose | Who Fills It |
|-------|---------|--------------|
| **Bronze** | Land raw data as-is | Business: B1 (what exists) / Engineers: B2-B5 (how to land it) |
| **Silver** | Clean & standardize one source at a time | Business: S1 (domains) / Engineers: S2-S4 (how to clean it) |
| **Gold** | Combine sources, build dimensions/facts, create KPIs | Business: G1, G8 (what questions to answer) / Engineers: G2-G7 (how to model it) |

---

## Layer Rules (Definitive)

### Bronze: Land Raw Data

| ✅ ALLOWED | ❌ NOT ALLOWED |
|-----------|----------------|
| Land data exactly as-is | Any transformation |
| Append mode only | Merge, upsert, overwrite |
| Add metadata (`_extracted_at`) | Filter or remove rows |
| Smart Read for incremental | Join any tables |
| Duplicates (expected) | Deduplicate |

### Silver: Clean One Source

| ✅ ALLOWED | ❌ NOT ALLOWED |
|-----------|----------------|
| Deduplicate | Join multiple source systems |
| Clean text, cast types | UNION multiple sources |
| Standardize codes | Build dimensions with SKs |
| Join with reference/lookup tables | SCD2 history tracking |
| Enrich via lookups | Business KPIs |

**Reference tables don't count as a "source system"** - they're supporting data.

### Gold: Combine & Model

| ✅ ALLOWED | ❌ NOT ALLOWED |
|-----------|----------------|
| Dimensions with surrogate keys | Silver-level cleaning |
| SCD2 history tracking | SCD2 on fact tables |
| UNION/JOIN across sources | Undefined grain on facts |
| Business KPIs | SCD2 without prior dedup |
| Aggregations | |

---

## How to Use: Step by Step

### Step 1: Bronze Layer

**Start here.** You can't clean data you haven't landed.

```
Bronze_Walkthrough.xlsx
├── B0_Overview      → Read first. Understand Bronze principles.
├── B1_Source_Inventory → List ALL source systems (business folks help here)
├── B2_Node_Design   → One row per Bronze node (technical details)
├── B3_Column_Mapping → Document what columns land
├── B4_Validation    → Plan quality checks
├── B5_Migration     → For legacy ETL migrations only
└── Reference        → Best practices & anti-patterns
```

**The rule:** Bronze = pure append, no transforms, no cleaning. Add metadata (`_extracted_at`). Duplicates are expected.

**When you're done:** Each row in B2 becomes one node in your Odibi YAML.

---

### Step 2: Silver Layer

**Only start Silver when Bronze is landing correctly.**

```
Silver_Walkthrough.xlsx
├── S0_Overview      → Read the "One-Source Test"
├── S1_Domain_Overview → Group nodes by business domain
├── S2_Node_Design   → One row per Silver node
├── S3_Column_Mapping → Source → Target with transformations
├── S4_Validation    → Ensure data is now trustworthy
└── Reference        → What belongs (and doesn't) in Silver
```

**The One-Source Test:** "Could this node run if only ONE source system existed?"
- YES → Silver ✓
- NO → Probably Gold

**Silver operations:** Deduplicate, clean text, cast types, standardize codes, enrich via lookups.

**What does NOT belong:** UNIONing multiple sources, building dimensions with surrogate keys, SCD2 tracking.

---

### Step 3: Gold Layer

**Only start Gold when Silver data is clean and deduplicated.**

```
Gold_Walkthrough.xlsx
├── G0_Overview      → Read the "Multi-Source Test"
├── G1_Business_Questions → What does the business need to know? (START HERE)
├── G2_Dimension_Design → Plan dimensions with SCD strategy
├── G3_Fact_Planning → Design fact tables (grain is critical)
├── G4_Node_Design   → One row per Gold node
├── G5_Column_Mapping → SQL transformation spec
├── G6_Validation    → Reconciliation checks
├── G7_FK_Relationships → Foreign key definitions
├── G8_Semantic_Metrics → Business metrics for self-service
└── Reference        → Best practices & anti-patterns
```

**Critical:** Fill G1 (Business Questions) FIRST. Gold exists to answer business questions - if you don't know the questions, you'll build the wrong things.

**Grain statement:** Every fact table needs one. "One row = one ___ per ___." If you can't say it, you haven't defined the grain.

---

## The Handoff: Walkthrough → YAML

Once your walkthrough is complete, translation is mechanical:

| Walkthrough Field | YAML Location |
|-------------------|---------------|
| Node_ID / Node_Name | `nodes.name` |
| Read_Connection | `read.connection` |
| Incremental_Mode | `incremental.mode` |
| Dedup_Keys | `params.keys` |
| Contract_Type | `validation.contracts[].type` |
| On_Failure | `validation.contracts[].on_failure` |

See `examples/walkthrough_test/` for a complete Bronze example.

---

## Who Fills What

### Business Stakeholders (Analysts, Managers, SMEs)

| Sheet | What to Fill |
|-------|--------------|
| B1_Source_Inventory | What data exists, who owns it, what it's for |
| G1_Business_Questions | What questions need answers (in plain English) |
| G8_Semantic_Metrics | How to calculate KPIs (in plain English) |

**Tip:** You don't need to know SQL. Describe things like you're explaining to a new employee.

### Data Engineers

| Sheet | What to Fill |
|-------|--------------|
| B2-B5 | How to land data technically |
| S2-S4 | How to clean and standardize |
| G2-G7 | How to model dimensions, facts, and relationships |

---

## Validation Fields Explained

Every validation sheet uses these fields:

| Field | Purpose | Values |
|-------|---------|--------|
| **Contract_Type** | What check to run | `not_null`, `unique`, `row_count`, `freshness`, `range`, `regex_match`, `accepted_values`, `custom_sql` |
| **On_Failure** | What happens when it fails | `error` (stop), `warn` (log), `filter` (remove rows), `quarantine` (save bad rows) |
| **Is_Quality_Gate** | Does this block the pipeline? | `Yes` / `No` |

---

## Common Mistakes

### Bronze
- ❌ Transforming or cleaning data (that's Silver's job)
- ❌ Filtering out "bad" rows (land everything, filter later)
- ❌ Forgetting `_extracted_at` metadata
- ❌ Using merge/upsert mode (always append)

### Silver
- ❌ Joining multiple source systems (that's Gold)
- ❌ Building dimensions with surrogate keys (that's Gold)
- ❌ Adding business logic beyond standardization

### Gold
- ❌ Doing Silver-level cleaning (data should arrive clean)
- ❌ Undefined grain on fact tables
- ❌ SCD2 without prior deduplication
- ❌ Building reports without knowing the business question

---

## Quick Start Checklist

- [ ] Open `Bronze_Walkthrough.xlsx`, read B0_Overview
- [ ] Complete the "BEFORE YOU START" checklist
- [ ] Fill B1 with business stakeholder help
- [ ] Fill B2 (one row per source to land)
- [ ] Fill B3 (column-level documentation)
- [ ] Fill B4 (validation contracts)
- [ ] Generate YAML from B2
- [ ] Build, test, deploy Bronze
- [ ] Repeat for Silver, then Gold

---

## Regenerating Workbooks

If you need to regenerate or customize the walkthroughs:

```bash
python scripts/create_de_walkthroughs.py
```

Output: `templates/walkthroughs/*.xlsx`

---

## Support

These walkthroughs are designed to work with the Odibi framework. See:
- `docs/guides/medallion_architecture.md` - Layer philosophy
- `docs/reference/yaml_schema.md` - YAML configuration reference
