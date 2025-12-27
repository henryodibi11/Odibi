# Data Engineering Planning Walkthroughs

> "30 minutes of planning saves 3 hours of debugging."

Professional-grade Excel workbooks that guide you through planning data pipelines **before writing code**. By the time you finish filling them out, writing YAML is just typing.

---

## Download the Walkthroughs

| Workbook | Sheets | Download |
|----------|--------|----------|
| **Bronze** | 7 sheets (B0-B5 + Reference) | [`Bronze_Walkthrough.xlsx`](https://github.com/henryodibi11/Odibi/raw/main/templates/walkthroughs/Bronze_Walkthrough.xlsx) |
| **Silver** | 6 sheets (S0-S4 + Reference) | [`Silver_Walkthrough.xlsx`](https://github.com/henryodibi11/Odibi/raw/main/templates/walkthroughs/Silver_Walkthrough.xlsx) |
| **Gold** | 10 sheets (G0-G8 + Reference) | [`Gold_Walkthrough.xlsx`](https://github.com/henryodibi11/Odibi/raw/main/templates/walkthroughs/Gold_Walkthrough.xlsx) |

Or regenerate them locally:

```bash
python scripts/create_de_walkthroughs.py
```

---

## Philosophy

**Plan first, code second.**

| Traditional Approach | Walkthrough Approach |
|---------------------|---------------------|
| Jump into code | Answer hard questions upfront |
| Hit edge cases | Document decisions |
| Backtrack | Write code once |
| Discover missing requirements | Ship |

---

## The Three Layers

```
┌─────────────────────────────────────────────────────────────┐
│  GOLD    Business sees this. Dimensions, Facts, KPIs.       │
│          Combines sources. Applies business logic.          │
├─────────────────────────────────────────────────────────────┤
│  SILVER  Clean & standardize ONE source at a time.          │
│          Deduplicate, cast types, map codes.                 │
├─────────────────────────────────────────────────────────────┤
│  BRONZE  Land raw data as-is. No transforms.                 │
│          Pure append. Add metadata. Duplicates expected.     │
└─────────────────────────────────────────────────────────────┘
```

---

## Who Fills What

### Business Stakeholders

| Layer | Sheet | What to Fill |
|-------|-------|--------------|
| Bronze | B1_Source_Inventory | What data exists, who owns it, what it's for |
| Gold | G1_Business_Questions | What questions need answers (plain English) |
| Gold | G8_Semantic_Metrics | How to calculate KPIs (plain English) |

!!! tip "No SQL Required"
    Describe things like you're explaining to a new employee.

### Data Engineers

| Layer | Sheets | What to Fill |
|-------|--------|--------------|
| Bronze | B2-B5 | How to land data technically |
| Silver | S2-S4 | How to clean and standardize |
| Gold | G2-G7 | How to model dimensions, facts, relationships |

---

## Step-by-Step Usage

### Step 1: Bronze Layer

**Start here.** You can't clean data you haven't landed.

```
Bronze_Walkthrough.xlsx
├── B0_Overview        → Read first. Understand Bronze principles.
├── B1_Source_Inventory → List ALL source systems
├── B2_Node_Design     → One row per Bronze node
├── B3_Column_Mapping  → Document what columns land
├── B4_Validation      → Plan quality checks
├── B5_Migration       → For legacy ETL migrations only
└── Reference          → Best practices & anti-patterns
```

**The Bronze Rule:** Pure append, no transforms, no cleaning. Add metadata (`_extracted_at`). Duplicates are expected.

**Odibi Patterns:** `append`, `rolling_window`, `stateful`

---

### Step 2: Silver Layer

**Only start when Bronze is landing correctly.**

```
Silver_Walkthrough.xlsx
├── S0_Overview        → Read the "One-Source Test"
├── S1_Domain_Overview → Group nodes by business domain
├── S2_Node_Design     → One row per Silver node
├── S3_Column_Mapping  → Source → Target with transformations
├── S4_Validation      → Ensure data is trustworthy
└── Reference          → What belongs (and doesn't)
```

**The One-Source Test:** "Could this node run if only ONE source system existed?"

- **YES** → Silver ✓
- **NO** → Probably Gold

!!! note "Reference Tables Are Allowed"
    The One-Source Test refers to *business source systems*, not reference data.
    
    **Silver CAN join with:**
    
    - Reference/lookup tables (code mappings, static lists)
    - Dimension lookups for enrichment (product_code → product_name)
    - Self-joins within the same source
    
    **Silver should NOT join:**
    
    - Multiple business source systems (SAP + Salesforce → Gold)
    - Cross-source conformed dimensions → Gold

**Odibi Patterns:** `deduplicate`, `merge`, `clean_text`, `validate_and_flag`

---

### Step 3: Gold Layer

**Only start when Silver data is clean and deduplicated.**

```
Gold_Walkthrough.xlsx
├── G0_Overview        → Read the "Multi-Source Test"
├── G1_Business_Questions → START HERE - what does business need?
├── G2_Dimension_Design → Plan dimensions with SCD strategy
├── G3_Fact_Planning   → Design fact tables (grain is critical)
├── G4_Node_Design     → One row per Gold node
├── G5_Column_Mapping  → SQL transformation spec
├── G6_Validation      → Reconciliation checks
├── G7_FK_Relationships → Foreign key definitions
├── G8_Semantic_Metrics → Business metrics for self-service
└── Reference          → Best practices & anti-patterns
```

!!! warning "Grain is Critical"
    Every fact table needs a grain statement: "One row = one ___ per ___."
    If you can't say it, you haven't defined the grain.

**Odibi Patterns:** `Dimension`, `SCD2`, `DateDimension`, `Fact`, `Aggregation`, `union`, `join`

---

## Walkthrough → YAML Translation

Once your walkthrough is complete, translation is mechanical:

| Walkthrough Field | YAML Location |
|-------------------|---------------|
| `Node_ID` / `Node_Name` | `nodes.name` |
| `Read_Connection` | `read.connection` |
| `Read_Format` | `read.format` |
| `Incremental_Mode` | `incremental.mode` |
| `Incremental_Column` | `incremental.column` |
| `Dedup_Keys` | `params.keys` |
| `Write_Mode` | `write.mode` |
| `Contract_Type` | `validation.contracts[].type` |
| `On_Failure` | `validation.contracts[].on_failure` |
| `Is_Quality_Gate` | `validation.contracts[].is_quality_gate` |

---

## Validation Fields

Every validation sheet uses these fields:

| Field | Purpose | Values |
|-------|---------|--------|
| `Contract_Type` | What check to run | `not_null`, `unique`, `row_count`, `freshness`, `range`, `regex_match`, `accepted_values`, `custom_sql` |
| `On_Failure` | What happens when it fails | `error` (stop), `warn` (log), `filter` (remove rows), `quarantine` (save bad rows) |
| `Is_Quality_Gate` | Does this block the pipeline? | `Yes` / `No` |

---

## Common Mistakes

### Bronze

| Mistake | Why It's Wrong |
|---------|----------------|
| Transforming or cleaning data | That's Silver's job |
| Filtering out "bad" rows | Land everything, filter later |
| Forgetting `_extracted_at` | You lose audit trail |
| Using merge/upsert mode | Always append in Bronze |

### Silver

| Mistake | Why It's Wrong |
|---------|----------------|
| Joining multiple sources | That's Gold |
| Building dimensions with SKs | That's Gold |
| Adding business logic | Keep it to standardization only |

### Gold

| Mistake | Why It's Wrong |
|---------|----------------|
| Doing Silver-level cleaning | Data should arrive clean |
| Undefined grain on facts | You'll get duplicates or gaps |
| SCD2 without prior dedup | History will be wrong |
| Building without business questions | You'll build the wrong thing |

---

## Quick Start Checklist

- [ ] Download or regenerate the workbooks
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

## Example: Bronze Node

**Scenario:** Land Production Orders from SAP ERP

### Filled Walkthrough (B2_Node_Design)

| Field | Value |
|-------|-------|
| Node_ID | BRZ_001 |
| Node_Name | bronze_production_orders |
| Read_Connection | sap_erp_prod |
| Read_Format | sql |
| Read_Table_or_Path | SAPPR1.dbo.AFKO |
| Incremental_Mode | rolling_window |
| Incremental_Column | AEDAT |
| Lookback | 3 |
| Lookback_Unit | day |
| Write_Path | bronze/production/orders |

### Generated YAML

```yaml
pipelines:
  - pipeline: bronze_production
    nodes:
      - name: bronze_production_orders
        read:
          connection: sap_erp_prod
          format: sql
          table: SAPPR1.dbo.AFKO
        incremental:
          mode: rolling_window
          column: AEDAT
          lookback: 3
          lookback_unit: day
        write:
          connection: delta_lake
          path: bronze/production/orders
          mode: append
```

**The walkthrough made this trivial.**

---

## See Also

- [Medallion Architecture](medallion_architecture.md) - Layer philosophy
- [YAML Schema Reference](../reference/yaml_schema.md) - Configuration details
- [Validation Contracts](../validation/contracts.md) - Quality check options
- [Best Practices](best_practices.md) - General guidance
