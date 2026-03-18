# AI Agent Guide for Odibi

**For:** AI Assistants (Amp, Cline, Continue, Claude)  
**Purpose:** Quick reference to help AI agents work effectively with odibi

---

## Core Philosophy

**Odibi is declarative.** You describe WHAT you want, not HOW to do it.

**Golden Rule:** Never write YAML manually. Always use MCP tools to generate it.

---

## Decision Tree: Which Tool to Use?

### User wants to build a pipeline

**Ask yourself:**

1. **Is it a common pattern?** (dimension, fact, scd2, merge, aggregation, date_dimension)
   - → Use `apply_pattern_template` (ONE call)
   - Example: Customer dimension, employee history, daily orders

2. **Is it multi-node with complex dependencies?**
   - → Use Phase 2 builder (create_pipeline → add_node → configure_* → render)
   - Example: Bronze → Silver → Gold with joins

3. **Is it bulk ingestion** (many tables)?
   - → Use `create_ingestion_pipeline` (ONE call)
   - Example: Load 10 tables from SQL database

4. **Do they have data to profile first?**
   - → Start with `profile_source` → `suggest_pipeline` → `apply_pattern_template`
   - This auto-suggests the right pattern!

---

## Common Patterns

### Pattern 1: Customer Dimension
**When:** Reference/lookup data that changes infrequently  
**Tool:** `apply_pattern_template`  
**Pattern:** dimension

```
User: "Build a customer dimension from dbo.Customer"

You:
1. list_patterns() → see dimension requires natural_key, surrogate_key
2. apply_pattern_template(
     pattern="dimension",
     pipeline_name="dim_customer",
     source_table="dbo.Customer",
     natural_key="customer_id",
     surrogate_key="customer_sk"
   )
3. Done! Valid YAML generated.
```

### Pattern 2: Track Changes Over Time
**When:** Need full history of changes  
**Tool:** `apply_pattern_template`  
**Pattern:** scd2

```
User: "Track employee changes over time"

You:
1. Recognize: "track changes" = SCD2 pattern
2. apply_pattern_template(
     pattern="scd2",
     keys=["employee_id"],
     tracked_columns=["name", "dept", "salary"]
   )
```

### Pattern 3: Daily Transactions
**When:** High-volume event/transaction data  
**Tool:** `apply_pattern_template`  
**Pattern:** fact

```
User: "Load daily orders"

You:
pattern="fact",
keys=["order_id"],
source_query="SELECT * FROM Orders WHERE order_date >= GETDATE()"
```

---

## Workflow Examples

### Workflow 1: Quick Pipeline (2 calls)
```
list_patterns()
→ apply_pattern_template(...)
→ Done!
```

### Workflow 2: Smart Suggestion (3 calls)
```
profile_source(connection, table)
→ suggest_pipeline(profile)  
→ apply_pattern_template(ready_for params)
→ Done!
```

### Workflow 3: Complex Multi-Node (8+ calls)
```
create_pipeline(name)
→ add_node("bronze")
→ configure_read(...)
→ configure_write(...)
→ add_node("silver", depends_on=["bronze"])
→ configure_*
→ render_pipeline_yaml()
→ Done!
```

---

## Common Mistakes to AVOID

### ❌ DON'T invent field names
```yaml
# WRONG
nodes:
  - name: test
    source:          # ← NO! Use "read:"
    sink:            # ← NO! Use "write:"
```

**Instead:** Let tools generate YAML. They use correct field names.

### ❌ DON'T guess transformer names
```yaml
# WRONG
transform:
  steps:
    - function: dedup  # ← Doesn't exist!
```

**Instead:** Call `list_transformers()` first, use exact names.

### ❌ DON'T write YAML strings
```python
# WRONG
yaml = """
pipelines:
  - pipeline: test
    ...
"""
```

**Instead:** Use `apply_pattern_template` or builder tools.

---

## Pattern Selection Guide

| User Says... | Pattern to Use | Required Params |
|--------------|----------------|-----------------|
| "customer/product table" | dimension | natural_key, surrogate_key |
| "track history/changes" | scd2 | keys, tracked_columns |
| "daily orders/events" | fact | keys (optional) |
| "calendar/date table" | date_dimension | start_date, end_date |
| "monthly summary" | aggregation | grain, measures |
| "merge/upsert" | merge | keys, target |

---

## Response Pattern

**Always include:**
1. ✅ What you did
2. ✅ The YAML generated
3. ✅ Next steps ("Save this to file.yaml and run with...")

**Example:**
```
I've generated a customer dimension pipeline using the dimension pattern.

[Generated YAML here]

Next steps:
1. Save this to dim_customer.yaml
2. Run: python -m odibi run dim_customer.yaml
```

---

## When User is Confused

**If they ask "how do I...?":**
1. Check if there's a pattern for it (`list_patterns`)
2. Suggest the right pattern with explanation
3. Show example call
4. Ask if they want you to generate it

**Example:**
```
User: "How do I track employee changes?"

You: "Use the SCD2 pattern - it tracks full history of changes.
     Want me to generate it? I'll need:
     - Keys (employee_id)
     - Columns to track (name, dept, salary)"
```

---

## Resources Available

You have automatic access to:
- **Odibi Deep Context** (2,200+ lines) - Complete framework docs
- **Cheat Sheet** - Quick reference
- **Pattern Guides** - Dimension, Fact, SCD2, etc.
- **MCP Setup Guide** - How tools work

**Read these when:**
- User asks "how does X work?"
- You're unsure about a pattern
- You need examples

---

## Key Reminders

1. **Patterns solve 80% of use cases** - Start with apply_pattern_template
2. **Tools prevent mistakes** - Enum constraints, validation built-in
3. **Errors have fixes** - Structured errors tell you exactly what to do
4. **Profile first** - When exploring new data, profile_source gives you everything
5. **Never guess** - list_transformers, list_patterns show what's available

---

## Success Metrics

**Good agent behavior:**
- ✅ Uses tools for discovery (doesn't guess)
- ✅ Generates valid YAML first try (100% success rate proven)
- ✅ Explains what pattern is appropriate and why
- ✅ Shows next steps clearly

**Bad agent behavior:**
- ❌ Writes YAML strings manually
- ❌ Invents transformer names
- ❌ Uses wrong field names (source/sink)
- ❌ Doesn't explain pattern choice

---

## Quick Reference

### Most Common Call Pattern
```
User: "Build X pipeline"

You:
1. list_patterns() if unsure which pattern
2. apply_pattern_template(pattern=X, ...)
3. Return YAML + next steps

Done in 2 calls!
```

### When to Use What

- **One source, one target, fits a pattern** → apply_pattern_template
- **Multiple nodes, custom logic** → Phase 2 builder
- **Many tables to ingest** → create_ingestion_pipeline
- **Exploring unknown data** → profile_source → suggest_pipeline
- **Validating existing YAML** → validate_pipeline

---

**Remember:** The tools are designed so cheap models (GPT-4o-mini) can use them perfectly. If you're struggling, check if you're using the right tool for the use case!
