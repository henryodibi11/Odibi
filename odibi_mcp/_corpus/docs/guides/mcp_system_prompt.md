# System Prompt for AI Agents Using Odibi MCP

**For:** Setting up AI assistants (Amp, Cline, Continue)  
**Purpose:** Short, effective prompts that leverage MCP resources

---

## The Problem We Solved

**Before:** You had to write 500+ line prompts teaching AI about odibi  
**After:** AI has auto-attached resources + 15 smart tools

---

## Recommended System Prompt (Short!)

```
You are an expert data engineer helping with odibi pipelines.

CRITICAL RULES:
1. NEVER write YAML manually - always use MCP tools
2. Call list_patterns() before choosing a pattern
3. Call list_transformers() before using transformers
4. Use apply_pattern_template for simple pipelines (80% of cases)
5. Read tool responses - they include next steps

Available MCP resources have complete odibi documentation.
When unsure, reference the resources before answering.

For common patterns:
- Customer/product tables → dimension pattern
- Track history → scd2 pattern
- Daily transactions → fact pattern
- Calendar table → date_dimension pattern
- Summaries → aggregation pattern
```

**That's it!** The MCP resources handle the rest.

---

## Why This Works

### Auto-Attached Resources
AI automatically has access to:
- ✅ ODIBI_DEEP_CONTEXT.md (2,200 lines of framework docs)
- ✅ Pattern guides (dimension, fact, scd2, etc.)
- ✅ Cheat sheet (quick reference)
- ✅ AI Agent Guide (decision trees, workflows)

**You don't need to paste docs!**

### Self-Documenting Tools
Every tool response includes:
- `next_step`: What to do next
- `ready_for`: Pre-filled params for next tool
- `suggestions`: Helpful hints
- `errors`: Structured with fixes

**AI knows what to do next automatically!**

### Enum Constraints
Can't use wrong values:
- Patterns: dimension | fact | scd2 | merge | aggregation | date_dimension
- Modes: overwrite | append | upsert | append_once | merge
- Formats: sql | csv | parquet | json | delta

**Impossible to make mistakes!**

---

## Advanced: Custom Instructions Per Assistant

### For Amp
Amp automatically loads MCP resources. Just add to workspace:

```json
// .vscode/settings.json
{
  "amp.mcpServers": {
    "odibi": {
      "command": "python",
      "args": ["-m", "odibi_mcp.server"],
      "cwd": "d:/odibi"
    }
  }
}
```

No system prompt needed - Amp reads resources automatically!

### For Cline
Add to Cline custom instructions:

```
When working with odibi:
- Use MCP tools, never write YAML manually
- Call list_patterns() to see available patterns
- Resources have complete documentation
```

### For Continue
Similar to Cline - short reminder to use tools.

---

## Example Conversations

### Good (Uses Tools)
```
User: "Build a customer dimension"

AI: I'll use the dimension pattern.
    [calls apply_pattern_template]
    Generated pipeline YAML:
    [shows YAML]

    Save this to dim_customer.yaml and run with:
    python -m odibi run dim_customer.yaml
```

### Bad (Writes YAML)
```
User: "Build a customer dimension"

AI: Here's the YAML:
    ```yaml
    nodes:
      - name: dim_customer
        source:  # ← WRONG FIELD!
    ```
```

---

## Tool Chaining Examples

### Example 1: Unknown Data → Pipeline
```
User: "I have a SQL table dbo.Employee, not sure what to do"

AI:
1. profile_source("db", "dbo.Employee")
   → Gets schema, row count, candidate keys

2. suggest_pipeline(profile)
   → Suggests: "dimension" (75% confidence)
   → Provides ready_for params

3. apply_pattern_template(**ready_for)
   → Generates valid YAML
```

### Example 2: Bulk Ingestion
```
User: "Load all tables from Sales schema"

AI:
1. map_environment("sales_db")
   → Finds: Orders, OrderLines, Customers, Products

2. create_ingestion_pipeline(
     pipeline_name="bronze_sales",
     source_connection="sales_db",
     tables=[...all 4 tables...]
   )

   → Generates 4-node parallel ingestion pipeline
```

---

## Troubleshooting

### AI generates invalid YAML
**Problem:** Not using tools  
**Fix:** Remind: "Use apply_pattern_template, don't write YAML"

### AI invents transformer names
**Problem:** Didn't call list_transformers  
**Fix:** "Call list_transformers() first to see what's available"

### AI uses wrong pattern
**Problem:** Didn't understand use case  
**Fix:** "Read the pattern guides in resources, or use suggest_pipeline"

---

## Testing Your Setup

Ask AI:
```
"List available odibi patterns"
```

**Expected:** AI calls `list_patterns()` tool and shows 6 patterns

**If AI writes a list manually:** Tools aren't connected properly

---

## Key Insight

**The MCP tools are designed for cheap models (GPT-4o-mini).**

If GPT-4o-mini can use them perfectly (proven in tests), ANY model can:
- GPT-4
- Claude Sonnet/Opus
- Gemini
- Local models

**Your job:** Just remind AI to use tools, not write YAML.

**AI's job:** Call tools, show results, explain next steps.

---

## Summary

### You Don't Need to Write:
- ❌ Long prompts explaining odibi
- ❌ Examples of YAML structure
- ❌ Field name lists
- ❌ Pattern documentation

### AI Has Automatically:
- ✅ Complete odibi docs (via resources)
- ✅ Pattern guides
- ✅ Tool descriptions with examples
- ✅ Decision trees
- ✅ Enum constraints

### Result:
**Your prompts:** "Build a customer dimension"  
**AI:** Uses tools correctly, generates valid YAML, 100% first-try success

**That's the goal!** And we achieved it. ✅
