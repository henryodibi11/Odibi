# How to Test Odibi MCP with Amp

## Setup (Done ✅)

Your `.vscode/settings.json` is already configured:
```json
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

---

## Test Plan

### 1. Restart Amp
- Reload VS Code window OR
- Start a new thread

This connects Amp to your MCP server and loads all 11 resources automatically.

### 2. Simple Test Prompts

**Try these SHORT prompts (no long instructions!):**

#### Test 1: Pattern Discovery
```
"What odibi patterns are available?"
```

**Expected:** I call `list_patterns()` and show 6 patterns

#### Test 2: Build Simple Pipeline  
```
"Build a customer dimension pipeline"
```

**Expected:**
- I call `apply_pattern_template`
- I use correct field names (read:/write:)
- I generate valid YAML
- I don't ask unnecessary questions

#### Test 3: Track Changes
```
"Track employee changes over time"
```

**Expected:**
- I recognize SCD2 pattern
- I call `apply_pattern_template(pattern="scd2", ...)`
- I include keys and tracked_columns

#### Test 4: Discovery
```
"Discover what's in my local connection"
```

**Expected:**
- I call `map_environment("local")`
- I show datasets found

---

## What Success Looks Like

### ✅ Good Response
```
I'll build a customer dimension pipeline.

[Generated YAML using dimension pattern]

pipelines:
  - pipeline: dimension_customer
    nodes:
      - name: dimension_node
        read:           ← Correct field name!
          connection: source_db
          format: sql
          table: dbo.Customer
        write:          ← Correct field name!
          connection: warehouse
          format: delta
          path: gold/dim_customer
          mode: overwrite
        transformer: dimension
        params:
          natural_key: customer_id
          surrogate_key: customer_sk

Save this to dim_customer.yaml and run with:
python -m odibi run dim_customer.yaml
```

### ❌ Bad Response (Shouldn't happen!)
```
Here's the YAML:

nodes:
  - name: customer
    source:           ← WRONG! Should be "read:"
      connection: db
    sink:             ← WRONG! Should be "write:"
      path: out.csv
```

---

## Verification Checklist

When I respond, verify:
- [ ] I used MCP tools (not manual YAML writing)
- [ ] I used `read:` and `write:` (not source:/sink:)
- [ ] I selected correct pattern for use case
- [ ] I used exact transformer names (if transformations needed)
- [ ] YAML is valid (no hallucinated fields)

---

## If Something Goes Wrong

### I ask too many questions
**Problem:** Not being proactive  
**Fix:** Resources need stronger "use defaults" instruction

### I use wrong field names (source:/sink:)
**Problem:** Not reading Critical Context  
**Fix:** Resources aren't loading or not prioritized

### I don't call tools
**Problem:** Tools not connected  
**Fix:** Check MCP server configuration

---

## Expected Outcome

**After restart, you should be able to:**
1. Say "Build a customer dimension" (6 words)
2. I generate valid YAML immediately
3. No long explanations needed from you
4. 100% success rate

**This proves MCP resources work!**

---

## Debug Commands

If it's not working:

```python
# Check resources load
import asyncio
from odibi_mcp import server
resources = asyncio.run(server.list_resources())
print(f"Resources: {len(resources)}")
print([r.name for r in resources])

# Check tools load
tools = asyncio.run(server.list_tools())
print(f"Tools: {len(tools)}")
```

Should show:
- 11 resources
- 19 tools

---

## Success Criteria

✅ **You write:** "Build X" (short prompt)  
✅ **I respond:** Valid YAML (using tools)  
✅ **No errors:** Correct field names, patterns  
✅ **You save:** File and run it  
✅ **It works:** Pipeline executes successfully  

**That's the goal. Test it now!** 🎯
