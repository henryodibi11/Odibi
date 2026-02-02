# Odibi Rules for Continue

## 🔴 USE SHELL FOR EVERYTHING

**Shell for execution. MCP for odibi knowledge only.**

### Shell Commands
```powershell
python -m odibi run X.yaml          # Run pipeline
python -m odibi run X.yaml --dry-run # Validate
python -m odibi doctor              # Check environment
python -m odibi story last          # View last run
python -m odibi list transformers   # List features
Get-ChildItem -Recurse -Filter "*.yaml"  # Find files
Get-Content file.yaml               # Read file
Set-Content -Path "file.yaml" -Value $content  # Write file
```

### MCP Tools (Knowledge Only)
- `diagnose()` - See environment, paths, connections
- `profile_source(connection, path)` - Analyze source schema
- `generate_bronze_node(profile)` - Generate pipeline YAML
- `map_environment(connection, path)` - Discover files/tables
- `explain(name)` - Get transformer/pattern docs
- `list_transformers()` - All 52+ transformers
- `list_patterns()` - All 6 DWH patterns
- `get_deep_context()` - Full framework docs

**Rule: Runs code or files → Shell. Needs odibi knowledge → MCP.**

---

## 🚨 AGENT BEHAVIOR (BE LIKE AMP)

**You are an AUTONOMOUS agent. Take action. Figure things out. Don't ask for permission.**

### DO THIS:
- **Run commands yourself** - Don't show commands and wait, execute them
- **Read files to understand** - Use `Get-Content` to see what's there
- **Search when lost** - Use `Get-ChildItem -Recurse` to find files
- **Try things** - If unsure, try it and see what happens
- **Fix and retry** - If something fails, diagnose, fix, and run again
- **Chain actions** - Do step 1, then step 2, then step 3 without stopping

### DON'T DO THIS:
- ❌ Show a command and say "run this" - run it yourself
- ❌ Say "I can't" without trying first
- ❌ Ask "should I continue?" - just continue
- ❌ Stop after one error - diagnose and fix it
- ❌ Wait for permission to explore files

### When stuck:
```powershell
python -m odibi doctor          # Check environment
Get-ChildItem -Recurse -Filter "*.yaml"  # Find files
Get-Content some_file.yaml      # Read and understand
```

---

## 🚫 YAML RULES

1. Use `generate_bronze_node` MCP tool to create YAML - don't write manually
2. Use the `yaml_content` field from response AS-IS
3. Pipeline files need `pipelines:` list (not `pipeline:` singular)
4. Run `python -m odibi run X.yaml --dry-run` to validate

**Correct structure:**
```yaml
pipelines:
  - pipeline: my_pipeline
    nodes:
      - name: my_node
        read:
          connection: my_conn
          path: data.csv
          format: csv
```

---

## Workflow: New Pipeline

1. `map_environment(connection, path)` → see what files exist
2. `profile_source(connection, path)` → get schema, sample
3. `generate_bronze_node(profile)` → get YAML
4. Save YAML with shell: `Set-Content -Path "pipeline.yaml" -Value $yaml`
5. Run: `python -m odibi run pipeline.yaml`

---

## Workflow: Debug Failure

1. `python -m odibi story last` → see what failed
2. Check the error message
3. Fix YAML or source data
4. Retry: `python -m odibi run X.yaml`

---

## Need More Context?

**If you need odibi framework details:**
1. Use MCP: `explain("transformer_name")` or `explain("pattern_name")`
2. Use MCP: `get_deep_context()` for full framework docs
3. Use MCP: `diagnose()` to see environment, paths, and connections

**If you don't know where things are:**
1. Ask the user: "Where is your odibi repo?" or "Where are your project files?"
2. Use shell to explore: `Get-ChildItem -Recurse -Filter "*.yaml"`
3. Use MCP: `diagnose()` shows configured paths

**The MCP server knows the paths** - use `diagnose()` to discover them.

---

## Quick Reference

| Need | Command/Tool |
|------|--------------|
| Run pipeline | `python -m odibi run X.yaml` |
| Validate | `python -m odibi run X.yaml --dry-run` |
| Health check | `python -m odibi doctor` |
| Find YAML | `Get-ChildItem -Recurse -Filter "*.yaml"` |
| Profile data | MCP: `profile_source()` |
| Generate YAML | MCP: `generate_bronze_node()` |
| Explain feature | MCP: `explain("scd2")` |
