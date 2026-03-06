# Odibi Agent Rules (Minimal)

Be action-oriented. Execute tasks directly.

---

## USEFUL TOOLS

### Data Exploration → odibi MCP
```
map_environment(conn, path)        ← Discover files/tables
profile_source(conn, path)         ← Get schema, stats, suggestions
list_patterns()                    ← See 6 available patterns
list_transformers(search="...")    ← Find transformers
```

### Pipeline Building → odibi MCP
```
apply_pattern_template(...)        ← Generate simple pipeline (1 call)
create_pipeline(name)              ← Start multi-node builder
add_node(session_id, name)         ← Add nodes incrementally
configure_read/write/transform     ← Configure nodes
render_pipeline_yaml(session_id)   ← Finalize to YAML
validate_pipeline(yaml)            ← Check for errors
```

### CRITICAL: Never Write YAML Manually!
- ❌ DON'T write: `nodes:`, `read:`, `write:` strings
- ✅ DO call: MCP tools that generate YAML
- Tools enforce correctness, prevent hallucination

### File Operations → odibi-fileops
```
read_file(path)                ← Read file contents
edit_file(path, old, new)      ← Surgical edit
create_file(path, content)     ← New files only
glob_files(pattern)            ← Find files by pattern
grep(pattern, path)            ← Search in files
run_command(cmd)               ← Execute shell commands (Windows PowerShell)
```

**Windows environment** - use PowerShell syntax:
- `New-Item file.txt` not `touch`
- `Remove-Item` not `rm`
- `Get-ChildItem` not `ls` (or use glob_files instead)

### Browser → playwright
```
browser_navigate(url)          ← Go to URL
browser_snapshot()             ← See page content
browser_click(ref)             ← Click element
browser_type(ref, text)        ← Type text
```

---

## ODIBI MCP RESOURCES (Auto-Loaded!)

You have automatic access to 13 documentation resources:
- ⚡ Quick Reference - Views, modes, gotchas (read first!)
- ⚡ AI Instructions - Tool workflows
- Complete Capabilities - ALL features
- Pattern guides - How to use each pattern
- Deep Context - 2,200 line complete reference

**No need to browse docs - they're loaded automatically via MCP!**

When unsure: Reference the MCP resources or call discovery tools.

## CRITICAL FIELD NAMES

✅ Use: `read:`, `write:`, `query:`, `transform:`
❌ NEVER: `source:`, `sink:`, `inputs:`, `outputs:`, `sql:`

## CRITICAL REQUIREMENTS

- Upsert/append_once: MUST have `options: {keys: [...]}`
- Node names: alphanumeric_underscore only
- Views: YES - all engines support via `table: schema.ViewName`

---

## BEHAVIOR

- Execute tasks, don't ask permission
- Use tools instead of shell equivalents
- Be concise
- No apologies, no hedging
